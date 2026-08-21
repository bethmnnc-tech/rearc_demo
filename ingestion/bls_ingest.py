# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # BLS PR (Productivity) Ingestion
# MAGIC
# MAGIC Pulls the **entire contents** of https://download.bls.gov/pub/time.series/pr/
# MAGIC and lands each file as-is in a Unity Catalog Volume.
# MAGIC
# MAGIC **Design goals** (see PROCESS.md for the full rationale):
# MAGIC 1. No hardcoded filenames -- we scrape the live directory listing, so a file
# MAGIC    BLS adds or removes tomorrow is picked up automatically.
# MAGIC 2. Idempotent / safe to re-run -- a manifest Delta table records the
# MAGIC    (filename, size, last_modified) we last saw for each file. A file is only
# MAGIC    re-downloaded if it's new or if BLS changed it. Unchanged files are
# MAGIC    skipped entirely (no network call), which is also just polite to BLS's
# MAGIC    servers.
# MAGIC 3. Compliant, not just "unblocked" -- every request carries a `User-Agent`
# MAGIC    with real contact info per https://www.bls.gov/bls/pss.htm, which is what
# MAGIC    resolves the 403 in the first place.
# MAGIC
# MAGIC Run this as a scheduled Databricks Job task (see `resources/ingestion_job.yml`)
# MAGIC ahead of the Lakeflow Declarative Pipeline update.

# COMMAND ----------

# Plain import rather than `%run ./common`: Databricks Git folders / Repos put
# a notebook's own directory on `sys.path` automatically, so this works
# unmodified in the workspace -- and it also makes this file importable by a
# normal `pytest` run from the repo root (see tests/), which `%run` would not.
import sys
import os

# Ensure the ingestion directory is on sys.path for the import to work
ingestion_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in dir() else '/Workspace/Repos/beth_ramsey/rearc_demo_test/ingestion'
if ingestion_dir not in sys.path:
    sys.path.insert(0, ingestion_dir)

from common import *  # noqa: F401,F403

# COMMAND ----------

import re
import time
from datetime import datetime, timezone

import requests

# pyspark is imported lazily inside the functions that actually need a Spark
# session (ensure_manifest_table / load_manifest / run_bls_ingestion), not at
# module level. That keeps everything above -- the index-scraping and
# manifest-diffing logic, which is the part with real branching to get wrong
# -- importable and unit-testable with plain `pytest`, no cluster required.
# See tests/test_bls_ingest.py.

# COMMAND ----------

# MAGIC %md ## 1. Discover what's actually in the BLS directory right now

# COMMAND ----------

# We deliberately do NOT try to regex-parse file size/date out of the index
# page's HTML formatting -- Apache/nginx autoindex layouts are easy to get
# subtly wrong and can change without notice. Instead we only use the index
# page to discover *filenames* (which `href="..."` reliably gives us however
# the page is styled), then issue a HEAD request per file to get the
# authoritative Content-Length / Last-Modified straight from the server. That
# also means this keeps working even if BLS ever switches from a plain
# directory listing to something else, as long as links to files are still
# `<a href="...">`.
# Updated to handle both quoted/unquoted and lowercase/uppercase HREF attributes
_HREF_RE = re.compile(r'href=(?:"([^"]*)"|\' ([^\']*)\' |([^\s>]+))', re.IGNORECASE)


def _is_data_file_link(href: str, index_url: str) -> bool:
    """True for hrefs that point at an actual file in this directory.

    Filters out: parent-dir nav ("../"), Apache's sort-order query links
    (e.g. "?C=N;O=D"), and anchors/mailto links. Handles both relative
    filenames ("pr.class") and absolute paths ("/pub/time.series/pr/pr.class").
    """
    if not href or href.startswith(("?", "#", "mailto:")):
        return False
    if href in ("../", ".."):
        return False
    if href.endswith("/"):  # a sub-directory, not a file
        return False
    
    # If href is an absolute path, check if it's within our target directory
    if href.startswith("/"):
        # Extract the directory path from index_url (e.g., "/pub/time.series/pr/")
        from urllib.parse import urlparse
        index_path = urlparse(index_url).path.rstrip("/") + "/"
        # Accept if the href is a file directly in our target directory
        if href.startswith(index_path) and href.count("/") == index_path.count("/"):
            return True
        return False
    
    return True


def _request_with_retries(method: str, url: str, headers: dict, max_retries: int, **kwargs) -> requests.Response:
    last_exc = None
    for attempt in range(max_retries):
        resp = requests.request(method, url, headers=headers, timeout=30, **kwargs)
        if resp.status_code == 403:
            raise PermissionError(
                f"BLS returned 403 Forbidden for {method} {url}. This almost always "
                "means the User-Agent header is missing/generic. Set RDQ_CONTACT_NAME / "
                "RDQ_CONTACT_EMAIL (see ingestion/common.py) to real contact info, per "
                "https://www.bls.gov/bls/pss.htm."
            )
        if resp.status_code == 429 or resp.status_code >= 500:
            wait = 2**attempt
            time.sleep(wait)
            last_exc = RuntimeError(f"HTTP {resp.status_code} from BLS, retrying in {wait}s")
            continue
        resp.raise_for_status()
        return resp
    raise last_exc or RuntimeError(f"Failed {method} {url} after {max_retries} retries")


def fetch_bls_index(index_url: str, headers: dict, max_retries: int = 4) -> list[RemoteFile]:
    """Discover every file currently in the BLS pr/ directory.

    Filenames come from the index page's <a href> links (robust to markup
    changes). Size and last-modified come from a HEAD request per file
    (authoritative, not scraped/parsed from HTML).
    """
    resp = _request_with_retries("GET", index_url, headers, max_retries)

    # Extract hrefs, flattening the tuple groups from the regex
    raw_hrefs = _HREF_RE.findall(resp.text)
    hrefs = [m[0] or m[1] or m[2] for m in raw_hrefs]
    filtered_hrefs = [href for href in hrefs if _is_data_file_link(href, index_url)]
    
    # Extract just the filename from absolute paths
    names = []
    for href in filtered_hrefs:
        if href.startswith("/"):
            # Extract the filename (last component of the path)
            names.append(href.split("/")[-1])
        else:
            names.append(href)
    # de-dupe while preserving order, in case a link appears more than once
    names = list(dict.fromkeys(names))

    if not names:
        # Provide diagnostic info: if BLS returned an error page instead of the
        # directory listing, show the beginning so the user can see what went wrong
        preview = resp.text[:500] if len(resp.text) > 500 else resp.text
        raise RuntimeError(
            f"Fetched the BLS index page but found zero file links. "
            f"Status: {resp.status_code}, Content-Type: {resp.headers.get('Content-Type')}. "
            f"Response preview (first 500 chars): {preview!r}"
        )

    files = []
    for name in names:
        head = _request_with_retries("HEAD", index_url.rstrip("/") + "/" + name, headers, max_retries)
        size = int(head.headers.get("Content-Length", -1))
        last_modified = head.headers.get("Last-Modified", "")
        files.append(RemoteFile(name=name, size_bytes=size, last_modified=last_modified))
    return files


# COMMAND ----------

# MAGIC %md ## 2. Diff against what we've already ingested (the manifest table)

# COMMAND ----------

def _manifest_schema():
    from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType

    return StructType(
        [
            StructField("file_name", StringType(), False),
            StructField("size_bytes", LongType(), False),
            StructField("last_modified", StringType(), False),
            StructField("ingested_at", TimestampType(), False),
        ]
    )


def ensure_manifest_table():
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {MANIFEST_TABLE} (
            file_name STRING NOT NULL,
            size_bytes BIGINT NOT NULL,
            last_modified STRING NOT NULL,
            ingested_at TIMESTAMP NOT NULL
        ) USING DELTA
        TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
        """
    )


def load_manifest() -> dict[str, RemoteFile]:
    rows = spark.table(MANIFEST_TABLE).collect()
    return {
        r.file_name: RemoteFile(name=r.file_name, size_bytes=r.size_bytes, last_modified=r.last_modified)
        for r in rows
    }


def files_to_pull(remote: list[RemoteFile], manifest: dict[str, RemoteFile]) -> list[RemoteFile]:
    """New files, or files whose size/last_modified changed since we last pulled them."""
    out = []
    for f in remote:
        prev = manifest.get(f.name)
        if prev is None or prev.size_bytes != f.size_bytes or prev.last_modified != f.last_modified:
            out.append(f)
    return out


# COMMAND ----------

# MAGIC %md ## 3. Download changed files into the Volume, then update the manifest
# MAGIC
# MAGIC Files are written to the **same path** every time (overwrite-in-place),
# MAGIC because BLS revises data under a stable filename (e.g. `pr.data.0.Current`
# MAGIC gets new quarters appended/revised, not renamed). The Bronze layer's Auto
# MAGIC Loader stream is configured with `cloudFiles.allowOverwrites = true` so a
# MAGIC changed file is correctly picked back up -- see pipeline/01_bronze.py.

# COMMAND ----------


def download_file(index_url: str, name: str, dest_dir: str, headers: dict) -> None:
    url = index_url.rstrip("/") + "/" + name
    resp = requests.get(url, headers=headers, timeout=120, stream=True)
    resp.raise_for_status()
    dest_path = f"{dest_dir}/{name}"
    # Volumes are exposed as a normal filesystem path, so a plain buffered
    # write works -- no need for dbutils.fs for binary/streamed content.
    with open(dest_path, "wb") as fh:
        for chunk in resp.iter_content(chunk_size=1024 * 1024):
            fh.write(chunk)


def run_bls_ingestion() -> dict:
    dbutils = get_dbutils()
    dbutils.fs.mkdirs(BLS_RAW_PATH)
    ensure_manifest_table()

    remote_files = fetch_bls_index(BLS_PR_INDEX_URL, REQUEST_HEADERS)
    manifest = load_manifest()
    to_pull = files_to_pull(remote_files, manifest)

    downloaded, failed = [], []
    for f in to_pull:
        try:
            download_file(BLS_PR_INDEX_URL, f.name, BLS_RAW_PATH, REQUEST_HEADERS)
            downloaded.append(f)
        except Exception as e:  # noqa: BLE001 - keep going for the rest of the batch
            print(f"FAILED to download {f.name}: {e}")
            failed.append(f.name)

    if downloaded:
        from pyspark.sql import Row

        now = datetime.now(timezone.utc)
        new_rows = [
            Row(file_name=f.name, size_bytes=f.size_bytes, last_modified=f.last_modified, ingested_at=now)
            for f in downloaded
        ]
        new_df = spark.createDataFrame(new_rows, schema=_manifest_schema())
        # MERGE so a re-download of a previously-seen (changed) file updates
        # its manifest row instead of duplicating it.
        new_df.createOrReplaceTempView("_bls_manifest_updates")
        spark.sql(
            f"""
            MERGE INTO {MANIFEST_TABLE} AS target
            USING _bls_manifest_updates AS updates
            ON target.file_name = updates.file_name
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            """
        )

    summary = {
        "remote_file_count": len(remote_files),
        "already_current": len(remote_files) - len(to_pull),
        "downloaded": [f.name for f in downloaded],
        "failed": failed,
    }
    print(summary)
    if failed:
        # Surface a non-zero exit for the Job run so a partial failure is visible,
        # rather than silently succeeding with missing files.
        raise RuntimeError(f"{len(failed)} file(s) failed to download: {failed}")
    return summary


# COMMAND ----------

if __name__ == "__main__" or "dbutils" in dir():
    # When running interactively (not via a job that passes base_parameters),
    # ensure widgets have proper values before common.py reads them. The
    # widgets are defined at the notebook level, but common.py's module-level
    # evaluation happens at import time, so we need to ensure they're populated
    # beforehand. This is a no-op when running via the job.
    try:
        # Only set if not already set to avoid overwriting job parameters
        try:
            current_name = dbutils.widgets.get("rdq_contact_name")
        except:
            dbutils.widgets.text("rdq_contact_name", "BethR")
        
        try:
            current_email = dbutils.widgets.get("rdq_contact_email")
        except:
            dbutils.widgets.text("rdq_contact_email", "bethmnnc@gmail.com")
    except:
        pass  # dbutils not available (e.g., pytest context)
    
    # Reload common module to pick up the widget values
    import sys
    if 'common' in sys.modules:
        import importlib
        import common
        importlib.reload(common)
        # Re-import all names
        from common import *  # noqa: F401,F403
    
    # Verify contact info is properly configured
    if CONTACT_NAME == "Your Name" or CONTACT_EMAIL == "your.email@example.com":
        raise ValueError(
            "Contact information not configured. Set the rdq_contact_name and "
            "rdq_contact_email notebook parameters to real values, per BLS policy "
            "at https://www.bls.gov/bls/pss.htm. Current values: "
            f"CONTACT_NAME='{CONTACT_NAME}', CONTACT_EMAIL='{CONTACT_EMAIL}'"
        )
    
    print(f"Using User-Agent: {BLS_USER_AGENT}")
    run_bls_ingestion()