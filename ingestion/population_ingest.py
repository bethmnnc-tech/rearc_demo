# Databricks notebook source
# MAGIC %md
# MAGIC # DataUSA Population Ingestion
# MAGIC
# MAGIC Pulls annual US population from the DataUSA Tesseract API
# MAGIC (https://datausa.io/about/api/) and lands the raw JSON response in the
# MAGIC same Volume used for the BLS data.
# MAGIC
# MAGIC This is a small, cheap, non-paginated response, so "idempotent" here just
# MAGIC means: don't write a new file if the content hasn't changed since last run.
# MAGIC We still keep every distinct version we've ever seen (content-addressed by
# MAGIC hash) so Bronze can be reasoned about historically, plus a stable
# MAGIC `population_latest.json` pointer that always reflects the most recent pull
# MAGIC for simple downstream reads.

# COMMAND ----------

# Plain import rather than `%run ./common` -- see bls_ingest.py for why.
from common import *  # noqa: F401,F403

# COMMAND ----------

import hashlib
import json
from datetime import datetime, timezone

import requests

# COMMAND ----------


def fetch_population_json(url: str, headers: dict, max_retries: int = 4) -> dict:
    last_exc = None
    for attempt in range(max_retries):
        resp = requests.get(url, headers=headers, timeout=30)
        if resp.status_code == 429 or resp.status_code >= 500:
            wait = 2**attempt
            print(f"HTTP {resp.status_code} from DataUSA, retrying in {wait}s")
            import time

            time.sleep(wait)
            last_exc = RuntimeError(f"HTTP {resp.status_code} from DataUSA")
            continue
        resp.raise_for_status()
        payload = resp.json()
        # The API is expected to return {"data": [ {...one row per Year...} ]}.
        # Fail loudly rather than silently landing something we can't use
        # downstream if DataUSA ever changes their response envelope.
        if "data" not in payload or not isinstance(payload["data"], list) or not payload["data"]:
            raise RuntimeError(
                "DataUSA response didn't contain a non-empty 'data' array as "
                f"expected. Got top-level keys: {list(payload.keys())}"
            )
        return payload
    raise last_exc or RuntimeError("Failed to fetch population API after retries")


def run_population_ingestion() -> dict:
    dbutils = get_dbutils()
    dbutils.fs.mkdirs(POPULATION_RAW_PATH)

    payload = fetch_population_json(POPULATION_API_URL, REQUEST_HEADERS)
    raw_bytes = json.dumps(payload, indent=2).encode("utf-8")
    content_hash = hashlib.sha256(raw_bytes).hexdigest()[:16]

    versioned_name = f"population_{content_hash}.json"
    versioned_path = f"{POPULATION_RAW_PATH}/{versioned_name}"
    latest_path = f"{POPULATION_RAW_PATH}/population_latest.json"

    already_have_this_version = versioned_name in {
        f.name for f in dbutils.fs.ls(POPULATION_RAW_PATH)
    }

    if not already_have_this_version:
        with open(versioned_path, "wb") as fh:
            fh.write(raw_bytes)

    # Always refresh the `_latest` pointer file so downstream/Bronze has one
    # stable path to point Auto Loader at, regardless of whether content changed.
    with open(latest_path, "wb") as fh:
        fh.write(raw_bytes)

    summary = {
        "content_hash": content_hash,
        "row_count": len(payload["data"]),
        "new_version_written": not already_have_this_version,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }
    print(summary)
    return summary


# COMMAND ----------

if __name__ == "__main__" or "dbutils" in dir():
    run_population_ingestion()
