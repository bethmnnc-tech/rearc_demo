"""
Shared configuration and helpers for the ingestion notebooks/jobs.

Import this from bls_ingest.py and population_ingest.py (either as a %run in a
Databricks notebook, or as a plain module if you package this as a wheel/job).

Everything here is intentionally dependency-light (stdlib + requests) so it can
run as a simple Databricks Python task, not just inside a notebook with a
Spark session attached -- Spark is only needed for the manifest table.
"""

from __future__ import annotations

import dataclasses
import os


def get_dbutils():
    """Return the notebook-scoped `dbutils`, for use from a plain .py module.

    Databricks injects `dbutils` into notebook global scope automatically, but
    a module imported via %run/import doesn't get it for free. This grabs it
    from the caller's IPython user namespace when running inside Databricks.
    Raises (any exception) when there's no notebook context to grab it from
    -- e.g. a plain `pytest` run -- which `_config_value` below relies on.
    """
    import IPython

    return IPython.get_ipython().user_ns["dbutils"]


def _config_value(widget_name: str, env_name: str, default: str) -> str:
    """Resolve one config value, in priority order:

    1. A Databricks job parameter / notebook widget of this name, if one is
       already set (e.g. via a Job's `base_parameters` -- see
       resources/ingestion_job.yml). Checked first, and *not* overwritten, so
       a value the job explicitly passed in always wins.
    2. An environment variable, for local/CI use or a quick manual override.
    3. The given default.

    Degrades cleanly outside Databricks (e.g. at import time during
    `pytest`, where `dbutils` doesn't exist) by falling straight through to
    (2)/(3).
    """
    try:
        dbutils = get_dbutils()
    except Exception:
        return os.environ.get(env_name, default)

    try:
        return dbutils.widgets.get(widget_name)
    except Exception:
        dbutils.widgets.text(widget_name, os.environ.get(env_name, default))
        return dbutils.widgets.get(widget_name)


@dataclasses.dataclass(frozen=True)
class RemoteFile:
    """One row of the BLS directory listing."""

    name: str
    size_bytes: int
    last_modified: str  # ISO-ish string as reported by BLS; treated as an opaque token


# ---------------------------------------------------------------------------
# Unity Catalog / Volume configuration
#
# These match setup/00_setup_catalog_schema_volume.py. Defaults below are set
# to this repo's actual dev environment; override via a Job parameter or env
# var (see `_config_value` above) if you use different names elsewhere.
#
# One catalog, four schemas: `bronze` / `silver` / `gold` hold the pipeline's
# medallion layers as three separate schemas (not three sets of prefixed
# tables in one schema) specifically so access control can be granted at the
# schema level -- e.g. "SELECT on the whole gold schema" for a read-only
# analyst -- instead of table-by-table. `volumes` holds the raw-file Volume,
# kept separate from bronze/silver/gold since it's storage for
# not-yet-structured data, not a queryable layer itself.
# ---------------------------------------------------------------------------
CATALOG = _config_value("rdq_catalog", "RDQ_CATALOG", "rearc_dev_001")
BRONZE_SCHEMA = _config_value("rdq_bronze_schema", "RDQ_BRONZE_SCHEMA", "bronze")
SILVER_SCHEMA = _config_value("rdq_silver_schema", "RDQ_SILVER_SCHEMA", "silver")
GOLD_SCHEMA = _config_value("rdq_gold_schema", "RDQ_GOLD_SCHEMA", "gold")
VOLUME_SCHEMA = _config_value("rdq_volume_schema", "RDQ_VOLUME_SCHEMA", "volumes")
VOLUME = _config_value("rdq_volume", "RDQ_VOLUME", "bls_gov")

VOLUME_ROOT = f"/Volumes/{CATALOG}/{VOLUME_SCHEMA}/{VOLUME}"
BLS_RAW_PATH = f"{VOLUME_ROOT}/bls_pr"
POPULATION_RAW_PATH = f"{VOLUME_ROOT}/population"

# Delta table that tracks what we've already pulled from BLS, so re-running
# the ingestion job doesn't re-download (or reprocess) unchanged files. Lives
# in the bronze schema since it's ingestion bookkeeping that directly feeds
# Bronze, not a queryable data asset in its own right.
MANIFEST_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.bls_ingestion_manifest"

# ---------------------------------------------------------------------------
# BLS access policy: https://www.bls.gov/bls/pss.htm
#
# BLS returns 403 Forbidden to requests that don't identify an owner it can
# contact. A descriptive User-Agent with real contact info fixes this and
# keeps us compliant with their stated policy -- this is NOT a workaround,
# it's what they explicitly ask automated clients to do.
#
# >>> REPLACE THIS WITH YOUR OWN NAME/EMAIL BEFORE RUNNING <<<
# ---------------------------------------------------------------------------
CONTACT_NAME = _config_value("rdq_contact_name", "RDQ_CONTACT_NAME", "BethR")
CONTACT_EMAIL = _config_value("rdq_contact_email", "RDQ_CONTACT_EMAIL", "bethmnnc@gmail.com")

BLS_USER_AGENT = f"{CONTACT_NAME} ({CONTACT_EMAIL}) - Rearc Data Quest take-home"

REQUEST_HEADERS = {
    "User-Agent": BLS_USER_AGENT,
}

BLS_PR_INDEX_URL = "https://download.bls.gov/pub/time.series/pr/"
POPULATION_API_URL = (
    "https://honolulu-api.datausa.io/tesseract/data.jsonrecords"
    "?cube=acs_yg_total_population_1&drilldowns=Year%2CNation&locale=en&measures=Population"
)
