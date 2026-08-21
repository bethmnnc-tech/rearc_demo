# Rearc Data Quest — Databricks Edition

BLS productivity time-series + DataUSA population, landed in a Unity Catalog
Volume and modeled as a Lakeflow (Spark) Declarative Pipeline: Bronze → Silver
→ Gold, with data-quality expectations, a Spark SQL primary implementation and
a PySpark alternate for all three analytical questions, and an automated check
that the two agree.

See **`PROCESS.md`** for the architecture rationale, trade-offs, and
retrospective, and the **AI usage disclosure** at the bottom of it.

## Before you run this

1. **Fill in real contact info for the BLS `User-Agent`.** BLS returns `403
   Forbidden` to requests it can't attribute to an owner
   (https://www.bls.gov/bls/pss.htm) — set `RDQ_CONTACT_NAME` /
   `RDQ_CONTACT_EMAIL` as job parameters (see `resources/ingestion_job.yml`)
   or edit the defaults in `ingestion/common.py`.
2. Confirm you have somewhere to create a catalog/schema (Databricks Free
   Edition works — see `setup/00_setup_catalog_schema_volume.py` for the
   fallback if you can't `CREATE CATALOG`).

## Environment

This repo is wired up for one catalog, `rearc_dev_001`, split into three
schemas — one per medallion layer — plus a schema for the raw-file Volume:

| Layer  | Location                                       |
|--------|-------------------------------------------------|
| Bronze | `rearc_dev_001.bronze`                          |
| Silver | `rearc_dev_001.silver`                          |
| Gold   | `rearc_dev_001.gold`                            |
| Raw    | `/Volumes/rearc_dev_001/volumes/bls_gov`        |

Bronze/Silver/Gold are separate schemas (not three sets of prefixed tables in
one schema) specifically so Gold can get its own schema-level read-only
grant — see `resources/grants_readonly_analyst.sql`. If you're using
different names, override them via the Asset Bundle variables in
`databricks.yml` (`catalog`, `bronze_schema`, `silver_schema`, `gold_schema`,
`volume_schema`, `volume_name`), or the matching `RDQ_*` env vars / job
parameters if running outside the bundle — see `ingestion/common.py`.

## Repo layout

```
ingestion/
  common.py               shared config (catalog/schema/volume names, BLS User-Agent)
  bls_ingest.py            scrapes download.bls.gov/pub/time.series/pr/, lands raw files idempotently
  population_ingest.py     pulls the DataUSA population API, lands raw JSON
setup/
  00_setup_catalog_schema_volume.py   one-time UC setup (idempotent)
pipeline/
  01_bronze.py              Auto Loader streaming tables, one per BLS/population source file
  02_silver.py              typed/cleaned/deduped, + human-readable series labels
  03_gold_sql.sql           PRIMARY: all 3 analytical questions in Spark SQL
  03_gold_pyspark_alt.py    ALTERNATE: same 3 questions in PySpark DataFrame API
  04_gold_verification.py   diffs primary vs. alternate; fails the pipeline if they disagree
resources/
  ingestion_job.yml         Databricks Job: setup -> ingest BLS + population -> run pipeline
  pipeline.yml              the Declarative Pipeline definition
  grants_readonly_analyst.sql   bonus: UC grants for a read-only analyst on Gold only
databricks.yml              Asset Bundle entrypoint (bonus: deploy without clicking through the UI)
tests/                       pytest suite for the ingestion logic that doesn't need a cluster
PROCESS.md                   architecture, trade-offs, retrospective, AI usage disclosure
```

## Running it

### Option A — Databricks Asset Bundles (recommended)

```bash
databricks configure                 # if you haven't already
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run -t dev bls_ingestion_job   # lands raw data, then runs the pipeline
```

Set `catalog` / `bronze_schema` / `silver_schema` / `gold_schema` /
`volume_schema` / `volume_name` / `contact_name` / `contact_email` either in
`databricks.yml`'s `variables` block or with `--var`, e.g.:

```bash
databricks bundle deploy -t dev \
  --var="contact_name=Beth,contact_email=you@example.com"
```

### Option B — click through the UI

1. Import this repo as a Databricks Repo (or Git folder).
2. Run `setup/00_setup_catalog_schema_volume.py` once.
3. Run `ingestion/bls_ingest.py`, then `ingestion/population_ingest.py`
   (either as ad-hoc notebook runs, or wire them into a Job — see
   `resources/ingestion_job.yml` for the shape).
4. Create a Lakeflow Declarative Pipeline pointing at the `pipeline/` folder
   (all 5 files as source code), catalog `rearc_dev_001` matching what you
   used in step 2, and set the `rdq.*` pipeline configuration values (see
   `resources/pipeline.yml` for the exact keys) if you didn't use the default
   schema names. Start an update.
5. Re-running steps 3–4 is safe — unchanged BLS files aren't re-downloaded,
   and Auto Loader won't reprocess files it's already ingested.

## Checking the answers

Once the pipeline has run, the three Gold tables answer the quest's
questions directly:

```sql
SELECT * FROM rearc_dev_001.gold.gold_population_stats;
SELECT * FROM rearc_dev_001.gold.gold_bls_best_year_per_series ORDER BY series_id;
SELECT * FROM rearc_dev_001.gold.gold_prs30006032_q01_population ORDER BY year;
```

`gold_verification_q1/q2/q3` each report a `diff_row_count` — should always be
`0`, confirming the SQL-primary and PySpark-alternate implementations agree.

## Local tests

The ingestion logic that has real branching to get wrong (which links in the
BLS directory listing count as files, which files need re-pulling, how the
population API response is validated) is unit-tested without needing a
cluster:

```bash
pip install -r requirements-dev.txt
pytest tests/ -v
```

## Screenshot

`screenshots/` is where the required screenshot of the running
pipeline/tables goes before submitting — see `screenshots/README.md`.
