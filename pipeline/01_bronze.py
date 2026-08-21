# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze layer
# MAGIC
# MAGIC Raw-as-landed ingestion of everything the `ingestion/` job dropped into the
# MAGIC Volume, via Auto Loader (`cloudFiles`) streaming tables.
# MAGIC
# MAGIC **Why Auto Loader / streaming tables here, not a batch `spark.read`:**
# MAGIC Auto Loader tracks which files it has already processed (via its
# MAGIC checkpoint), so re-triggering this pipeline does **not** reprocess files
# MAGIC it already ingested -- new/changed files are picked up incrementally.
# MAGIC That's what satisfies "if we re-run this, it shouldn't reprocess anything
# MAGIC it's already ingested" at the pipeline layer, on top of the ingestion job's
# MAGIC own manifest-based skip logic (which avoids the network call in the first
# MAGIC place). `cloudFiles.allowOverwrites = true` additionally means: if BLS
# MAGIC revises `pr.data.0.Current` in place (same filename, new content -- which is
# MAGIC exactly how BLS publishes revisions), Auto Loader will pick that up as a
# MAGIC change rather than ignoring it because "we've seen this path before."
# MAGIC
# MAGIC **Why bronze stays stringly-typed:** Bronze should be a faithful,
# MAGIC replayable copy of the source. Trimming BLS's fixed-width padding, casting
# MAGIC types, and validating value ranges are all Silver's job -- that way if a
# MAGIC cleaning rule turns out to be wrong, Bronze still has the untouched original
# MAGIC to reprocess from.

# COMMAND ----------

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

CATALOG = spark.conf.get("rdq.catalog", "rearc_dev_001")
BRONZE_SCHEMA = spark.conf.get("rdq.bronze_schema", "bronze")
SILVER_SCHEMA = spark.conf.get("rdq.silver_schema", "silver")  # noqa: F841 -- used by other pipeline files
GOLD_SCHEMA = spark.conf.get("rdq.gold_schema", "gold")  # noqa: F841 -- used by other pipeline files
VOLUME_SCHEMA = spark.conf.get("rdq.volume_schema", "volumes")
VOLUME = spark.conf.get("rdq.volume", "bls_gov")
VOLUME_ROOT = f"/Volumes/{CATALOG}/{VOLUME_SCHEMA}/{VOLUME}"
BLS_RAW_PATH = f"{VOLUME_ROOT}/bls_pr"
POPULATION_RAW_PATH = f"{VOLUME_ROOT}/population"

# Bronze/Silver/Gold are three separate schemas (not three sets of prefixed
# tables in one schema), so every @dlt.table below is registered with an
# explicit "{schema}.{name}" name -- and every downstream dlt.read() must
# reference that exact qualified string. See ingestion/common.py for why.

# COMMAND ----------

# MAGIC %md
# MAGIC ## `pr.data.0.Current`
# MAGIC
# MAGIC This file's header/sample confirms it already carries the **full** history
# MAGIC per series (its first rows for `PRS30006011` start at 1995, not the current
# MAGIC year), while `pr.data.1.AllData` is a larger, overlapping export of the same
# MAGIC underlying series. We land both raw (below), but build Silver/Gold only from
# MAGIC `pr.data.0.Current` to avoid double-counting the same observations from two
# MAGIC overlapping sources -- see PROCESS.md for the full reasoning.
# MAGIC

# COMMAND ----------

BLS_DATA_SCHEMA = StructType(
    [
        StructField("series_id", StringType(), True),
        StructField("year", StringType(), True),
        StructField("period", StringType(), True),
        StructField("value", StringType(), True),
        StructField("footnote_codes", StringType(), True),
    ]
)


@dlt.table(
    name=f"{BRONZE_SCHEMA}.bronze_bls_data",
    comment="Raw pr.data.0.Current, one row per series_id/year/period as published by BLS.",
)
@dlt.expect_or_drop("has_series_id", "series_id IS NOT NULL")
@dlt.expect_or_drop("has_year", "year IS NOT NULL")
@dlt.expect_or_drop("has_period", "period IS NOT NULL")
@dlt.expect("has_value", "value IS NOT NULL")  # flag, don't drop -- a null value may still be worth keeping raw
def bronze_bls_data():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("sep", "\t")
        .option("header", "true")
        .option("cloudFiles.allowOverwrites", "true")
        .option("pathGlobFilter", "pr.data.0.Current")
        .schema(BLS_DATA_SCHEMA)
        .load(BLS_RAW_PATH)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## `pr.data.1.AllData` -- landed for completeness/auditability, not used downstream
# MAGIC

# COMMAND ----------

@dlt.table(
    name=f"{BRONZE_SCHEMA}.bronze_bls_data_alldata",
    comment="Raw pr.data.1.AllData, landed for completeness. Not used by Silver/Gold -- see PROCESS.md.",
)
def bronze_bls_data_alldata():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("sep", "\t")
        .option("header", "true")
        .option("cloudFiles.allowOverwrites", "true")
        .option("pathGlobFilter", "pr.data.1.AllData")
        .schema(BLS_DATA_SCHEMA)
        .load(BLS_RAW_PATH)
        .withColumn("_ingested_at", F.current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## `pr.series` -- series metadata (needed to build human-readable labels in Gold)
# MAGIC

# COMMAND ----------

BLS_SERIES_SCHEMA = StructType(
    [
        StructField("series_id", StringType(), True),
        StructField("sector_code", StringType(), True),
        StructField("class_code", StringType(), True),
        StructField("measure_code", StringType(), True),
        StructField("duration_code", StringType(), True),
        StructField("seasonal", StringType(), True),
        StructField("base_year", StringType(), True),
        StructField("footnote_codes", StringType(), True),
        StructField("begin_year", StringType(), True),
        StructField("begin_period", StringType(), True),
        StructField("end_year", StringType(), True),
        StructField("end_period", StringType(), True),
    ]
)


@dlt.table(name=f"{BRONZE_SCHEMA}.bronze_bls_series", comment="Raw pr.series -- one row of metadata per series_id.")
@dlt.expect_or_drop("has_series_id", "series_id IS NOT NULL")
def bronze_bls_series():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("sep", "\t")
        .option("header", "true")
        .option("cloudFiles.allowOverwrites", "true")
        .option("pathGlobFilter", "pr.series")
        .schema(BLS_SERIES_SCHEMA)
        .load(BLS_RAW_PATH)
        .withColumn("_ingested_at", F.current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Small reference/lookup files
# MAGIC
# MAGIC `pr.sector`, `pr.class`, `pr.measure`, `pr.duration`, `pr.seasonal`,
# MAGIC `pr.footnote` all share the same shape (tab-separated, header row, a
# MAGIC `*_code` -> `*_text` mapping). We land each as its own bronze table with an
# MAGIC inferred schema -- they're tiny, low-risk reference data, and letting Spark
# MAGIC infer types here (vs. hand-writing 6 near-identical StructTypes) is a
# MAGIC deliberate low-ceremony choice for genuinely static lookup tables. `pr.measure`
# MAGIC and `pr.sector` feed the Gold-layer human-readable labels; the rest are
# MAGIC landed for completeness ("pull the full contents of the folder").
# MAGIC
# MAGIC We deliberately do NOT turn on `cloudFiles.inferColumnTypes` here:
# MAGIC `measure_code` values like `"01"` are meaningful zero-padded codes, and
# MAGIC type inference would silently turn them into the integer `1`, breaking the
# MAGIC join against `pr.series.measure_code` (also a padded string) in Silver.
# MAGIC Everything lands as STRING; Silver casts only the columns that need it.
# MAGIC

# COMMAND ----------

LOOKUP_FILES = ["pr.sector", "pr.class", "pr.measure", "pr.duration", "pr.seasonal", "pr.footnote", "pr.period"]

for _lookup_name in LOOKUP_FILES:
    # Table names can't contain ".", and DLT needs a distinct function per
    # table -- build one via a small factory instead of copy/pasting the body
    # 7 times.
    def _make_lookup_table(file_name):
        table_name = f"{BRONZE_SCHEMA}.bronze_bls_" + file_name.replace(".", "_")

        @dlt.table(name=table_name, comment=f"Raw {file_name} reference/lookup file.")
        def _bronze_lookup():
            return (
                spark.readStream.format("cloudFiles")
                .option("cloudFiles.format", "csv")
                .option("sep", "\t")
                .option("header", "true")
                .option("cloudFiles.allowOverwrites", "true")
                .option("pathGlobFilter", file_name)
                .load(BLS_RAW_PATH)
                .withColumn("_ingested_at", F.current_timestamp())
            )

        return _bronze_lookup

    _make_lookup_table(_lookup_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## `pr.txt` -- BLS's own documentation for this survey
# MAGIC
# MAGIC Not tabular (it's the human-readable field dictionary BLS ships alongside
# MAGIC the data -- series/data/mapping file formats, field definitions, update
# MAGIC schedule), but it's still part of "the full contents of the folder," and
# MAGIC it's the actual authoritative source this pipeline's schemas and the
# MAGIC Silver-layer label logic were built against (see PROCESS.md) -- worth
# MAGIC landing as its own bronze table for the same auditability reason as
# MAGIC everything else here: so a reviewer can see exactly what we built against,
# MAGIC not just take our word for the schema.
# MAGIC

# COMMAND ----------

@dlt.table(
    name=f"{BRONZE_SCHEMA}.bronze_bls_pr_txt",
    comment="Raw pr.txt, BLS's documentation for the PR survey, one row per line.",
)
def bronze_bls_pr_txt():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "text")
        .option("cloudFiles.allowOverwrites", "true")
        .option("pathGlobFilter", "pr.txt")
        .load(BLS_RAW_PATH)
        .withColumnRenamed("value", "line")
        .withColumn("_ingested_at", F.current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Population API (DataUSA)
# MAGIC
# MAGIC Landed as JSON; the API's field casing isn't pinned down in this repo (see
# MAGIC PROCESS.md), so bronze intentionally keeps whatever columns the source
# MAGIC actually returns rather than assuming exact names -- Silver normalizes them.

# COMMAND ----------

@dlt.table(
    name=f"{BRONZE_SCHEMA}.bronze_population",
    comment="Raw DataUSA population API response, one row per (Year, Nation) record.",
)
@dlt.expect("has_data_row", "true")  # placeholder row-level check; real validation happens post-explode in Silver
def bronze_population():
    raw = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("multiLine", "true")
        .option("cloudFiles.allowOverwrites", "true")
        .option("pathGlobFilter", "population_latest.json")
        .option("cloudFiles.schemaHints", "data ARRAY<STRUCT<Year:STRING,Nation:STRING,Population:STRING,Slug_Nation:STRING,ID_Nation:STRING>>")
        .load(POPULATION_RAW_PATH)
    )
    return raw.select(F.explode("data").alias("record"), F.current_timestamp().alias("_ingested_at")).select(
        "record.*", "_ingested_at"
    )