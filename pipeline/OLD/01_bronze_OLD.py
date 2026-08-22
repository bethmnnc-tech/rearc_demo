# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC ## Bronze Layer Overview
# MAGIC
# MAGIC This notebook implements the **Bronze (Raw) layer** of a medallion architecture data pipeline for BLS (Bureau of Labor Statistics) productivity data and US population data.
# MAGIC
# MAGIC ### Key Features
# MAGIC
# MAGIC **Auto Loader with Checkpointing**
# MAGIC - Uses Databricks Auto Loader (`cloudFiles` format) to incrementally ingest data from Unity Catalog volumes
# MAGIC - Checkpoints prevent duplicate data loads if the notebook is rerun
# MAGIC - Safe for scheduled/repeated execution
# MAGIC
# MAGIC **No Transformations**
# MAGIC - Bronze tables mirror source data exactly
# MAGIC - Preserves ability to reload from bronze as a starting point
# MAGIC - Acts as an audit trail of raw data as received
# MAGIC
# MAGIC **Data Sources**
# MAGIC 1. **BLS Productivity Survey** (`pr.*` files from `bls_gov` volume)
# MAGIC    - Main data file: `pr.data.0.Current` (full historical time series per series_id)
# MAGIC    - Alternate data file: `pr.data.1.AllData` (landed for completeness, not used downstream)
# MAGIC    - Series metadata: `pr.series`
# MAGIC    - Reference/lookup tables: `pr.sector`, `pr.class`, `pr.measure`, `pr.duration`, `pr.seasonal`, `pr.footnote`, `pr.period`
# MAGIC    - Documentation: `pr.txt` (BLS field dictionary)
# MAGIC
# MAGIC 2. **Population Data** (DataUSA API from `population` volume)
# MAGIC    - JSON format with Year/Nation/Population records
# MAGIC
# MAGIC **Data Quality**
# MAGIC - Uses DLT expectations to validate required fields
# MAGIC - `@dlt.expect_or_drop` removes invalid rows
# MAGIC - `@dlt.expect` logs quality metrics without dropping
# MAGIC
# MAGIC **Metadata Tracking**
# MAGIC - All tables include `_ingested_at` timestamp
# MAGIC - Data tables include `_source_file` for traceability
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Create varibles with defaults

# COMMAND ----------

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType


# COMMAND ----------

# Read from pipeline configuration (works in DLT pipeline execution)
CATALOG = spark.conf.get("rdq.catalog", "rearc_dev_001")
BRONZE_SCHEMA = spark.conf.get("rdq.bronze_schema", "bronze")
SILVER_SCHEMA = spark.conf.get("rdq.silver_schema", "silver")
GOLD_SCHEMA = spark.conf.get("rdq.gold_schema", "gold")
VOLUME_SCHEMA = spark.conf.get("rdq.volume_schema", "volumes")
VOLUME = spark.conf.get("rdq.volume", "bls_gov")
VOLUME_ROOT = f"/Volumes/{CATALOG}/{VOLUME_SCHEMA}/{VOLUME}"
BLS_RAW_PATH = f"{VOLUME_ROOT}/bls_pr"
POPULATION_RAW_PATH = f"{VOLUME_ROOT}/population"

# COMMAND ----------

# MAGIC %md
# MAGIC ## `pr.data.0.Current`
# MAGIC
# MAGIC Checked the header and first few rows — this file has the full historical 
# MAGIC data for each series. For example, `PRS30006011` starts back in 1995, not 
# MAGIC just recent years. Meanwhile `pr.data.1.AllData` is bigger and has a lot of 
# MAGIC the same series.
# MAGIC
# MAGIC We're loading both into bronze (raw storage), but downstream in silver and 
# MAGIC gold we only use `pr.data.0.Current`. That way we don't accidentally count 
# MAGIC the same data points twice. See PROCESS.md for more detail on why.
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
    name=f"{CATALOG}.{BRONZE_SCHEMA}.bronze_bls_data",
    comment="Raw pr.data.0.Current, one row per series_id/year/period as published by BLS.",
)
@dlt.expect_or_drop("has_series_id", "series_id IS NOT NULL")  #Expectations allows you check if the data is good.  If not, you drop or isolate the data into a separate table.
@dlt.expect_or_drop("has_year", "year IS NOT NULL")
@dlt.expect_or_drop("has_period", "period IS NOT NULL")
@dlt.expect("has_value", "value IS NOT NULL")
def bronze_bls_data():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("sep", "\t")
        .option("header", "true")
        .option("cloudFiles.allowOverwrites", "true")
        .option("pathGlobFilter", "pr.data.0.Current")
        .schema(BLS_DATA_SCHEMA) #defined schema above
        .load(BLS_RAW_PATH)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## `pr.data.1.AllData` 
# MAGIC landed for completeness/auditability, not used downstream
# MAGIC

# COMMAND ----------

@dlt.table(
    name=f"{CATALOG}.{BRONZE_SCHEMA}.bronze_bls_data_alldata",
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
# MAGIC ## `pr.series` 
# MAGIC series metadata (needed to build legible labels in Gold)
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


@dlt.table(name=f"{CATALOG}.{BRONZE_SCHEMA}.bronze_bls_series", comment="Raw pr.series -- one row of metadata per series_id.")
@dlt.expect_or_drop("has_series_id", "series_id IS NOT NULL")  #drop if key is null
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
        table_name = f"{CATALOG}.{BRONZE_SCHEMA}.bronze_bls_" + file_name.replace(".", "_")

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
# MAGIC ## `pr.txt` -- BLS's own documentation
# MAGIC
# MAGIC This isn't a data file — it's BLS's field dictionary that comes with the
# MAGIC survey data. Has file format specs, field definitions, and the update
# MAGIC schedule.
# MAGIC
# MAGIC We're loading it into bronze anyway because the schemas and label mappings
# MAGIC in Silver were built from this file (details in PROCESS.md). Keeping it
# MAGIC here means anyone reviewing the pipeline can check our work against the
# MAGIC original source instead of taking the schema on faith.
# MAGIC

# COMMAND ----------

@dlt.table(
    name=f"{CATALOG}.{BRONZE_SCHEMA}.bronze_bls_pr_txt",
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
    name=f"{CATALOG}.{BRONZE_SCHEMA}.bronze_population",
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