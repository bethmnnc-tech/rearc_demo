"""
Bronze layer.

Raw-as-landed ingestion of everything the `ingestion/` job dropped into the
Volume, via Auto Loader (`cloudFiles`) streaming tables.

Written against the pyspark.pipelines API (imported as `dp` below), the
current API for both open-source Apache Spark Declarative Pipelines and
Databricks Lakeflow Declarative Pipelines. The older `dlt` module still works
(Databricks keeps it for backward compatibility) but is now the legacy
import -- `pyspark.pipelines` is what Databricks recommends going forward,
and it's what actually ships in open-source Spark, so it's what this repo
uses. This is a plain .py source file, not a notebook: Spark Declarative
Pipelines source code is ordinary .py/.sql files (the pipeline UI's
"transformations" folder convention), not notebook cells -- there's no
"# Databricks notebook source" header or "# COMMAND ----------" cell markers
here on purpose.

Why Auto Loader / streaming tables here, not a batch spark.read: Auto Loader
tracks which files it has already processed (via its checkpoint), so
re-triggering this pipeline does NOT reprocess files it already ingested --
new/changed files are picked up incrementally. That's what satisfies "if we
re-run this, it shouldn't reprocess anything it's already ingested" at the
pipeline layer, on top of the ingestion job's own manifest-based skip logic
(which avoids the network call in the first place). `cloudFiles.allowOverwrites
= true` additionally means: if BLS revises pr.data.0.Current in place (same
filename, new content -- which is exactly how BLS publishes revisions), Auto
Loader will pick that up as a change rather than ignoring it because "we've
seen this path before."

Why bronze stays stringly-typed: Bronze should be a faithful, replayable copy
of the source. Trimming BLS's fixed-width padding, casting types, and
validating value ranges are all Silver's job -- that way if a cleaning rule
turns out to be wrong, Bronze still has the untouched original to reprocess
from.
"""

from pyspark import pipelines as dp
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
# tables in one schema), so every @dp.table below is registered with an
# explicit "{schema}.{name}" name -- and every downstream spark.read.table()
# call must reference that exact qualified string. See ingestion/common.py
# for why.


# --- pr.data.0.Current -------------------------------------------------
# This file's header/sample confirms it already carries the FULL history per
# series (its first rows for PRS30006011 start at 1995, not the current
# year), while pr.data.1.AllData is a larger, overlapping export of the same
# underlying series. We land both raw (below), but build Silver/Gold only
# from pr.data.0.Current to avoid double-counting the same observations from
# two overlapping sources -- see PROCESS.md for the full reasoning.

BLS_DATA_SCHEMA = StructType(
    [
        StructField("series_id", StringType(), True),
        StructField("year", StringType(), True),
        StructField("period", StringType(), True),
        StructField("value", StringType(), True),
        StructField("footnote_codes", StringType(), True),
    ]
)


@dp.table(
    name=f"{BRONZE_SCHEMA}.bronze_bls_data",
    comment="Raw pr.data.0.Current, one row per series_id/year/period as published by BLS.",
)
@dp.expect_or_drop("has_series_id", "series_id IS NOT NULL")
@dp.expect_or_drop("has_year", "year IS NOT NULL")
@dp.expect_or_drop("has_period", "period IS NOT NULL")
@dp.expect("has_value", "value IS NOT NULL")  # flag, don't drop -- a null value may still be worth keeping raw
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


# --- pr.data.1.AllData -- landed for completeness/auditability, not used downstream

@dp.table(
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


# --- pr.series -- series metadata (needed to build human-readable labels in Gold)

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


@dp.table(name=f"{BRONZE_SCHEMA}.bronze_bls_series", comment="Raw pr.series -- one row of metadata per series_id.")
@dp.expect_or_drop("has_series_id", "series_id IS NOT NULL")
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


# --- Small reference/lookup files ---------------------------------------
# pr.sector, pr.class, pr.measure, pr.duration, pr.seasonal, pr.footnote,
# pr.period all share the same shape (tab-separated, header row, a
# `*_code` -> `*_text` mapping). We land each as its own bronze table with
# an inferred schema -- they're tiny, low-risk reference data, and letting
# Spark infer types here (vs. hand-writing 7 near-identical StructTypes) is
# a deliberate low-ceremony choice for genuinely static lookup tables.
# pr.measure, pr.sector, pr.duration, and pr.class feed the Gold-layer
# human-readable labels; the rest are landed for completeness ("pull the
# full contents of the folder").
#
# We deliberately do NOT turn on cloudFiles.inferColumnTypes here:
# measure_code values like "01" are meaningful zero-padded codes, and type
# inference would silently turn them into the integer 1, breaking the join
# against pr.series.measure_code (also a padded string) in Silver.
# Everything lands as STRING; Silver casts only the columns that need it.

LOOKUP_FILES = ["pr.sector", "pr.class", "pr.measure", "pr.duration", "pr.seasonal", "pr.footnote", "pr.period"]

for _lookup_name in LOOKUP_FILES:
    # Table names can't contain ".", and the pipeline needs a distinct
    # function per table -- build one via a small factory instead of
    # copy/pasting the body 7 times.
    def _make_lookup_table(file_name):
        table_name = f"{BRONZE_SCHEMA}.bronze_bls_" + file_name.replace(".", "_")

        @dp.table(name=table_name, comment=f"Raw {file_name} reference/lookup file.")
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


# --- pr.txt -- BLS's own documentation for this survey ------------------
# Not tabular (it's the human-readable field dictionary BLS ships alongside
# the data -- series/data/mapping file formats, field definitions, update
# schedule), but it's still part of "the full contents of the folder," and
# it's the actual authoritative source this pipeline's schemas and the
# Silver-layer label logic were built against (see PROCESS.md) -- worth
# landing as its own bronze table for the same auditability reason as
# everything else here: so a reviewer can see exactly what we built
# against, not just take our word for the schema.

@dp.table(
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


# --- Population API (DataUSA) --------------------------------------------
# Landed as JSON; the API's field casing isn't pinned down in this repo (see
# PROCESS.md), so bronze intentionally keeps whatever columns the source
# actually returns rather than assuming exact names -- Silver normalizes them.

@dp.table(
    name=f"{BRONZE_SCHEMA}.bronze_population",
    comment="Raw DataUSA population API response, one row per (Year, Nation) record.",
)
@dp.expect("has_data_row", "true")  # placeholder row-level check; real validation happens post-explode in Silver
def bronze_population():
    raw = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("multiLine", "true")
        .option("cloudFiles.allowOverwrites", "true")
        .option("pathGlobFilter", "population_latest.json")
        .load(POPULATION_RAW_PATH)
    )
    return raw.select(F.explode("data").alias("record"), F.current_timestamp().alias("_ingested_at")).select(
        "record.*", "_ingested_at"
    )
