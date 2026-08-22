# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Silver layer
# MAGIC
# MAGIC Typed, cleaned, deduplicated, and (for the series dimension) enriched with
# MAGIC legible labels.
# MAGIC
# MAGIC **Why materialized views (`dlt.read`, full recompute) instead of streaming
# MAGIC here:** Bronze is where "don't reprocess files we've already ingested"
# MAGIC matters, and Auto Loader handles that. Silver's job -- dedup, type
# MAGIC casting, joins against small dimension tables -- is naturally a
# MAGIC whole-table operation, and at this data volume (a couple MB) a full
# MAGIC recompute on every pipeline update is both correct and cheap. If this
# MAGIC dataset were multi-TB and append-only, we'd switch Silver to
# MAGIC `dlt.read_stream` + a windowed `dropDuplicatesWithinWatermark`, or an
# MAGIC `APPLY CHANGES INTO` target -- see PROCESS.md's trade-offs section.

# COMMAND ----------

import dlt
from pyspark.sql import functions as F


# COMMAND ----------

CATALOG = spark.conf.get("rdq.catalog", "rearc_dev_001")
BRONZE_SCHEMA = spark.conf.get("rdq.bronze_schema", "bronze")
SILVER_SCHEMA = spark.conf.get("rdq.silver_schema", "silver")

# COMMAND ----------

# MAGIC %md
# MAGIC ## `silver_bls_data`
# MAGIC
# MAGIC Cleans `bronze_bls_data`: trims BLS's fixed-width padding, casts types,
# MAGIC drops exact-duplicate rows (BLS occasionally republishes a file with no
# MAGIC actual content change, which would otherwise show up as dupes after a
# MAGIC Bronze re-ingest), and validates the period code shape.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Data quality expectations
# MAGIC
# MAGIC The `silver_bls_data` table below enforces quality expectations through DLT's `@dlt.expect_or_drop()` decorator:
# MAGIC
# MAGIC - **`valid_series_id`**: Drops rows with null or empty series IDs
# MAGIC - **`valid_year`**: Drops years outside the plausible range 1900–2100
# MAGIC - **`valid_period`**: Drops period codes that don't match BLS's quarterly format (`Q01`–`Q05`)
# MAGIC - **`has_value`**: Logs a warning (but keeps the row) when the observation value is null
# MAGIC
# MAGIC These rules ensure downstream Gold-layer consumers can safely join and aggregate without defensive null-handling.
# MAGIC

# COMMAND ----------

VALID_PERIOD_RE = r"^Q0[1-5]$" #looking for valid Quarter Numbers - Q01-Q05 (Q05 is Full Year Summary)


@dlt.table(
    name=f"{CATALOG}.{SILVER_SCHEMA}.silver_bls_data",
    comment="Cleaned/typed BLS productivity observations: one row per series_id/year/period.",
)
@dlt.expect_or_drop("valid_series_id", "series_id IS NOT NULL AND series_id != ''")
@dlt.expect_or_drop("valid_year", "year IS NOT NULL AND year BETWEEN 1900 AND 2100")
@dlt.expect_or_drop("valid_period", f"period RLIKE '{VALID_PERIOD_RE}'")
@dlt.expect("has_value", "value IS NOT NULL")
def silver_bls_data():
    return (
        dlt.read(f"{CATALOG}.{BRONZE_SCHEMA}.bronze_bls_data")
        .select(
            F.trim(F.col("series_id")).alias("series_id"),
            F.col("year").cast("int").alias("year"),
            F.trim(F.col("period")).alias("period"),
            F.col("value").cast("double").alias("value"),
            F.trim(F.col("footnote_codes")).alias("footnote_codes"),
        )
        .dropDuplicates(["series_id", "year", "period"])
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## `silver_bls_series`
# MAGIC
# MAGIC Joins series metadata with sector/measure/duration/class/seasonal lookups
# MAGIC to create readable labels. The raw `pr.series` table only gives us numeric
# MAGIC codes like `sector_code` and `measure_code` -- not much help when you're
# MAGIC trying to figure out what a series actually tracks.
# MAGIC
# MAGIC According to the BLS field dictionary (`pr.txt`, sections 6/7):
# MAGIC - `measure_code` tells you what's being measured (unit labor costs, etc.)
# MAGIC - `duration_code` is critical because it tells you whether the value is a
# MAGIC   percent change or an index level -- same number, completely different
# MAGIC   meaning
# MAGIC - `class_code` identifies which employee group the data covers
# MAGIC
# MAGIC We roll all four codes plus seasonal adjustment into one label so
# MAGIC downstream users don't have to decode anything.
# MAGIC

# COMMAND ----------

@dlt.table(
    name=f"{CATALOG}.{SILVER_SCHEMA}.silver_bls_series",
    comment="Series metadata enriched with a legible label, e.g. "
    "'Manufacturing: Labor productivity (output per hour), % change from same quarter a year ago, "
    "all persons, Seasonally Adjusted'.",
)
@dlt.expect_or_drop("valid_series_id", "series_id IS NOT NULL AND series_id != ''")
def silver_bls_series():
    series = dlt.read(f"{CATALOG}.{BRONZE_SCHEMA}.bronze_bls_series").select(
        F.trim(F.col("series_id")).alias("series_id"),
        F.trim(F.col("sector_code")).alias("sector_code"),
        F.trim(F.col("class_code")).alias("class_code"),
        F.trim(F.col("measure_code")).alias("measure_code"),
        F.trim(F.col("duration_code")).alias("duration_code"),
        F.trim(F.col("seasonal")).alias("seasonal"),
    )
    sector = dlt.read(f"{CATALOG}.{BRONZE_SCHEMA}.bronze_bls_pr_sector").select(
        F.trim(F.col("sector_code").cast("string")).alias("sector_code"),
        F.col("sector_name"),
    )
    measure = dlt.read(f"{CATALOG}.{BRONZE_SCHEMA}.bronze_bls_pr_measure").select(
        F.trim(F.col("measure_code").cast("string")).alias("measure_code"),
        F.col("measure_text"),
    )
    duration = dlt.read(f"{CATALOG}.{BRONZE_SCHEMA}.bronze_bls_pr_duration").select(
        F.trim(F.col("duration_code").cast("string")).alias("duration_code"),
        F.col("duration_text"),
    )
    class_ = dlt.read(f"{CATALOG}.{BRONZE_SCHEMA}.bronze_bls_pr_class").select(
        F.trim(F.col("class_code").cast("string")).alias("class_code"),
        F.col("class_text"),
    )

    seasonal_label = (
        F.when(F.col("seasonal") == "S", F.lit("Seasonally Adjusted"))
        .when(F.col("seasonal") == "U", F.lit("Not Seasonally Adjusted"))
        .otherwise(F.col("seasonal"))
    )

    return (
        series.join(sector, "sector_code", "left")
        .join(measure, "measure_code", "left")
        .join(duration, "duration_code", "left")
        .join(class_, "class_code", "left")
        .withColumn(
            "series_label",
            F.concat_ws(
                ": ",
                F.coalesce(F.col("sector_name"), F.lit("Unknown sector")),
                F.concat_ws(
                    ", ",
                    F.coalesce(F.col("measure_text"), F.lit("Unknown measure")),
                    F.col("duration_text"),
                    F.col("class_text"),
                    seasonal_label,
                ),
            ),
        )
        .select(
            "series_id",
            "sector_code",
            "sector_name",
            "measure_code",
            "measure_text",
            "duration_code",
            "duration_text",
            "class_code",
            "class_text",
            "seasonal",
            "series_label",
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## `silver_population`
# MAGIC
# MAGIC The DataUSA API should give us `Year` and `Population`, but in practice
# MAGIC the casing can vary—sometimes it's `ID Year` instead. Rather than assume
# MAGIC the exact field names and risk breaking on the next API change, we search
# MAGIC for them case-insensitively. (See PROCESS.md for the story of why.)

# COMMAND ----------

def _find_col(df, *candidates):
    lower_map = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in lower_map:
            return lower_map[cand.lower()]
    raise ValueError(f"None of {candidates} found in columns {df.columns}")


@dlt.table(
    name=f"{SILVER_SCHEMA}.silver_population", comment="Cleaned, typed, deduplicated annual US population."
)
@dlt.expect_or_drop("valid_year", "year IS NOT NULL")
@dlt.expect_or_drop("valid_population", "population IS NOT NULL AND population > 0")
def silver_population():
    bronze = dlt.read(f"{CATALOG}.{BRONZE_SCHEMA}.bronze_population")
    year_col = _find_col(bronze, "Year", "ID Year")
    pop_col = _find_col(bronze, "Population")
    nation_col_candidates = [c for c in bronze.columns if c.lower() in ("nation", "id nation")]
    nation_col = nation_col_candidates[0] if nation_col_candidates else None

    select_cols = [
        F.col(year_col).cast("int").alias("year"),
        F.col(pop_col).cast("long").alias("population"),
    ]
    if nation_col:
        select_cols.append(F.col(nation_col).alias("nation"))

    return bronze.select(*select_cols).dropDuplicates(["year"])