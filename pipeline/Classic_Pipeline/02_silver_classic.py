# Databricks notebook source
# DBTITLE 1,Silver Layer - Classic Approach
# MAGIC %md
# MAGIC # Silver Layer - Classic Approach
# MAGIC
# MAGIC **Migrated from DLT pipeline to classic notebooks**
# MAGIC
# MAGIC This notebook cleans, types, and enriches Bronze data into Silver Delta tables.
# MAGIC
# MAGIC ### What This Does:
# MAGIC 1. **silver_bls_data**: Clean and type BLS productivity data, remove duplicates
# MAGIC 2. **silver_bls_series**: Enrich series metadata with human-readable labels by joining lookup tables
# MAGIC 3. **silver_population**: Normalize population data column names and types
# MAGIC
# MAGIC ### Key Changes from DLT:
# MAGIC * Reads from Bronze using `spark.table()` instead of `dlt.read()`
# MAGIC * Data quality checks use `.filter()` instead of `@dlt.expect_or_drop`
# MAGIC * Explicit writes with `saveAsTable()`

# COMMAND ----------

# DBTITLE 1,Configuration
from pyspark.sql import functions as F
import re

dbutils.widgets.text("catalog", "rearc_dev_001")
dbutils.widgets.text("bronze_schema", "bronze")
dbutils.widgets.text("silver_schema", "silver")

CATALOG = dbutils.widgets.get("catalog")
BRONZE_SCHEMA = dbutils.widgets.get("bronze_schema")
SILVER_SCHEMA = dbutils.widgets.get("silver_schema")

print(f"Configuration:")
print(f"  Catalog: {CATALOG}")
print(f"  Bronze Schema: {BRONZE_SCHEMA}")
print(f"  Silver Schema: {SILVER_SCHEMA}")

# COMMAND ----------

# DBTITLE 1,Silver BLS Data
# MAGIC %md
# MAGIC ## silver_bls_data
# MAGIC
# MAGIC Cleans bronze_bls_data: trims fixed-width padding, casts types, removes duplicates, validates period codes.

# COMMAND ----------

# DBTITLE 1,Load silver_bls_data
VALID_PERIOD_RE = r"^Q0[1-5]$"

# Read from Bronze
df_bls_data_bronze = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.bronze_bls_data")

# Clean and type
df_silver_bls_data = (
    df_bls_data_bronze
    .select(
        F.trim(F.col("series_id")).alias("series_id"),
        F.col("year").cast("int").alias("year"),
        F.trim(F.col("period")).alias("period"),
        F.col("value").cast("double").alias("value"),
        F.trim(F.col("footnote_codes")).alias("footnote_codes"),
    )
    .dropDuplicates(["series_id", "year", "period"])
)

# Data quality: filter invalid records
df_silver_bls_data_clean = (
    df_silver_bls_data
    .filter(F.col("series_id").isNotNull())
    .filter(F.col("series_id") != "")
    .filter(F.col("year").isNotNull())
    .filter(F.col("year").between(1900, 2100))
    .filter(F.col("period").rlike(VALID_PERIOD_RE))
)

# Write to Silver
df_silver_bls_data_clean.write \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.silver_bls_data")

print(f"✓ Loaded {df_silver_bls_data_clean.count():,} rows to {CATALOG}.{SILVER_SCHEMA}.silver_bls_data")

# COMMAND ----------

# DBTITLE 1,Silver BLS Series
# MAGIC %md
# MAGIC ## silver_bls_series
# MAGIC
# MAGIC Enrich series metadata by joining sector, measure, duration, class, and seasonal lookup tables to build human-readable labels.
# MAGIC
# MAGIC Example label: "Manufacturing: Labor productivity (output per hour), % change from same quarter a year ago, all persons, Seasonally Adjusted"

# COMMAND ----------

# DBTITLE 1,Load silver_bls_series
# Read series and lookup tables
series = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.bronze_bls_series").select(
    F.trim(F.col("series_id")).alias("series_id"),
    F.trim(F.col("sector_code")).alias("sector_code"),
    F.trim(F.col("class_code")).alias("class_code"),
    F.trim(F.col("measure_code")).alias("measure_code"),
    F.trim(F.col("duration_code")).alias("duration_code"),
    F.trim(F.col("seasonal")).alias("seasonal"),
)

sector = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.bronze_pr_sector").select(
    F.trim(F.col("sector_code").cast("string")).alias("sector_code"),
    F.col("sector_name"),
)

measure = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.bronze_pr_measure").select(
    F.trim(F.col("measure_code").cast("string")).alias("measure_code"),
    F.col("measure_text"),
)

duration = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.bronze_pr_duration").select(
    F.trim(F.col("duration_code").cast("string")).alias("duration_code"),
    F.col("duration_text"),
)

class_tbl = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.bronze_pr_class").select(
    F.trim(F.col("class_code").cast("string")).alias("class_code"),
    F.col("class_text"),
)

# Build seasonal label
seasonal_label = (
    F.when(F.col("seasonal") == "S", F.lit("Seasonally Adjusted"))
    .when(F.col("seasonal") == "U", F.lit("Not Seasonally Adjusted"))
    .otherwise(F.col("seasonal"))
)

# Join all lookups and build label
df_silver_series = (
    series
    .join(sector, "sector_code", "left")
    .join(measure, "measure_code", "left")
    .join(duration, "duration_code", "left")
    .join(class_tbl, "class_code", "left")
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

# Data quality: drop rows without series_id
df_silver_series_clean = (
    df_silver_series
    .filter(F.col("series_id").isNotNull())
    .filter(F.col("series_id") != "")
)

# Write to Silver
df_silver_series_clean.write \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.silver_bls_series")

print(f"✓ Loaded {df_silver_series_clean.count():,} rows to {CATALOG}.{SILVER_SCHEMA}.silver_bls_series")
print("\nSample labels:")
df_silver_series_clean.select("series_id", "series_label").limit(3).display()

# COMMAND ----------

# DBTITLE 1,Silver Population
# MAGIC %md
# MAGIC ## silver_population
# MAGIC
# MAGIC Normalize population data: handle various column name casing from the DataUSA API, cast types, remove duplicates.

# COMMAND ----------

# DBTITLE 1,Load silver_population
def find_col(df, *candidates):
    """Find column by case-insensitive match from candidates"""
    lower_map = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in lower_map:
            return lower_map[cand.lower()]
    raise ValueError(f"None of {candidates} found in columns {df.columns}")

# Read from Bronze
bronze_population = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.bronze_population")

# Find columns (handle various casings from the API)
year_col = find_col(bronze_population, "Year", "ID Year")
pop_col = find_col(bronze_population, "Population")

# Try to find nation column (optional)
nation_col_candidates = [c for c in bronze_population.columns if c.lower() in ("nation", "id nation", "nation id")]
nation_col = nation_col_candidates[0] if nation_col_candidates else None

# Build select list
select_cols = [
    F.col(year_col).cast("int").alias("year"),
    F.col(pop_col).cast("long").alias("population"),
]
if nation_col:
    select_cols.append(F.col(nation_col).alias("nation"))

# Transform and deduplicate
df_silver_population = (
    bronze_population
    .select(*select_cols)
    .dropDuplicates(["year"])
)

# Data quality: filter invalid records
df_silver_population_clean = (
    df_silver_population
    .filter(F.col("year").isNotNull())
    .filter(F.col("population").isNotNull())
    .filter(F.col("population") > 0)
)

# Write to Silver
df_silver_population_clean.write \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.silver_population")

print(f"✓ Loaded {df_silver_population_clean.count():,} rows to {CATALOG}.{SILVER_SCHEMA}.silver_population")
print("\nSample data:")
df_silver_population_clean.orderBy(F.col("year").desc()).limit(5).display()

# COMMAND ----------

# DBTITLE 1,Summary
# MAGIC %md
# MAGIC ## Silver Layer Complete ✓
# MAGIC
# MAGIC All Bronze data has been cleaned, typed, and enriched into Silver Delta tables.
# MAGIC
# MAGIC **Next Step:** Run the Gold layer notebook to create analytics-ready aggregated tables.

# COMMAND ----------

