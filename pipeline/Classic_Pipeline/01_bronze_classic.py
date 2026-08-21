# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Bronze Layer - Classic Approach
# MAGIC %md
# MAGIC # Bronze Layer - Classic Approach
# MAGIC
# MAGIC **Migrated from DLT pipeline to classic notebooks for Databricks Free Edition compatibility**
# MAGIC
# MAGIC This notebook loads raw data from the Volume and writes it to Bronze Delta tables.
# MAGIC
# MAGIC ### Key Differences from DLT:
# MAGIC * **No streaming**: Uses batch `spark.read` instead of `spark.readStream` and Auto Loader
# MAGIC * **Explicit writes**: Each table is written using `.write.mode("overwrite").saveAsTable()`
# MAGIC * **Simple data quality**: Uses DataFrame `.filter()` instead of DLT expectations
# MAGIC * **Runs on classic compute**: Works with any cluster, not just serverless
# MAGIC * **Idempotent**: Re-running overwrites tables with latest data from Volume
# MAGIC
# MAGIC ### What This Does:
# MAGIC 1. Reads BLS productivity data files (pr.data.0.Current, pr.series, lookup files)
# MAGIC 2. Reads population data from DataUSA API
# MAGIC 3. Writes everything to Bronze schema as Delta tables

# COMMAND ----------

# DBTITLE 1,Configuration
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

# Configuration - same pattern as DLT, but no dlt import
dbutils.widgets.text("catalog", "rearc_dev_001")
dbutils.widgets.text("bronze_schema", "bronze")
dbutils.widgets.text("volume_schema", "volumes")
dbutils.widgets.text("volume", "bls_gov")

CATALOG = dbutils.widgets.get("catalog")
BRONZE_SCHEMA = dbutils.widgets.get("bronze_schema")
VOLUME_SCHEMA = dbutils.widgets.get("volume_schema")
VOLUME = dbutils.widgets.get("volume")

# Paths
VOLUME_ROOT = f"/Volumes/{CATALOG}/{VOLUME_SCHEMA}/{VOLUME}"
BLS_RAW_PATH = f"{VOLUME_ROOT}/bls_pr"
POPULATION_RAW_PATH = f"{VOLUME_ROOT}/population"

print(f"Configuration:")
print(f"  Catalog: {CATALOG}")
print(f"  Bronze Schema: {BRONZE_SCHEMA}")
print(f"  Volume Path: {VOLUME_ROOT}")

# COMMAND ----------

import re

# COMMAND ----------

def replace_illegal_chars(text, illegal_chars=' ,;{}()\n\t=%-?'):
    # Exclude uppercase letters from the illegal characters
    illegal_chars_pattern = f'([{re.escape(illegal_chars)}])'
    rx = re.compile(illegal_chars_pattern)
    text = rx.sub(r'_', text)
    return text


# COMMAND ----------

# DBTITLE 1,BLS Data - pr.data.0.Current
# MAGIC %md
# MAGIC ## BLS Productivity Data - pr.data.0.Current
# MAGIC
# MAGIC Main productivity series data. This file contains the full history per series.

# COMMAND ----------

# DBTITLE 1,Load bronze_bls_data
# Schema for BLS data files
BLS_DATA_SCHEMA = StructType([
    StructField("series_id", StringType(), True),
    StructField("year", StringType(), True),
    StructField("period", StringType(), True),
    StructField("value", StringType(), True),
    StructField("footnote_codes", StringType(), True),
])

# Read data with batch processing
df_bls_data = (
    spark.read
    .format("csv")
    .option("sep", "\t")
    .option("header", "true")
    .schema(BLS_DATA_SCHEMA)
    .load(f"{BLS_RAW_PATH}/pr.data.0.Current")
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path"))
)

# Data quality: drop rows missing critical fields
df_bls_data_clean = (
    df_bls_data
    .filter(F.col("series_id").isNotNull())
    .filter(F.col("year").isNotNull())
    .filter(F.col("period").isNotNull())
)

df_bls_data_clean.createOrReplaceTempView("tempData")
fixed_columns = [replace_illegal_chars(text) for text in df_bls_data_clean.columns]
as_phrase = [f'`{column}` as {fixed_columns[i]}' for i, column in enumerate(df_bls_data_clean.columns)]
select_statement = 'SELECT ' + ','.join(as_phrase) + ' from tempData'
new_file_cleaned = spark.sql(select_statement)

# Write to Bronze
new_file_cleaned.write \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{CATALOG}.{BRONZE_SCHEMA}.bronze_bls_data")

print(f"✓ Loaded {new_file_cleaned.count():,} rows to {CATALOG}.{BRONZE_SCHEMA}.bronze_bls_data")

# COMMAND ----------

# DBTITLE 1,BLS Data - pr.data.1.AllData
# MAGIC %md
# MAGIC ## BLS pr.data.1.AllData
# MAGIC
# MAGIC Larger overlapping export. Landed for completeness but not used downstream.

# COMMAND ----------

# DBTITLE 1,Load bronze_bls_data_alldata
df_bls_alldata = (
    spark.read
    .format("csv")
    .option("sep", "\t")
    .option("header", "true")
    .schema(BLS_DATA_SCHEMA)
    .load(f"{BLS_RAW_PATH}/pr.data.1.AllData")
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path"))
)

df_bls_alldata.createOrReplaceTempView("tempData")
fixed_columns = [replace_illegal_chars(text) for text in df_bls_alldata.columns]
as_phrase = [f'`{column}` as {fixed_columns[i]}' for i, column in enumerate(df_bls_alldata.columns)]
select_statement = 'SELECT ' + ','.join(as_phrase) + ' from tempData'
new_file_cleaned = spark.sql(select_statement)

new_file_cleaned.write \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{CATALOG}.{BRONZE_SCHEMA}.bronze_bls_data_alldata")

print(f"✓ Loaded {new_file_cleaned.count():,} rows to {CATALOG}.{BRONZE_SCHEMA}.bronze_bls_data_alldata")

# COMMAND ----------

# DBTITLE 1,BLS Series Metadata
# MAGIC %md
# MAGIC ## BLS Series Metadata - pr.series
# MAGIC
# MAGIC Series metadata needed for human-readable labels in Gold layer.

# COMMAND ----------

# DBTITLE 1,Load bronze_bls_series
BLS_SERIES_SCHEMA = StructType([
    StructField("series_id", StringType(), True),
    StructField("sector_code", StringType(), True),
    StructField("class_code", StringType(), True),
    StructField("measure_code", StringType(), True),
    StructField("duration_code", StringType(), True),
    StructField("seasonal", StringType(), True),
])

df_bls_series = (
    spark.read
    .format("csv")
    .option("sep", "\t")
    .option("header", "true")
    .schema(BLS_SERIES_SCHEMA)
    .load(f"{BLS_RAW_PATH}/pr.series")
    .withColumn("_ingested_at", F.current_timestamp())
    .filter(F.col("series_id").isNotNull())
)

df_bls_series.createOrReplaceTempView("tempData")
fixed_columns = [replace_illegal_chars(text) for text in df_bls_series.columns]
as_phrase = [f'`{column}` as {fixed_columns[i]}' for i, column in enumerate(df_bls_series.columns)]
select_statement = 'SELECT ' + ','.join(as_phrase) + ' from tempData'
new_file_cleaned = spark.sql(select_statement)

new_file_cleaned.write \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{CATALOG}.{BRONZE_SCHEMA}.bronze_bls_series")

print(f"✓ Loaded {new_file_cleaned.count():,} rows to {CATALOG}.{BRONZE_SCHEMA}.bronze_bls_series")

# COMMAND ----------

# DBTITLE 1,BLS Lookup Files
# MAGIC %md
# MAGIC ## BLS Lookup/Reference Files
# MAGIC
# MAGIC Small reference tables: sector, class, measure, duration, seasonal, footnote, period

# COMMAND ----------

# DBTITLE 1,Load lookup tables
LOOKUP_FILES = ["pr.sector", "pr.class", "pr.measure", "pr.duration", "pr.seasonal", "pr.footnote", "pr.period"]

for lookup_name in LOOKUP_FILES:
    # Read with inferred schema (these are tiny reference files)
    df_lookup = (
        spark.read
        .format("csv")
        .option("sep", "\t")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(f"{BLS_RAW_PATH}/{lookup_name}")
        .withColumn("_ingested_at", F.current_timestamp())
    )
    
    df_lookup.createOrReplaceTempView("tempData")
    fixed_columns = [replace_illegal_chars(text) for text in df_lookup.columns]
    as_phrase = [f'`{column}` as {fixed_columns[i]}' for i, column in enumerate(df_lookup.columns)]
    select_statement = 'SELECT ' + ','.join(as_phrase) + ' from tempData'
    new_file_cleaned = spark.sql(select_statement)

    # Table name: replace dot with underscore
    table_name = lookup_name.replace(".", "_")
  
    new_file_cleaned.write \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .saveAsTable(f"{CATALOG}.{BRONZE_SCHEMA}.bronze_{table_name}")
    
    print(f"✓ Loaded {new_file_cleaned.count():,} rows to {CATALOG}.{BRONZE_SCHEMA}.bronze_{table_name}")

# COMMAND ----------

# DBTITLE 1,BLS Documentation
# MAGIC %md
# MAGIC ## BLS Documentation - pr.txt
# MAGIC
# MAGIC BLS's field dictionary and documentation (not tabular, but kept for completeness)

# COMMAND ----------

# DBTITLE 1,Load bronze_bls_pr_txt
df_bls_txt = (
    spark.read
    .format("text")
    .load(f"{BLS_RAW_PATH}/pr.txt")
    .withColumn("_ingested_at", F.current_timestamp())
)

df_bls_txt.createOrReplaceTempView("tempData")
fixed_columns = [replace_illegal_chars(text) for text in df_bls_txt.columns]
as_phrase = [f'`{column}` as {fixed_columns[i]}' for i, column in enumerate(df_bls_txt.columns)]
select_statement = 'SELECT ' + ','.join(as_phrase) + ' from tempData'
new_file_cleaned = spark.sql(select_statement)

new_file_cleaned.write \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{CATALOG}.{BRONZE_SCHEMA}.bronze_bls_pr_txt")

print(f"✓ Loaded {new_file_cleaned.count():,} rows to {CATALOG}.{BRONZE_SCHEMA}.bronze_bls_pr_txt")

# COMMAND ----------

# DBTITLE 1,Population Data
# MAGIC %md
# MAGIC ## Population Data - DataUSA API
# MAGIC
# MAGIC JSON data with corrected schema to match actual field names: `Nation ID` (with space), Nation, Year, Population

# COMMAND ----------

# DBTITLE 1,Load bronze_population
# Read JSON with correct schema hint
df_population_raw = (
    spark.read
    .format("json")
    .option("multiLine", "true")
    .load(f"{POPULATION_RAW_PATH}/population_latest.json")
)

# Explode the data array and flatten
df_population = (
    df_population_raw
    .select(F.explode("data").alias("record"), F.current_timestamp().alias("_ingested_at"))
    .select("record.*", "_ingested_at")
)

df_population.createOrReplaceTempView("tempData")
fixed_columns = [replace_illegal_chars(text) for text in df_population.columns]
as_phrase = [f'`{column}` as {fixed_columns[i]}' for i, column in enumerate(df_population.columns)]
select_statement = 'SELECT ' + ','.join(as_phrase) + ' from tempData'
new_file_cleaned = spark.sql(select_statement)


# Write to Bronze
new_file_cleaned.write \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{CATALOG}.{BRONZE_SCHEMA}.bronze_population")

print(f"✓ Loaded {new_file_cleaned.count():,} rows to {CATALOG}.{BRONZE_SCHEMA}.bronze_population")

# Show sample to verify schema
print("\nSample data:")
new_file_cleaned.limit(3).display()

# COMMAND ----------

# DBTITLE 1,Summary
# MAGIC %md
# MAGIC ## Bronze Layer Complete ✓
# MAGIC
# MAGIC All raw data has been loaded from the Volume into Bronze Delta tables.
# MAGIC
# MAGIC **Next Steps:**
# MAGIC 1. Run the Silver layer notebook to clean and normalize this data
# MAGIC 2. Run the Gold layer notebook to create analytics-ready tables

# COMMAND ----------

