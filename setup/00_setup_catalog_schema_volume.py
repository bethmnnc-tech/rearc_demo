# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # One-time setup: Catalog / Schemas / Volume
# MAGIC
# MAGIC Creates the Unity Catalog objects everything else in this repo assumes:
# MAGIC one catalog, a `bronze` / `silver` / `gold` schema per medallion layer (kept
# MAGIC separate specifically so Gold can get its own schema-level read-only grant
# MAGIC -- see `resources/grants_readonly_analyst.sql`), and a `volumes` schema
# MAGIC holding the raw-file Volume. Idempotent (`IF NOT EXISTS` throughout) --
# MAGIC safe to re-run.
# MAGIC
# MAGIC On **Databricks Free Edition**, `CATALOG` defaults to the workspace's
# MAGIC default catalog if you don't have permission to create new catalogs --
# MAGIC change `rdq_catalog` below to an existing catalog you can write to if
# MAGIC `CREATE CATALOG` fails for you.

# COMMAND ----------

dbutils.widgets.text("rdq_catalog", "rearc_dev_001")
dbutils.widgets.text("rdq_bronze_schema", "bronze")
dbutils.widgets.text("rdq_silver_schema", "silver")
dbutils.widgets.text("rdq_gold_schema", "gold")
dbutils.widgets.text("rdq_volume_schema", "volumes")
dbutils.widgets.text("rdq_volume", "bls_gov")

CATALOG = dbutils.widgets.get("rdq_catalog")
BRONZE_SCHEMA = dbutils.widgets.get("rdq_bronze_schema")
SILVER_SCHEMA = dbutils.widgets.get("rdq_silver_schema")
GOLD_SCHEMA = dbutils.widgets.get("rdq_gold_schema")
VOLUME_SCHEMA = dbutils.widgets.get("rdq_volume_schema")
VOLUME = dbutils.widgets.get("rdq_volume")

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")

for schema in [BRONZE_SCHEMA, SILVER_SCHEMA, GOLD_SCHEMA, VOLUME_SCHEMA]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{VOLUME_SCHEMA}.{VOLUME}")

# Sub-directories for each raw source.
for sub in ["bls_pr", "population"]:
    dbutils.fs.mkdirs(f"/Volumes/{CATALOG}/{VOLUME_SCHEMA}/{VOLUME}/{sub}")

print(f"Ready: /Volumes/{CATALOG}/{VOLUME_SCHEMA}/{VOLUME}")
print(f"Schemas: {CATALOG}.{BRONZE_SCHEMA}, {CATALOG}.{SILVER_SCHEMA}, {CATALOG}.{GOLD_SCHEMA}, {CATALOG}.{VOLUME_SCHEMA}")
