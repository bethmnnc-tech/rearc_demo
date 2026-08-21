# Databricks notebook source
# DBTITLE 1,Gold Layer - Classic Approach
# MAGIC %md
# MAGIC # Gold Layer - Classic Approach
# MAGIC
# MAGIC **Migrated from DLT pipeline to classic notebooks**
# MAGIC
# MAGIC This notebook creates analytics-ready Gold tables that answer the three Rearc Data Quest questions:
# MAGIC
# MAGIC 1. **Q1**: Mean & standard deviation of US population (2013-2018)
# MAGIC 2. **Q2**: Best year per BLS series (year with largest summed quarterly value)
# MAGIC 3. **Q3**: PRS30006032 Q01 values joined with population
# MAGIC
# MAGIC ### Key Changes from DLT:
# MAGIC * Uses `CREATE OR REPLACE TABLE` instead of `CREATE OR REFRESH MATERIALIZED VIEW`
# MAGIC * No DLT constraints - validation happens via DataFrame filters before write
# MAGIC * Reads from Silver using standard table references

# COMMAND ----------

# DBTITLE 1,Configuration
dbutils.widgets.text("catalog", "rearc_dev_001")
dbutils.widgets.text("silver_schema", "silver")
dbutils.widgets.text("gold_schema", "gold")

CATALOG = dbutils.widgets.get("catalog")
SILVER_SCHEMA = dbutils.widgets.get("silver_schema")
GOLD_SCHEMA = dbutils.widgets.get("gold_schema")


print(f"Configuration:")
print(f"  Catalog: {CATALOG}")
print(f"  Silver Schema: {SILVER_SCHEMA}")
print(f"  Gold Schema: {GOLD_SCHEMA}")

# Set context for SQL cells
spark.sql(f"USE CATALOG {CATALOG}")

# COMMAND ----------

# DBTITLE 1,Q1: Population Statistics
# MAGIC %md
# MAGIC ## Q1 - Population Statistics (2013-2018)
# MAGIC
# MAGIC Calculate mean and sample standard deviation of US population for years 2013-2018 inclusive.
# MAGIC
# MAGIC `STDDEV()` in Spark SQL is the sample standard deviation (Bessel-corrected, dividing by N-1).

# COMMAND ----------

# DBTITLE 1,Create gold_population_stats
sql_1 = f"""
CREATE OR REPLACE TABLE  {CATALOG}.{GOLD_SCHEMA}.gold_population_stats
COMMENT 'Q1: Mean & sample stddev of annual US population, 2013-2018 inclusive.'
AS
SELECT
  2013 AS start_year,
  2018 AS end_year,
  COUNT(*) AS years_included,
  MEAN(population) AS mean_population,
  STDDEV(population) AS stddev_samp_population,
  STDDEV_POP(population) AS stddev_pop_population
FROM {CATALOG}.{SILVER_SCHEMA}.silver_population
WHERE year BETWEEN 2013 AND 2018
"""
spark.sql(sql_1)

# COMMAND ----------

# DBTITLE 1,Verify Q1 results
# Verify the results
q1_result = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.gold_population_stats")
print("Q1 Results:")
q1_result.display()

# Data quality check
assert q1_result.count() == 1, "Should have exactly 1 row"
assert q1_result.select("years_included").first()[0] == 6, "Should have all 6 years"
print("✓ Q1 validation passed")

# COMMAND ----------

# DBTITLE 1,Q2: Best Year per Series
# MAGIC %md
# MAGIC ## Q2 - Best Year per Series
# MAGIC
# MAGIC For each BLS series, find the year with the largest summed quarterly value.
# MAGIC
# MAGIC * Only includes quarters Q01-Q04 (excludes Q05, the annual average)
# MAGIC * Ties are broken by taking the earlier year for deterministic results
# MAGIC * Includes human-readable series labels

# COMMAND ----------

# DBTITLE 1,Create gold_bls_best_year_per_series
sql_2 = f"""
CREATE OR REPLACE TABLE {CATALOG}.{GOLD_SCHEMA}.gold_bls_best_year_per_series
COMMENT 'Q2: The year with the largest summed quarterly value per series_id, with human-readable label.'
AS
WITH yearly_sums AS (
  SELECT 
    series_id, 
    year, 
    SUM(value) AS summed_value
  FROM {CATALOG}.{SILVER_SCHEMA}.silver_bls_data
  WHERE period IN ('Q01', 'Q02', 'Q03', 'Q04')
  GROUP BY series_id, year
),
ranked AS (
  SELECT
    series_id,
    year,
    summed_value,
    ROW_NUMBER() OVER (PARTITION BY series_id ORDER BY summed_value DESC, year ASC) AS rn
  FROM yearly_sums
)
SELECT
  r.series_id,
  COALESCE(s.series_label, 'Unknown series: ' || r.series_id) AS series_label,
  r.year AS best_year,
  r.summed_value AS best_year_summed_value
FROM ranked r
LEFT JOIN {CATALOG}.{SILVER_SCHEMA}.silver_bls_series s 
  ON r.series_id = s.series_id
WHERE r.rn = 1
"""
spark.sql(sql_2)

# COMMAND ----------

# DBTITLE 1,Verify Q2 results
# Verify the results
q2_result = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.gold_bls_best_year_per_series")
print(f"Q2 Results: {q2_result.count():,} series")
print("\nSample best years:")
q2_result.limit(10).display()

# Data quality check
assert q2_result.filter("series_id IS NULL OR best_year IS NULL").count() == 0, "All rows should have series_id and best_year"
print("✓ Q2 validation passed")

# COMMAND ----------

# DBTITLE 1,Q3: PRS30006032 Q01 with Population
# MAGIC %md
# MAGIC ## Q3 - PRS30006032, Q01 Values with Population
# MAGIC
# MAGIC Show PRS30006032 series, period Q01 values by year, joined with population where available.
# MAGIC
# MAGIC * LEFT JOIN so years without population data are still included (population will be NULL)

# COMMAND ----------

# DBTITLE 1,Create gold_prs30006032_q01_population
sql_3 = f"""
CREATE OR REPLACE TABLE {CATALOG}.{GOLD_SCHEMA}.gold_prs30006032_q01_population
COMMENT 'Q3: PRS30006032 / Q01 value by year, left-joined to population.'
AS
SELECT
  d.year,
  d.value,
  p.population
FROM {CATALOG}.{SILVER_SCHEMA}.silver_bls_data d
LEFT JOIN {CATALOG}.{SILVER_SCHEMA}.silver_population p 
  ON d.year = p.year
WHERE d.series_id = 'PRS30006032' 
  AND d.period = 'Q01'
ORDER BY d.year
"""
spark.sql(sql_3)

# COMMAND ----------

# DBTITLE 1,Verify Q3 results
# Verify the results
q3_result = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.gold_prs30006032_q01_population")
print(f"Q3 Results: {q3_result.count():,} years")
print("\nAll years:")
q3_result.display()

# Data quality check
assert q3_result.filter("year IS NULL OR value IS NULL").count() == 0, "All rows should have year and value"
print("✓ Q3 validation passed")

# COMMAND ----------

# DBTITLE 1,Summary
# MAGIC %md
# MAGIC ## Gold Layer Complete ✓
# MAGIC
# MAGIC All three analytical questions have been answered:
# MAGIC
# MAGIC 1. ✓ **Population Statistics** (2013-2018)
# MAGIC 2. ✓ **Best Year per BLS Series**
# MAGIC 3. ✓ **PRS30006032 Q01 with Population**
# MAGIC
# MAGIC All Gold tables are now available in `${rdq.catalog}.${rdq.gold_schema}.*`

# COMMAND ----------

