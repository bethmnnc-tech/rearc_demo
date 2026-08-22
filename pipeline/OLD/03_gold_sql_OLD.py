# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC
# MAGIC # Overview
# MAGIC
# MAGIC This notebook implements the **Gold layer** of a medallion architecture data pipeline, answering three analytical questions:
# MAGIC
# MAGIC 1. **Q1**: Mean and standard deviation of annual US population, 2013-2018 inclusive
# MAGIC 2. **Q2**: Best year per series_id (largest summed quarterly value) from BLS data
# MAGIC 3. **Q3**: PRS30006032 series Q01 values by year, joined with population data
# MAGIC
# MAGIC The notebook uses **Spark SQL** as the primary implementation, creating materialized views with declarative data quality constraints. Each Gold table includes:
# MAGIC - Built-in data quality expectations (constraints)
# MAGIC - Inline documentation of business logic and assumptions
# MAGIC
# MAGIC A parallel PySpark implementation exists in `03_gold_pyspark_alt.py` for comparison and verification purposes (`04_gold_verification.py` diffs both implementations).
# MAGIC
# MAGIC **Parameterization**: Schema names are configurable via widgets (`catalog`,`rdq_silver_schema`, `rdq_gold_schema`). SQL statements are wrapped in Python f-strings and executed via `spark.sql()`.
# MAGIC
# MAGIC **Data sources**: Silver-layer tables (`silver_population`, `silver_bls_data`, `silver_bls_series`)
# MAGIC

# COMMAND ----------

# DBTITLE 1,Setup variables from widgets
dbutils.widgets.text("catalog", "rearc_dev_001")
dbutils.widgets.text("rdq_silver_schema", "silver")
dbutils.widgets.text("rdq_gold_schema", "gold")

CATALOG = dbutils.widgets.get("catalog")
SILVER_SCHEMA = dbutils.widgets.get("rdq_silver_schema")
GOLD_SCHEMA = dbutils.widgets.get("rdq_gold_schema")
print(CATALOG)
print(SILVER_SCHEMA)
print(GOLD_SCHEMA)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q1 -- mean & stddev of annual US population, 2013-2018 inclusive
# MAGIC
# MAGIC `STDDEV()` in Spark SQL is the sample standard deviation (`STDDEV_SAMP`,
# MAGIC Bessel-corrected, dividing by N-1) -- the conventional default when treating
# MAGIC these six years as a sample. `STDDEV_POP` is included alongside it so the
# MAGIC choice is visible rather than silently baked in.

# COMMAND ----------

# DBTITLE 1,Q1: Population stats
# Q1: Mean & stddev of annual US population, 2013-2018 inclusive for this demo
sql_q1 = f"""
CREATE OR REFRESH MATERIALIZED VIEW {CATALOG}.{GOLD_SCHEMA}.gold_population_stats (
  CONSTRAINT has_all_six_years EXPECT (years_included = 6) ON VIOLATION FAIL UPDATE,
  CONSTRAINT stats_not_null EXPECT (mean_population IS NOT NULL AND stddev_samp_population IS NOT NULL) ON VIOLATION FAIL UPDATE
)
COMMENT 'Q1 (primary, Spark SQL): mean & sample stddev of annual US population, 2013-2018 inclusive.'
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

spark.sql(sql_q1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q2 -- best year per series_id (largest summed quarterly value)
# MAGIC
# MAGIC "Quarters" = periods `Q01`-`Q04`; `Q05` (BLS's annual-average marker) is
# MAGIC excluded so it isn't double-counted against the four real quarters. Ties
# MAGIC (two years with an identical summed value) are broken by taking the
# MAGIC earlier year, for a deterministic result -- documented here since the spec
# MAGIC doesn't say.

# COMMAND ----------

# DBTITLE 1,Q2: Best year per series
# Q2: Best year per series_id (largest summed quarterly value)
sql_q2 = f"""
CREATE OR REFRESH MATERIALIZED VIEW {CATALOG}.{GOLD_SCHEMA}.gold_bls_best_year_per_series (
  CONSTRAINT has_series_id EXPECT (series_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT has_best_year EXPECT (best_year IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'The year with the largest summed quarterly value, per series_id'
AS
WITH yearly_sums AS (
  SELECT series_id, year, SUM(value) AS summed_value
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
LEFT JOIN {CATALOG}.{SILVER_SCHEMA}.silver_bls_series s ON r.series_id = s.series_id
WHERE r.rn = 1
"""

spark.sql(sql_q2)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q3 -- PRS30006032, period Q01: value by year, joined with that year's population
# MAGIC
# MAGIC A LEFT JOIN, per the spec ("joined... where available") -- population is
# MAGIC `NULL` for any year the DataUSA API doesn't cover.

# COMMAND ----------

# DBTITLE 1,Q3: PRS30006032 Q01 with population
# Q3: PRS30006032, period Q01: value by year, joined with that year's population for this demo
sql_q3 = f"""
CREATE OR REFRESH MATERIALIZED VIEW {CATALOG}.{GOLD_SCHEMA}.gold_prs30006032_q01_population (
  CONSTRAINT has_year EXPECT (year IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT has_value EXPECT (value IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "Q3 (primary, Spark SQL): PRS30006032 / Q01 value by year, left-joined to that year's population."
AS
SELECT
  d.year,
  d.value,
  p.population
FROM {CATALOG}.{SILVER_SCHEMA}.silver_bls_data d
LEFT JOIN {CATALOG}.{SILVER_SCHEMA}.silver_population p ON d.year = p.year
WHERE d.series_id = 'PRS30006032' AND d.period = 'Q01'
ORDER BY d.year
"""

spark.sql(sql_q3)