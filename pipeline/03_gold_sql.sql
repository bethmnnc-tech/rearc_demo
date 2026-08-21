-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Gold layer -- Spark SQL (primary)
-- MAGIC
-- MAGIC Spark SQL is the **primary** implementation for all three analytical
-- MAGIC questions; PySpark equivalents live in `03_gold_pyspark_alt.py` (also
-- MAGIC wired into the pipeline, as `*_pyspark_alt` tables) so both are actually
-- MAGIC executed every run and can be diffed against each other -- see
-- MAGIC `04_gold_verification.py`.
-- MAGIC
-- MAGIC Why SQL as primary: these three questions are set-based aggregations and
-- MAGIC joins (GROUP BY, window function, a left join) that read most naturally as
-- MAGIC SQL, and materialized views let each Gold table declare its own
-- MAGIC freshness/quality contract right next to the query. PySpark earns its keep
-- MAGIC upstream in Bronze/Silver, where we need imperative control (retries, a
-- MAGIC dynamic per-file table factory, defensive column-name lookups for the
-- MAGIC population API).
-- MAGIC
-- MAGIC Table/schema names below are hardcoded (`gold.`, `silver.`) rather than
-- MAGIC parameterized -- unlike the Python pipeline files, this plain SQL source
-- MAGIC has no access to the pipeline's `spark.conf` values. If you rename the
-- MAGIC `silver`/`gold` schemas, update the references in this file to match.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q1 -- mean & stddev of annual US population, 2013-2018 inclusive
-- MAGIC
-- MAGIC `STDDEV()` in Spark SQL is the sample standard deviation (`STDDEV_SAMP`,
-- MAGIC Bessel-corrected, dividing by N-1) -- the conventional default when treating
-- MAGIC these six years as a sample. `STDDEV_POP` is included alongside it so the
-- MAGIC choice is visible rather than silently baked in.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW gold.gold_population_stats (
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
FROM silver.silver_population
WHERE year BETWEEN 2013 AND 2018;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q2 -- best year per series_id (largest summed quarterly value)
-- MAGIC
-- MAGIC "Quarters" = periods `Q01`-`Q04`; `Q05` (BLS's annual-average marker) is
-- MAGIC excluded so it isn't double-counted against the four real quarters. Ties
-- MAGIC (two years with an identical summed value) are broken by taking the
-- MAGIC earlier year, for a deterministic result -- documented here since the spec
-- MAGIC doesn't say.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW gold.gold_bls_best_year_per_series (
  CONSTRAINT has_series_id EXPECT (series_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT has_best_year EXPECT (best_year IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Q2 (primary, Spark SQL): the year with the largest summed quarterly value, per series_id, with a human-readable label.'
AS
WITH yearly_sums AS (
  SELECT series_id, year, SUM(value) AS summed_value
  FROM silver.silver_bls_data
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
LEFT JOIN silver.silver_bls_series s ON r.series_id = s.series_id
WHERE r.rn = 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q3 -- PRS30006032, period Q01: value by year, joined with that year's population
-- MAGIC
-- MAGIC A LEFT JOIN, per the spec ("joined... where available") -- population is
-- MAGIC `NULL` for any year the DataUSA API doesn't cover.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW gold.gold_prs30006032_q01_population (
  CONSTRAINT has_year EXPECT (year IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT has_value EXPECT (value IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "Q3 (primary, Spark SQL): PRS30006032 / Q01 value by year, left-joined to that year's population."
AS
SELECT
  d.year,
  d.value,
  p.population
FROM silver.silver_bls_data d
LEFT JOIN silver.silver_population p ON d.year = p.year
WHERE d.series_id = 'PRS30006032' AND d.period = 'Q01'
ORDER BY d.year;