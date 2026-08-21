# Databricks notebook source
# MAGIC %md
# MAGIC # Gold layer -- PySpark (documented alternate)
# MAGIC
# MAGIC Same three questions as `03_gold_sql.sql`, implemented with the PySpark
# MAGIC DataFrame API instead of Spark SQL. These are wired into the pipeline as
# MAGIC real tables (not just left in a notebook) so they actually run every
# MAGIC update and can be diffed against the SQL primary in
# MAGIC `04_gold_verification.py` -- "equivalent alternate" here means "provably
# MAGIC produces the same answer," not just "looks similar."

# COMMAND ----------

import dlt
from pyspark.sql import functions as F
from pyspark.sql import Window

SILVER_SCHEMA = spark.conf.get("rdq.silver_schema", "silver")
GOLD_SCHEMA = spark.conf.get("rdq.gold_schema", "gold")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q1 (alternate, PySpark)
# MAGIC

# COMMAND ----------

@dlt.table(
    name=f"{GOLD_SCHEMA}.gold_population_stats_pyspark_alt",
    comment="Q1 (alternate, PySpark): same result as gold_population_stats, via the DataFrame API.",
)
@dlt.expect_or_fail("has_all_six_years", "years_included = 6")
def gold_population_stats_pyspark_alt():
    df = dlt.read(f"{SILVER_SCHEMA}.silver_population").filter(F.col("year").between(2013, 2018))
    return df.select(
        F.lit(2013).alias("start_year"),
        F.lit(2018).alias("end_year"),
        F.count("*").alias("years_included"),
        F.mean("population").alias("mean_population"),
        F.stddev_samp("population").alias("stddev_samp_population"),
        F.stddev_pop("population").alias("stddev_pop_population"),
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q2 (alternate, PySpark)
# MAGIC

# COMMAND ----------

@dlt.table(
    name=f"{GOLD_SCHEMA}.gold_bls_best_year_per_series_pyspark_alt",
    comment="Q2 (alternate, PySpark): same result as gold_bls_best_year_per_series, via the DataFrame API.",
)
@dlt.expect_or_drop("has_series_id", "series_id IS NOT NULL")
@dlt.expect_or_drop("has_best_year", "best_year IS NOT NULL")
def gold_bls_best_year_per_series_pyspark_alt():
    quarterly = dlt.read(f"{SILVER_SCHEMA}.silver_bls_data").filter(
        F.col("period").isin("Q01", "Q02", "Q03", "Q04")
    )

    yearly_sums = quarterly.groupBy("series_id", "year").agg(F.sum("value").alias("summed_value"))

    # Same tie-break as the SQL version: highest sum first, earliest year wins ties.
    window = Window.partitionBy("series_id").orderBy(F.col("summed_value").desc(), F.col("year").asc())
    ranked = yearly_sums.withColumn("rn", F.row_number().over(window)).filter(F.col("rn") == 1)

    series = dlt.read(f"{SILVER_SCHEMA}.silver_bls_series").select("series_id", "series_label")

    return (
        ranked.join(series, "series_id", "left")
        .withColumn(
            "series_label",
            F.coalesce(F.col("series_label"), F.concat(F.lit("Unknown series: "), F.col("series_id"))),
        )
        .select(
            "series_id",
            "series_label",
            F.col("year").alias("best_year"),
            F.col("summed_value").alias("best_year_summed_value"),
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q3 (alternate, PySpark)

# COMMAND ----------

@dlt.table(
    name=f"{GOLD_SCHEMA}.gold_prs30006032_q01_population_pyspark_alt",
    comment="Q3 (alternate, PySpark): same result as gold_prs30006032_q01_population, via the DataFrame API.",
)
@dlt.expect_or_drop("has_year", "year IS NOT NULL")
def gold_prs30006032_q01_population_pyspark_alt():
    data = dlt.read(f"{SILVER_SCHEMA}.silver_bls_data").filter(
        (F.col("series_id") == "PRS30006032") & (F.col("period") == "Q01")
    )
    population = dlt.read(f"{SILVER_SCHEMA}.silver_population")

    return data.join(population, on="year", how="left").select("year", "value", "population").orderBy("year")