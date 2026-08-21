# Databricks notebook source
# MAGIC %md
# MAGIC # Verification: SQL primary vs. PySpark alternate
# MAGIC
# MAGIC For each Gold question, this diffs the SQL-primary table against its
# MAGIC PySpark-alternate twin (symmetric `EXCEPT` both directions) and fails the
# MAGIC pipeline update if they ever disagree. This is the actual proof that
# MAGIC "equally fluent in both" produced the *same* answer, not just similar-looking
# MAGIC code -- and it doubles as a regression check if either implementation is
# MAGIC edited later.

# COMMAND ----------

import dlt
from pyspark.sql import functions as F

GOLD_SCHEMA = spark.conf.get("rdq.gold_schema", "gold")

# COMMAND ----------


def _symmetric_diff_count(left, right):
    return left.exceptAll(right).count() + right.exceptAll(left).count()


# COMMAND ----------

@dlt.table(
    name=f"{GOLD_SCHEMA}.gold_verification_q1",
    comment="Row count of differences between gold_population_stats and its PySpark alternate. Should always be 0.",
)
@dlt.expect_or_fail("sql_and_pyspark_agree", "diff_row_count = 0")
def gold_verification_q1():
    primary = dlt.read(f"{GOLD_SCHEMA}.gold_population_stats")
    alt = dlt.read(f"{GOLD_SCHEMA}.gold_population_stats_pyspark_alt")
    return spark.createDataFrame(
        [(_symmetric_diff_count(primary, alt),)], schema="diff_row_count INT"
    )


@dlt.table(
    name=f"{GOLD_SCHEMA}.gold_verification_q2",
    comment="Row count of differences between gold_bls_best_year_per_series and its PySpark alternate. Should always be 0.",
)
@dlt.expect_or_fail("sql_and_pyspark_agree", "diff_row_count = 0")
def gold_verification_q2():
    primary = dlt.read(f"{GOLD_SCHEMA}.gold_bls_best_year_per_series")
    alt = dlt.read(f"{GOLD_SCHEMA}.gold_bls_best_year_per_series_pyspark_alt")
    return spark.createDataFrame(
        [(_symmetric_diff_count(primary, alt),)], schema="diff_row_count INT"
    )


@dlt.table(
    name=f"{GOLD_SCHEMA}.gold_verification_q3",
    comment="Row count of differences between gold_prs30006032_q01_population and its PySpark alternate. Should always be 0.",
)
@dlt.expect_or_fail("sql_and_pyspark_agree", "diff_row_count = 0")
def gold_verification_q3():
    primary = dlt.read(f"{GOLD_SCHEMA}.gold_prs30006032_q01_population")
    alt = dlt.read(f"{GOLD_SCHEMA}.gold_prs30006032_q01_population_pyspark_alt")
    return spark.createDataFrame(
        [(_symmetric_diff_count(primary, alt),)], schema="diff_row_count INT"
    )
