"""
Verification: SQL primary vs. PySpark alternate.

For each Gold question, this diffs the SQL-primary table against its
PySpark-alternate twin (symmetric EXCEPT both directions) and fails the
pipeline update if they ever disagree. This is the actual proof that
"equally fluent in both" produced the SAME answer, not just similar-looking
code -- and it doubles as a regression check if either implementation is
edited later.

Plain .py source file (not a notebook) -- see the header comment in
01_bronze.py for why, and for the dlt -> pyspark.pipelines note.
"""

from pyspark import pipelines as dp

GOLD_SCHEMA = spark.conf.get("rdq.gold_schema", "gold")


def _symmetric_diff_count(left, right):
    return left.exceptAll(right).count() + right.exceptAll(left).count()


@dp.materialized_view(
    name=f"{GOLD_SCHEMA}.gold_verification_q1",
    comment="Row count of differences between gold_population_stats and its PySpark alternate. Should always be 0.",
)
@dp.expect_or_fail("sql_and_pyspark_agree", "diff_row_count = 0")
def gold_verification_q1():
    primary = spark.read.table(f"{GOLD_SCHEMA}.gold_population_stats")
    alt = spark.read.table(f"{GOLD_SCHEMA}.gold_population_stats_pyspark_alt")
    return spark.createDataFrame(
        [(_symmetric_diff_count(primary, alt),)], schema="diff_row_count INT"
    )


@dp.materialized_view(
    name=f"{GOLD_SCHEMA}.gold_verification_q2",
    comment="Row count of differences between gold_bls_best_year_per_series and its PySpark alternate. Should always be 0.",
)
@dp.expect_or_fail("sql_and_pyspark_agree", "diff_row_count = 0")
def gold_verification_q2():
    primary = spark.read.table(f"{GOLD_SCHEMA}.gold_bls_best_year_per_series")
    alt = spark.read.table(f"{GOLD_SCHEMA}.gold_bls_best_year_per_series_pyspark_alt")
    return spark.createDataFrame(
        [(_symmetric_diff_count(primary, alt),)], schema="diff_row_count INT"
    )


@dp.materialized_view(
    name=f"{GOLD_SCHEMA}.gold_verification_q3",
    comment="Row count of differences between gold_prs30006032_q01_population and its PySpark alternate. Should always be 0.",
)
@dp.expect_or_fail("sql_and_pyspark_agree", "diff_row_count = 0")
def gold_verification_q3():
    primary = spark.read.table(f"{GOLD_SCHEMA}.gold_prs30006032_q01_population")
    alt = spark.read.table(f"{GOLD_SCHEMA}.gold_prs30006032_q01_population_pyspark_alt")
    return spark.createDataFrame(
        [(_symmetric_diff_count(primary, alt),)], schema="diff_row_count INT"
    )
