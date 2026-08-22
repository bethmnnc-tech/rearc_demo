"""
Silver layer.

Typed, cleaned, deduplicated, and (for the series dimension) enriched with
legible labels.

Why @dp.materialized_view (full recompute) instead of streaming here: Bronze
is where "don't reprocess files we've already ingested" matters, and Auto
Loader handles that. Silver's job -- dedup, type casting, joins against small
dimension tables -- is naturally a whole-table operation, and at this data
volume (a couple MB) a full recompute on every pipeline update is both
correct and cheap. If this dataset were multi-TB and append-only, we'd switch
Silver to a streaming @dp.table + a windowed dropDuplicatesWithinWatermark,
or an auto-CDC ("APPLY CHANGES INTO") target -- see PROCESS.md's trade-offs
section.

Plain .py source file (not a notebook) -- see the header comment in
01_bronze.py for why, and for the dlt -> pyspark.pipelines note.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

BRONZE_SCHEMA = spark.conf.get("rdq.bronze_schema", "bronze")
SILVER_SCHEMA = spark.conf.get("rdq.silver_schema", "silver")


# --- silver_bls_data ------------------------------------------------------
# Cleans bronze_bls_data: trims BLS's fixed-width padding, casts types, drops
# exact-duplicate rows (BLS occasionally republishes a file with no actual
# content change, which would otherwise show up as dupes after a Bronze
# re-ingest), and validates the period code shape.

VALID_PERIOD_RE = r"^Q0[1-5]$"


@dp.materialized_view(
    name=f"{SILVER_SCHEMA}.silver_bls_data",
    comment="Cleaned/typed BLS productivity observations: one row per series_id/year/period.",
)
@dp.expect_or_drop("valid_series_id", "series_id IS NOT NULL AND series_id != ''")
@dp.expect_or_drop("valid_year", "year IS NOT NULL AND year BETWEEN 1900 AND 2100")
@dp.expect_or_drop("valid_period", f"period RLIKE '{VALID_PERIOD_RE}'")
@dp.expect("has_value", "value IS NOT NULL")
def silver_bls_data():
    return (
        spark.read.table(f"{BRONZE_SCHEMA}.bronze_bls_data")
        .select(
            F.trim(F.col("series_id")).alias("series_id"),
            F.col("year").cast("int").alias("year"),
            F.trim(F.col("period")).alias("period"),
            F.col("value").cast("double").alias("value"),
            F.trim(F.col("footnote_codes")).alias("footnote_codes"),
        )
        .dropDuplicates(["series_id", "year", "period"])
    )


# --- silver_bls_series ------------------------------------------------------
# Joins the series dimension against the sector/measure/duration/class/
# seasonal lookups to build a legible label per series -- pr.series on
# its own only has opaque numeric codes (sector_code, measure_code,
# duration_code, class_code...), not descriptive text. Per pr.txt (Section
# 6/7, the BLS-supplied field dictionary for this survey): measure_code is
# "the specific factor measured" (e.g. Unit labor costs), duration_code
# distinguishes "percent changes [or] indexes" (e.g. "% change year ago" vs.
# an index level) -- this materially changes how a value should be read, so
# it belongs in the label, not just the measure -- and class_code identifies
# the "employee group to which data pertain." All four, plus seasonal
# adjustment status, go into the label.

@dp.materialized_view(
    name=f"{SILVER_SCHEMA}.silver_bls_series",
    comment="Series metadata enriched with a legible label, e.g. "
    "'Manufacturing: Labor productivity (output per hour), % change from same quarter a year ago, "
    "all persons, Seasonally Adjusted'.",
)
@dp.expect_or_drop("valid_series_id", "series_id IS NOT NULL AND series_id != ''")
def silver_bls_series():
    series = spark.read.table(f"{BRONZE_SCHEMA}.bronze_bls_series").select(
        F.trim(F.col("series_id")).alias("series_id"),
        F.trim(F.col("sector_code")).alias("sector_code"),
        F.trim(F.col("class_code")).alias("class_code"),
        F.trim(F.col("measure_code")).alias("measure_code"),
        F.trim(F.col("duration_code")).alias("duration_code"),
        F.trim(F.col("seasonal")).alias("seasonal"),
    )
    sector = spark.read.table(f"{BRONZE_SCHEMA}.bronze_bls_pr_sector").select(
        F.trim(F.col("sector_code").cast("string")).alias("sector_code"),
        F.col("sector_name"),
    )
    measure = spark.read.table(f"{BRONZE_SCHEMA}.bronze_bls_pr_measure").select(
        F.trim(F.col("measure_code").cast("string")).alias("measure_code"),
        F.col("measure_text"),
    )
    duration = spark.read.table(f"{BRONZE_SCHEMA}.bronze_bls_pr_duration").select(
        F.trim(F.col("duration_code").cast("string")).alias("duration_code"),
        F.col("duration_text"),
    )
    class_ = spark.read.table(f"{BRONZE_SCHEMA}.bronze_bls_pr_class").select(
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


# --- silver_population ------------------------------------------------------
# Normalizes column names defensively: DataUSA's Tesseract API is expected to
# return Year and Population fields, but we match case-insensitively and
# tolerate a few known variants ("ID Year") rather than hard-failing on exact
# casing we haven't been able to verify against a live response -- see
# PROCESS.md's retrospective for why.

def _find_col(df, *candidates):
    lower_map = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in lower_map:
            return lower_map[cand.lower()]
    raise ValueError(f"None of {candidates} found in columns {df.columns}")


@dp.materialized_view(
    name=f"{SILVER_SCHEMA}.silver_population", comment="Cleaned, typed, deduplicated annual US population."
)
@dp.expect_or_drop("valid_year", "year IS NOT NULL")
@dp.expect_or_drop("valid_population", "population IS NOT NULL AND population > 0")
def silver_population():
    bronze = spark.read.table(f"{BRONZE_SCHEMA}.bronze_population")
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
