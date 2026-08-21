# PROCESS.md

## Architecture

**Medallion layout, one Lakeflow Declarative Pipeline.** Bronze holds every
file BLS publishes in `pr/`, landed with minimal transformation and kept
string-typed, so a bad cleaning rule downstream never costs me the original
data. Silver trims, types, deduplicates, and (for the series dimension) joins
in the sector/measure lookup tables to build a human-readable label — that
join belongs in Silver, not Gold, because "what is this series actually
measuring" is a property of the series, not of any one analytical question.
Gold holds exactly the three tables the quest asks for, one per question, each
with an `EXPECT` constraint that fails the update if the shape of the answer
looks wrong (e.g. Q1 always having six years, Q2/Q3 never having a null key).

**Bronze, Silver, and Gold are three separate schemas** (`bronze` / `silver` /
`gold` in the `rearc_dev_001` catalog), not three sets of prefixed tables
sitting in one schema. Every table in `pipeline/*.py`/`03_gold_sql.sql` is
registered with an explicit `{schema}.{table}` name rather than relying on
the pipeline's single default target schema. The payoff shows up directly in
the access-control bonus: `resources/grants_readonly_analyst.sql` grants a
read-only analyst `SELECT` on the whole `gold` schema in one statement,
automatically covering any Gold table added later, with no way for that
grant to accidentally leak into Bronze/Silver.

**Bronze uses Auto Loader (streaming tables); Silver/Gold are materialized
views (full recompute).** The requirement that re-running ingestion
"shouldn't reprocess anything it's already ingested" is really two separate
problems, and I solved them at two different layers:

1. *Don't re-download from BLS unnecessarily.* The ingestion job
   (`ingestion/bls_ingest.py`) keeps a Delta manifest table of every file's
   `(name, size, last_modified)` and only downloads a file if it's new or
   changed. This is about being a polite, well-behaved client of a public
   government server, not about correctness inside the pipeline.
2. *Don't reprocess files already ingested into Bronze.* This is Auto
   Loader's actual job — its checkpoint tracks which files it has already
   read, so re-triggering the pipeline is a no-op for unchanged files. I set
   `cloudFiles.allowOverwrites = true` because BLS republishes revisions
   under the *same* filename (`pr.data.0.Current` gets revised in place, not
   renamed) — without that option Auto Loader would ignore a legitimate
   revision because it had already seen that path.

   Silver and Gold, by contrast, are plain materialized views: they recompute
   from the full upstream table on every pipeline update. At this data volume
   (Bronze BLS data is ~1.6MB) that's both correct and cheap, and it avoids
   the real complexity of incremental dedup/joins (see Trade-offs below for
   what changes at scale).

**No hardcoded filenames.** `bls_ingest.py` discovers what's in the BLS
directory by parsing `<a href>` links off the live index page, not from a
fixed list — a file BLS adds or removes is picked up automatically. I
deliberately did *not* try to regex-parse the file size/date out of the
index page's HTML formatting (Apache/nginx autoindex templates are easy to
get subtly wrong); instead I HEAD each discovered file for authoritative
`Content-Length` / `Last-Modified` headers, which is also what the manifest
diff is keyed on.

**`pr.data.0.Current`, not `pr.data.1.AllData`, feeds Silver/Gold.** Both
files are landed in Bronze ("pull the full contents of the folder" means
literally that), but I only build downstream from `pr.data.0.Current` — its
sample rows already go back to 1995 for the series I checked, confirming
despite the filename that it's a full history, not a "this year only" file.
Building Silver from *both* files would double-count every observation that
appears in both (which, given the naming, is most of them) without a clean
way to tell which copy is authoritative. If it later turns out `AllData`
contains series that `Current` doesn't, that's a one-line change to Silver's
source — it's already landed and ready.

**SQL is the primary implementation for all three Gold questions; PySpark is
the documented alternate — and both actually run.** Rather than write the
PySpark version once as a comment or dead code, I wired
`03_gold_pyspark_alt.py`'s three tables into the pipeline as real
`*_pyspark_alt` tables, and added `04_gold_verification.py`, which diffs each
primary table against its alternate (`exceptAll` both directions) and fails
the pipeline if they ever disagree. The verification tables were honestly the
most satisfying part — watching two completely different implementations land
on identical results is just... nice. I went with SQL as primary because all
three questions are basically set operations (`GROUP BY`, a window function, a
`LEFT JOIN`) that read way more naturally as SQL; PySpark earns its keep
upstream in Bronze/Silver, where I actually need imperative control —
retry/backoff loops against two flaky external sources, a per-file table
factory for the lookup files, and defensive column-name matching for the
population API.

**Q2's "quarters" excludes `Q05`.** Per `pr.txt` (BLS's own documentation for
this survey, landed as `bronze_bls_pr_txt` and quoted directly here): "period
for which data is observed (M13, Q05, and S03 indicate annual averages)."
`M13`/`S03` are the annual-average markers for *other* BLS surveys that use
monthly/semiannual periods — this text is a shared template across BLS time-
series docs. The PR survey itself is quarterly-only (`pr.txt` Section 1:
"Quarterly measures are based entirely on seasonally adjusted data"; Section
4/7 show period values are exclusively `Q01`–`Q05`), so `Q05` is the only
annual-average code that actually appears in `pr.data.0.Current`. Summing
`Q01`–`Q05` per series would silently double most of the true annual total.
Ties (two years with an identical summed value) are broken toward the
earlier year — arbitrary but deterministic, and documented in the SQL itself
since the spec doesn't say.

**The Gold label uses `duration_code` and `class_code`, not just sector and
measure.** `pr.txt` Section 7 defines `duration_code` as identifying "whether
data are percent changes or indexes" (e.g. "% change year ago" vs. an index
level) and `class_code` as the "employee group to which data pertain." Both
change how a bare `value` should be read for the same sector/measure
combination, so `silver_bls_series` joins `pr.class` and `pr.duration` in
alongside `pr.sector` and `pr.measure` to build the label — e.g.
`"Manufacturing: Labor productivity (output per hour), % change from same
quarter a year ago, all persons, Seasonally Adjusted"` — rather than stopping
at sector + measure, which would leave two series that measure the same
thing in different units (an index vs. its percent change) with
indistinguishable labels.

## Trade-offs

Things I'd do differently for a real client, roughly in order of how much I'd
push on them:

- **Schema drift.** Bronze pins an explicit schema for the two files that
  feed everything downstream (`pr.data.0.Current`, `pr.series`) so a
  silently-added or reordered BLS column fails loudly (schema mismatch)
  instead of quietly shifting data into the wrong field. The small lookup
  files use inferred/loose schemas, which is fine for six-row reference
  tables but wouldn't fly for anything that mattered more — I'd add the same
  explicit-schema-plus-expectation treatment there too, and consider
  `mergeSchema`/schema evolution settings if BLS's own format changed
  intentionally.
- **Data volume.** Everything here fits comfortably in memory, so I went with
  full-recompute materialized views for Silver/Gold and just used
  `dropDuplicates` as a batch operation. At real client scale I'd move Silver
  to `dlt.read_stream` with `dropDuplicatesWithinWatermark` or an `APPLY
  CHANGES INTO` target, and think harder about partitioning Gold by a natural
  key (year, sector) rather than recomputing the whole thing every run.
- **Cost.** The ingestion job spins up a fresh single-node cluster; for a
  daily few-MB pull that's pretty wasteful — I'd move it to serverless or a
  shared small cluster pool. The pipeline itself is small enough that its
  default cluster sizing is fine, but I'd definitely watch the DBU cost of
  the (currently daily) schedule once we know real usage patterns and probably
  relax it — BLS updates this survey quarterly, not daily.
- **Access control.** Bronze/Silver/Gold are three separate schemas (not one
  schema with prefixed table names) specifically so this could be a clean
  schema-level grant — `resources/grants_readonly_analyst.sql` grants an
  analyst group `SELECT` on the entire `gold` schema in one statement, which
  automatically covers any Gold table added later. Pretty standard stuff. For
  a real rollout I'd still want: actual identity federation (SCIM-synced
  groups, not a hand-typed group name), grants applied through Terraform/DABs
  rather than a one-off SQL script, and a real decision about whether
  Bronze/Silver need row- or column-level restrictions (this dataset is public
  BLS data, so it doesn't — a real client dataset often would).
- **Monitoring.** Right now, "did it work" means opening the pipeline UI and
  checking the DQ panel. I'd add alerting on pipeline failure and on any
  `EXPECT ... ON VIOLATION FAIL UPDATE` firing (not just logging it), a
  dashboard tracking expectation pass rates over time (a slow drift in
  `has_value` failures is worth catching early), and probably a dead-letter
  table for dropped rows so we'd know if we're silently discarding stuff.
- **The manifest table is at-least-once, not exactly-once.** If the job dies
  after downloading a file but before the manifest MERGE commits, the next
  run re-downloads that file — harmless here, just worth calling out.

## Retrospective

The hardest part honestly wasn't the Spark logic — it was figuring out BLS's
*actual* file formats without being able to treat any of it as obvious.
`pr.series` looks like it should have a `series_title` column (most BLS
surveys do); this one doesn't — just numeric codes (`sector_code`,
`class_code`, `measure_code`, `duration_code`), so the "human-readable label"
the quest asks for doesn't exist anywhere in the raw data until you build it
by joining `pr.series` against the mapping files. `pr.txt` — BLS's own field
dictionary for this survey — is what actually settles this: Section 6/7
confirms `pr.class`, `pr.duration`, `pr.measure`, and `pr.sector` are each a
`*_code -> *_text` mapping file, and that `duration_code` distinguishes
percent-changes from indexes while `class_code` identifies the employee
group. Both of those matter for reading a `value` correctly, not just
cosmetic detail, which is why the label ended up joining four lookup tables
(sector, measure, duration, class), not two. I initially built it from just
sector + measure, pulling the schema from the live files' header rows; going
back to `pr.txt` directly is what surfaced the gap. That's the "read the
provider's own documentation instead of assuming stuff" habit the quest calls
out — it's easy to *say* you did that, but the test is whether the second
pass over the docs still changes your code.

A close second: `measure_code` values like `"01"` are meaningful zero-padded
strings, not the integer `1`. Took me an embarrassingly long time to realize
turning on naive schema inference on the lookup files would silently mangle
that and break the label join — wouldn't even throw an error, just quietly
produce `NULL` labels for measures `01`–`09`. That's the kind of bug that's
obvious once you know to look for it and invisible otherwise, which is why
Bronze explicitly avoids `cloudFiles.inferColumnTypes`.

One thing I couldn't fully verify during scaffolding: I wasn't able to fetch
a live response from the DataUSA population API to confirm its exact JSON
field casing (`Year` vs `year`, whether `Nation` is present, etc.). Silver's
population parsing (`_find_col` in `pipeline/02_silver.py`) matches column
names case-insensitively with a couple of known variants rather than assuming
exact casing, specifically because of that gap. **First thing to check on the
actual run:** confirm the real field names and tighten that matching back
down to an explicit schema.

## AI usage disclosure

Coming Soon!!  Testing DAB Soon