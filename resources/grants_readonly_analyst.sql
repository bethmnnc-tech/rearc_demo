-- Bonus: Unity Catalog access controls for a read-only analyst consuming the
-- Gold layer only.
--
-- Run this manually (as a workspace/catalog admin) after the pipeline has
-- created its tables. Adjust the group name to match your workspace's
-- identity setup -- `analysts` here is a placeholder Databricks account
-- group.
--
-- Design: Gold lives in its own schema (`gold`), separate from `bronze` and
-- `silver` -- see ingestion/common.py for why. That's what makes this a
-- clean *schema-level* grant instead of a table-by-table one: an analyst
-- gets read access to everything in Gold, automatically including any new
-- Gold table added later, without ever being able to see Bronze/Silver or
-- the raw Volume. Bronze/Silver contain raw and intermediate data an analyst
-- shouldn't need (and Bronze in particular is an unfiltered mirror of a
-- public government source here, but the principle generalizes to any
-- client dataset where raw/intermediate layers may carry PII or licensing
-- restrictions Gold has already stripped out).

-- Let the group see the catalog exists, without granting anything inside it yet.
GRANT USE CATALOG ON CATALOG rearc_dev_001 TO `analysts`;

-- Read-only on the entire gold schema -- covers gold_population_stats,
-- gold_bls_best_year_per_series, gold_prs30006032_q01_population, and any
-- Gold table added later, in one grant.
GRANT USE SCHEMA ON SCHEMA rearc_dev_001.gold TO `analysts`;
GRANT SELECT ON SCHEMA rearc_dev_001.gold TO `analysts`;

-- Explicitly do NOT grant USE SCHEMA / SELECT on `bronze` or `silver`, and do
-- NOT grant READ VOLUME on `rearc_dev_001.volumes.bls_gov` -- an analyst
-- consuming Gold has no need for any of them.
