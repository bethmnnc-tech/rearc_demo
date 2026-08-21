# Screenshot

The quest asks for a screenshot of your tables/pipeline in Databricks, since
the reviewer may not have access to your live workspace.

After running the pipeline, grab one (or a few) screenshots showing:

- The Lakeflow Declarative Pipeline DAG (Bronze -> Silver -> Gold, including
  the `*_pyspark_alt` and `gold_verification_*` tables), from the pipeline's
  **Graph** view.
- The data-quality/expectations panel showing the `EXPECT` constraints
  passing.
- Optionally, a `SELECT * FROM gold_...` result for each of the three
  questions.

Drop the image file(s) in this folder (e.g. `pipeline_dag.png`,
`gold_results.png`) before you zip up the repo to submit.
