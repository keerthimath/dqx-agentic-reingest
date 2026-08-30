# Raw Jobs API payloads

Kept for teams that deploy via the REST API / Terraform instead of the DAB in
`../databricks.yml` + `../resources/`. **The bundle is the recommended path** —
if you change the DAG here, mirror it in `resources/dqx_agentic_reingest_job.yml` (or
drop this directory once the bundle is the only deploy path in use).

Databricks For Each tasks wrap a single task, so the per-quarantine-table
pipeline is a **child job** the parent fans out to:

1. Create the child job first:
   `databricks jobs create --json @dqx_agentic_reingest_process_job.json`
2. Put the returned `job_id` into `dqx_agentic_reingest_parent_job.json`
   (`run_job_task.job_id`, currently `0`).
3. Create the parent:
   `databricks jobs create --json @dqx_agentic_reingest_parent_job.json`

Both payloads have catalog/schema names (`main.dqx_studio.*`) hardcoded — the
DAB templates these from bundle variables per target instead.

`dqx_dry_run_job.json` is the standalone read-only estimate (`01` + For Each →
`07_dry_run_report`); it has no child job and can be created on its own. The
parent/process pair also carries a `dry_run` job parameter (default `false`)
that, when `true`, holds `05`'s Silver MERGE.
