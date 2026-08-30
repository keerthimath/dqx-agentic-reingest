# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC ## 04 · Apply fix + re-validate
# MAGIC The deterministic gate, for both playbook and agent remediations. Runs
# MAGIC once per quarantine FQN (inside the child job's task graph).
# MAGIC
# MAGIC Picks up review-queue rows for this FQN that are either:
# MAGIC   - `status = 'curated'` with `confidence >= threshold` (playbook rows are
# MAGIC     always 1.0 and clear the bar), or
# MAGIC   - `status = 'revalidated_fail'` with `retry_count < max_retries` (a
# MAGIC     previous pass patched them but they still failed — try again).
# MAGIC
# MAGIC Branches on `remediation_action`:
# MAGIC   - `drop` — resolved by removal, no re-validation (usually already set to
# MAGIC     `resolved_duplicate` by 03a; handled here defensively).
# MAGIC   - `keep` / `patch_fields` — apply the column patch, strip the quarantine
# MAGIC     `_*` bookkeeping columns, then re-run the **exact** DQX checks that
# MAGIC     quarantined the row (loaded from the same rules table `dqx-validator`
# MAGIC     used, via `Validator.approved_checks_for`).

# COMMAND ----------
dbutils.widgets.text("quarantine_fqn", "")
dbutils.widgets.text("review_queue_fqn", "main.dqx_studio._dq_review_queue")
dbutils.widgets.text("env", "dev")
dbutils.widgets.text("rules_table", "")  # override; blank => derived from env via the wheel's tables.yaml
dbutils.widgets.text("lib_path", "/Workspace/dqx-agentic-reingest/notebooks/lib")
dbutils.widgets.text("confidence_threshold", "0.8")
dbutils.widgets.text("max_retries", "3")

quarantine_fqn = dbutils.widgets.get("quarantine_fqn")
review_queue_fqn = dbutils.widgets.get("review_queue_fqn")
env = dbutils.widgets.get("env")
rules_table = dbutils.widgets.get("rules_table").strip() or None
lib_path = dbutils.widgets.get("lib_path")
confidence_threshold = float(dbutils.widgets.get("confidence_threshold"))
max_retries = int(dbutils.widgets.get("max_retries"))

staged_table = f"{quarantine_fqn}_staged_for_reingest"

# COMMAND ----------
from pyspark.sql import functions as F
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient
from dqx_validator import Validator

dq_engine = DQEngine(WorkspaceClient())

# COMMAND ----------
queue = spark.table(review_queue_fqn).filter(F.col("quarantine_fqn") == quarantine_fqn)

actionable = queue.filter(
    ((F.col("status") == "curated") & (F.col("confidence") >= confidence_threshold))
    | ((F.col("status") == "revalidated_fail") & (F.col("retry_count") < max_retries))
)

n_actionable = actionable.count()
print(f"{n_actionable} actionable review rows for {quarantine_fqn}")

if n_actionable == 0:
    dbutils.jobs.taskValues.set(key="staged_table", value="")
    dbutils.jobs.taskValues.set(key="silver_fqn", value="")
    dbutils.jobs.taskValues.set(key="merge_key", value="")
    dbutils.jobs.taskValues.set(key="passed_count", value=0)
    dbutils.notebook.exit("0")

# COMMAND ----------
# --- 'drop' rows: resolved by removal, never re-validated / reingested.
drop_ids = [r.row_id for r in
            actionable.filter(F.col("remediation_action") == "drop")
                      .select("row_id").collect()]
if drop_ids:
    spark.sql(f"""
        UPDATE {review_queue_fqn}
        SET status = 'resolved_removed', updated_at = current_timestamp()
        WHERE quarantine_fqn = '{quarantine_fqn}'
          AND row_id IN ({",".join(f"'{i}'" for i in drop_ids)})
    """)
print(f"{len(drop_ids)} rows resolved by removal")

to_validate = actionable.filter(F.coalesce(F.col("remediation_action"), F.lit("")) != "drop")

# COMMAND ----------
# The quarantine table carries one _data_source (the Silver target) and one
# _row_id_keys (the MERGE key columns). Cross-table quarantine or a hash-of-
# all-columns _row_id can't be safely reingested — escalate those rows.
import sys
if lib_path not in sys.path:
    sys.path.append(lib_path)
from reingest_common import resolve_reingest_target
from pyspark.sql import Window

# Dedupe the quarantine table to the latest pass per _row_id so joins on row_id
# stay 1:1 (otherwise the staged table / Silver MERGE gets duplicate keys).
_latest = Window.partitionBy("_row_id").orderBy(F.col("_generated_at").desc_nulls_last())
q = (
    spark.table(quarantine_fqn)
    .withColumn("_rn", F.row_number().over(_latest))
    .filter(F.col("_rn") == 1).drop("_rn")
)

meta = (
    q.select("_data_source", "_row_id_keys", F.col("_is_cross_table"))
     .distinct()
     .collect()
)
reingestable, silver_fqn, merge_key_cols, reason = resolve_reingest_target(
    r.asDict() for r in meta
)

if not reingestable:
    bad_ids = [r.row_id for r in to_validate.select("row_id").collect()]
    if bad_ids:
        spark.sql(f"""
            UPDATE {review_queue_fqn}
            SET status = 'escalated', updated_at = current_timestamp()
            WHERE quarantine_fqn = '{quarantine_fqn}'
              AND row_id IN ({",".join(f"'{i}'" for i in bad_ids)})
        """)
    print(f"{len(bad_ids)} rows escalated — not reingestable ({reason})")
    dbutils.jobs.taskValues.set(key="staged_table", value="")
    dbutils.jobs.taskValues.set(key="silver_fqn", value="")
    dbutils.jobs.taskValues.set(key="merge_key", value="")
    dbutils.jobs.taskValues.set(key="passed_count", value=0)
    dbutils.notebook.exit("0")

print(f"Silver target: {silver_fqn} · merge key: {merge_key_cols}")

# COMMAND ----------
# Join quarantine rows to their proposed fix, apply the patch, drop the
# quarantine bookkeeping columns.
patch_cols_rows = (
    to_validate
    .select(F.explode(F.map_keys(F.coalesce(F.col("proposed_fix"),
                                            F.create_map()))).alias("k"))
    .distinct()
    .collect()
)
patch_columns = [r["k"] for r in patch_cols_rows]

underscore_cols = [c for c in q.columns if c.startswith("_")]
data_columns = [c for c in q.columns if not c.startswith("_")]

candidates = (
    q.withColumn("row_id", F.col("_row_id"))
     .join(to_validate.select("row_id", "review_id", "proposed_fix",
                              "remediation_source", "confidence"),
           on="row_id")
)

for col in patch_columns:
    if col in candidates.columns:
        dtype = q.schema[col].dataType.simpleString()
        candidates = candidates.withColumn(
            col,
            F.coalesce(
                F.element_at(F.col("proposed_fix"), F.lit(col)).cast(dtype),
                F.col(col),
            ),
        )

clean = candidates.select(
    *data_columns,
    "row_id",
    F.col("remediation_source"),
    F.col("confidence"),
)

# COMMAND ----------
# Re-run the exact checks that quarantined this data.
#
# Caveat for dataset-level rules (e.g. PK uniqueness): a dedupe_keep_latest
# 'keep' row is unique within this candidate batch, but may still collide with
# a row already in Silver. Extend with a lookup against silver_fqn before
# trusting the pass — left as a follow-up (see AGENT.md open items).
checks = Validator(env=env, rules_table=rules_table).approved_checks_for(silver_fqn)

valid_df, still_failing_df = dq_engine.apply_checks_and_split(
    clean.drop("remediation_source", "confidence"), checks
)

passed_ids = [r.row_id for r in valid_df.select("row_id").collect()]
failed_ids = [r.row_id for r in still_failing_df.select("row_id").collect()]
print(f"Re-validation: {len(passed_ids)} passed, {len(failed_ids)} still failing")

# COMMAND ----------
if passed_ids:
    staged = (
        clean.filter(F.col("row_id").isin(passed_ids))
             .withColumn("agent_curated", F.col("remediation_source") == F.lit("agent"))
             .drop("remediation_source")
    )
    staged.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(staged_table)

    spark.sql(f"""
        UPDATE {review_queue_fqn}
        SET status = 'revalidated_pass', updated_at = current_timestamp()
        WHERE quarantine_fqn = '{quarantine_fqn}'
          AND row_id IN ({",".join(f"'{i}'" for i in passed_ids)})
    """)

if failed_ids:
    spark.sql(f"""
        UPDATE {review_queue_fqn}
        SET status = 'revalidated_fail',
            retry_count = retry_count + 1,
            updated_at = current_timestamp()
        WHERE quarantine_fqn = '{quarantine_fqn}'
          AND row_id IN ({",".join(f"'{i}'" for i in failed_ids)})
    """)

# COMMAND ----------
dbutils.jobs.taskValues.set(key="staged_table", value=staged_table if passed_ids else "")
dbutils.jobs.taskValues.set(key="silver_fqn", value=silver_fqn if passed_ids else "")
dbutils.jobs.taskValues.set(key="merge_key", value=",".join(merge_key_cols) if passed_ids else "")
dbutils.jobs.taskValues.set(key="passed_count", value=len(passed_ids))
dbutils.notebook.exit(str(len(passed_ids)))
