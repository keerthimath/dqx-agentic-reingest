# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC ## 02 · Triage
# MAGIC Runs once per FQN (inside the Workflow's For Each task). Pulls
# MAGIC unprocessed rows from this quarantine table and seeds `_dq_review_queue`
# MAGIC — one row per quarantined `_row_id` that isn't already tracked.
# MAGIC
# MAGIC Quarantine contract (written by `dqx-validator`):
# MAGIC   `_row_id`        string        — business-key hash, the review-queue key
# MAGIC   `_error`         array<struct> — failed error-level checks (`.name`, `.message`)
# MAGIC   `_warning`       array<struct> — failed warn-level checks
# MAGIC   `_generated_at`  timestamp
# MAGIC   `_data_source`   string        — source table_fqn == the reingest Silver target

# COMMAND ----------
dbutils.widgets.text("quarantine_fqn", "")
dbutils.widgets.text("review_queue_fqn", "main.dqx_studio._dq_review_queue")
dbutils.widgets.text("run_id", "")
dbutils.widgets.text("reset_queue", "false")  # true = drop this FQN's queue rows and re-triage from scratch

quarantine_fqn = dbutils.widgets.get("quarantine_fqn")
review_queue_fqn = dbutils.widgets.get("review_queue_fqn")
run_id = dbutils.widgets.get("run_id")
reset_queue = dbutils.widgets.get("reset_queue").strip().lower() == "true"

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql import Window
import uuid

quarantine_df = spark.table(quarantine_fqn)

# rule_violations = names of every failed error + warning check on the row.
# _error / _warning are array<struct{name, message, ...}> (may be null/empty).
failed_names = F.array_union(
    F.coalesce(F.transform(F.col("_error"), lambda c: c["name"]), F.array()),
    F.coalesce(F.transform(F.col("_warning"), lambda c: c["name"]), F.array()),
)

# One quarantine table can hold several append passes for the same _row_id;
# keep only the most recent quarantined version of each row.
latest = Window.partitionBy("_row_id").orderBy(F.col("_generated_at").desc_nulls_last())
deduped = (
    quarantine_df
    .withColumn("_rn", F.row_number().over(latest))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
    .withColumn("row_id", F.col("_row_id"))
    .withColumn("rule_violations", failed_names)
)

if reset_queue:
    n = spark.sql(
        f"DELETE FROM {review_queue_fqn} WHERE quarantine_fqn = '{quarantine_fqn}'"
    )
    print(f"reset_queue: cleared existing review-queue rows for {quarantine_fqn}")

already_queued = (
    spark.table(review_queue_fqn)
    .filter(F.col("quarantine_fqn") == quarantine_fqn)
    .select("row_id")
)

new_rows = deduped.join(already_queued, on="row_id", how="left_anti")

new_rows_count = new_rows.count()
print(f"{new_rows_count} new quarantined rows to triage from {quarantine_fqn}")

# COMMAND ----------
queue_rows = (
    new_rows
    .withColumn("review_id", F.expr("uuid()"))
    .withColumn("quarantine_fqn", F.lit(quarantine_fqn))
    .withColumn("remediation_source", F.lit(None).cast("string"))
    .withColumn("playbook_strategy", F.lit(None).cast("string"))
    .withColumn("agent_decision", F.lit(None).cast("string"))
    .withColumn("remediation_action", F.lit(None).cast("string"))
    .withColumn("proposed_fix", F.lit(None).cast("map<string,string>"))
    .withColumn("confidence", F.lit(None).cast("double"))
    .withColumn("status", F.lit("pending"))
    .withColumn("retry_count", F.lit(0))
    .withColumn("data_source", F.col("_data_source"))
    .withColumn("quarantine_generated_at", F.col("_generated_at"))
    .withColumn("run_id", F.lit(run_id))
    .withColumn("created_at", F.current_timestamp())
    .withColumn("updated_at", F.current_timestamp())
    .select(
        "review_id", "quarantine_fqn", "row_id", "rule_violations",
        "remediation_source", "playbook_strategy", "agent_decision",
        "remediation_action", "proposed_fix", "confidence", "status",
        "retry_count", "data_source", "quarantine_generated_at",
        "run_id", "created_at", "updated_at",
    )
)

queue_rows.write.mode("append").saveAsTable(review_queue_fqn)

# COMMAND ----------
dbutils.jobs.taskValues.set(key="triaged_count", value=new_rows_count)
dbutils.notebook.exit(str(new_rows_count))
