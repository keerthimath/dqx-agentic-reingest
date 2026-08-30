# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC ## 06 · Audit & metrics
# MAGIC Runs once per quarantine FQN, last in the child job. Rolls this run's
# MAGIC outcomes into `_dq_reingest_audit` and writes a row to `dq_summary_metrics`
# MAGIC in the DQX summary-metrics shape so agentic reingest shows up on the
# MAGIC shared DQX Monitoring dashboard alongside pre-commit checks.
# MAGIC
# MAGIC Counts come from upstream task values (per-FQN, per-run) with a
# MAGIC `_dq_review_queue` snapshot as backstop for anything the gate skipped.

# COMMAND ----------
dbutils.widgets.text("quarantine_fqn", "")
dbutils.widgets.text("review_queue_fqn", "main.dqx_studio._dq_review_queue")
dbutils.widgets.text("metrics_fqn", "main.dqx_studio.dq_summary_metrics")
dbutils.widgets.text("audit_fqn", "main.dqx_studio._dq_reingest_audit")
dbutils.widgets.text("run_id", "")

quarantine_fqn = dbutils.widgets.get("quarantine_fqn")
review_queue_fqn = dbutils.widgets.get("review_queue_fqn")
metrics_fqn = dbutils.widgets.get("metrics_fqn")
audit_fqn = dbutils.widgets.get("audit_fqn")
run_id = dbutils.widgets.get("run_id")

# COMMAND ----------
def tv(task_key, key):
    try:
        return int(dbutils.jobs.taskValues.get(taskKey=task_key, key=key, default=0) or 0)
    except Exception:
        return 0

triaged_count = tv("triage", "triaged_count")
playbook_resolved_count = tv("playbook_remediate", "playbook_resolved_count")
reingested_count = tv("reingest", "reingested_count")
escalated_count = tv("reingest", "escalated_count")

# COMMAND ----------
from pyspark.sql import functions as F

by_status = {
    r["status"]: r["c"]
    for r in (
        spark.table(review_queue_fqn)
        .filter(F.col("quarantine_fqn") == quarantine_fqn)
        .filter(F.col("run_id") == run_id)
        .groupBy("status").agg(F.count("*").alias("c"))
        .collect()
    )
}
still_failing = int(by_status.get("revalidated_fail", 0))
error_rows = int(by_status.get("escalated", 0) + by_status.get("rejected", 0))
reingest_rate = float(reingested_count / triaged_count) if triaged_count else None

# COMMAND ----------
# Explicit schemas — Spark Connect can't infer a column that is all-None.
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType

audit_schema = StructType([
    StructField("run_id", StringType()),
    StructField("quarantine_fqn", StringType()),
    StructField("triaged_count", LongType()),
    StructField("playbook_resolved_count", LongType()),
    StructField("reingested_count", LongType()),
    StructField("escalated_count", LongType()),
    StructField("reingest_rate", DoubleType()),
])
(
    spark.createDataFrame(
        [(run_id, quarantine_fqn, triaged_count, playbook_resolved_count,
          reingested_count, escalated_count, reingest_rate)],
        schema=audit_schema,
    )
    .withColumn("run_at", F.current_timestamp())
    .write.mode("append").saveAsTable(audit_fqn)
)

# COMMAND ----------
metrics_schema = StructType([
    StructField("metric_source", StringType()),
    StructField("table_fqn", StringType()),
    StructField("run_id", StringType()),
    StructField("input_row_count", LongType()),
    StructField("passed_row_count", LongType()),
    StructField("failed_row_count", LongType()),
    StructField("error_row_count", LongType()),
    StructField("warning_row_count", LongType()),
])
(
    spark.createDataFrame(
        [("dqx_agentic_reingest", quarantine_fqn, run_id, triaged_count,
          reingested_count, still_failing + escalated_count, error_rows, 0)],
        schema=metrics_schema,
    )
    .withColumn("run_at", F.current_timestamp())
    .write.mode("append").saveAsTable(metrics_fqn)
)

# COMMAND ----------
print(
    f"run_id={run_id} fqn={quarantine_fqn} triaged={triaged_count} "
    f"playbook_resolved={playbook_resolved_count} reingested={reingested_count} "
    f"escalated={escalated_count} by_status={by_status}"
)
