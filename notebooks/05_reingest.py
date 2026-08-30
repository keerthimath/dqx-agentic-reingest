# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC ## 05 · Reingest
# MAGIC MERGEs re-validated rows into Silver with lineage columns. Runs once per
# MAGIC quarantine FQN. Rows that have failed re-validation `max_retries` times
# MAGIC auto-escalate to human review instead of looping forever.
# MAGIC
# MAGIC `staged_table`, `silver_fqn` and `merge_key` come from `04`'s task values.
# MAGIC When `04` staged nothing (all rows failed / escalated), this task still
# MAGIC runs the retry-cap escalation sweep and exits.
# MAGIC
# MAGIC **`dry_run = true`**: compute the exact insert/update split against Silver
# MAGIC and report it, but perform **no** `MERGE`, no queue status change, and no
# MAGIC escalation. Upstream tasks (02–04) still populated the review queue and
# MAGIC the staged table — only the Silver write is held. For a zero-side-effect
# MAGIC estimate use `07_dry_run_report.py` instead.

# COMMAND ----------
dbutils.widgets.text("quarantine_fqn", "")
dbutils.widgets.text("staged_table", "")
dbutils.widgets.text("silver_fqn", "")
dbutils.widgets.text("review_queue_fqn", "main.dqx_studio._dq_review_queue")
dbutils.widgets.text("merge_key", "")  # CSV of business-key columns
dbutils.widgets.text("max_retries", "3")
dbutils.widgets.text("dry_run", "false")
dbutils.widgets.text("run_id", "")

quarantine_fqn = dbutils.widgets.get("quarantine_fqn")
staged_table = dbutils.widgets.get("staged_table")
silver_fqn = dbutils.widgets.get("silver_fqn")
review_queue_fqn = dbutils.widgets.get("review_queue_fqn")
merge_key_cols = [c.strip() for c in dbutils.widgets.get("merge_key").split(",") if c.strip()]
max_retries = int(dbutils.widgets.get("max_retries"))
dry_run = dbutils.widgets.get("dry_run").strip().lower() == "true"
run_id = dbutils.widgets.get("run_id")

# COMMAND ----------
from pyspark.sql import functions as F

reingested_count = 0
would_insert = would_update = 0

if staged_table and silver_fqn and merge_key_cols:
    staged = (
        spark.table(staged_table)
        .drop("row_id")  # the _row_id hash is not a Silver column
        .withColumn("confidence", F.col("confidence").cast("double"))
        .withColumn("reingest_run_id", F.lit(run_id))
        .withColumn("reingested_at", F.current_timestamp())
    )

    # Only merge columns that actually exist in Silver (plus the lineage
    # columns Silver is expected to carry: agent_curated, confidence,
    # reingest_run_id, reingested_at).
    silver = spark.table(silver_fqn)
    silver_cols = set(silver.columns)
    common_cols = [c for c in staged.columns if c in silver_cols]
    missing_in_silver = [c for c in staged.columns if c not in silver_cols]
    if missing_in_silver:
        print(f"NOTE: staged columns not in {silver_fqn}, dropped from MERGE: {missing_in_silver}")

    for k in merge_key_cols:
        if k not in common_cols:
            raise ValueError(f"merge key '{k}' is not a column of {silver_fqn}")

    staged_sel = staged.select(*common_cols)
    staged_count = staged_sel.count()

    # insert vs update split (needed for both the real and dry-run paths)
    silver_keys = silver.select(*merge_key_cols).distinct()
    would_update = staged_sel.join(silver_keys, on=merge_key_cols, how="left_semi").count()
    would_insert = staged_count - would_update

    if dry_run:
        print(
            f"DRY RUN — would MERGE {staged_count} rows into {silver_fqn}: "
            f"{would_insert} inserts, {would_update} updates. No write performed."
        )
    else:
        staged_sel.createOrReplaceTempView("staged_reingest")

        on_clause = " AND ".join(f"t.{k} = s.{k}" for k in merge_key_cols)
        update_cols = [c for c in common_cols if c not in merge_key_cols]
        set_clause = ", ".join(f"t.{c} = s.{c}" for c in update_cols)
        insert_cols = ", ".join(common_cols)
        insert_vals = ", ".join(f"s.{c}" for c in common_cols)

        spark.sql(f"""
            MERGE INTO {silver_fqn} AS t
            USING staged_reingest AS s
            ON {on_clause}
            WHEN MATCHED THEN UPDATE SET {set_clause}
            WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
        """)

        reingested_count = staged_count
        print(f"Reingested {reingested_count} rows into {silver_fqn}")

        spark.sql(f"""
            UPDATE {review_queue_fqn}
            SET status = 'reingested', updated_at = current_timestamp()
            WHERE quarantine_fqn = '{quarantine_fqn}' AND status = 'revalidated_pass'
        """)
else:
    print("Nothing staged for reingest this run.")

# COMMAND ----------
if dry_run:
    escalated_count = spark.sql(f"""
        SELECT count(*) AS c FROM {review_queue_fqn}
        WHERE quarantine_fqn = '{quarantine_fqn}'
          AND status = 'revalidated_fail' AND retry_count >= {max_retries}
    """).collect()[0]["c"]
    print(f"DRY RUN — {escalated_count} rows would auto-escalate (retry cap {max_retries}).")
else:
    # Auto-escalate anything that's exhausted its retries instead of looping.
    spark.sql(f"""
        UPDATE {review_queue_fqn}
        SET status = 'escalated', updated_at = current_timestamp()
        WHERE quarantine_fqn = '{quarantine_fqn}'
          AND status = 'revalidated_fail'
          AND retry_count >= {max_retries}
    """)
    escalated_count = spark.sql(f"""
        SELECT count(*) AS c FROM {review_queue_fqn}
        WHERE quarantine_fqn = '{quarantine_fqn}' AND status = 'escalated'
          AND updated_at >= current_timestamp() - INTERVAL 1 HOUR
    """).collect()[0]["c"]
    print(f"{escalated_count} rows escalated (retry cap {max_retries} or not reingestable)")

# COMMAND ----------
dbutils.jobs.taskValues.set(key="reingested_count", value=reingested_count)
dbutils.jobs.taskValues.set(key="escalated_count", value=escalated_count)
dbutils.jobs.taskValues.set(key="would_reingest_insert", value=would_insert)
dbutils.jobs.taskValues.set(key="would_reingest_update", value=would_update)
dbutils.notebook.exit(str(reingested_count if not dry_run else would_insert + would_update))
