# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC ## 07 · Dry-run report  (READ-ONLY)
# MAGIC Estimates how much of a quarantine table would flow back into Silver if
# MAGIC the pipeline ran now — **without writing anything** to `_dq_review_queue`,
# MAGIC a staged table, or Silver. Appends one summary row per (run, FQN) to
# MAGIC `_dq_dry_run_report`.
# MAGIC
# MAGIC It does the deterministic half for real (playbook match → strategy →
# MAGIC re-run the exact DQX checks) and reports the LLM half as a count only —
# MAGIC agent outcomes are unknown until a real curation run, and the dry run
# MAGIC never calls the model.

# COMMAND ----------
dbutils.widgets.text("quarantine_fqn", "")
dbutils.widgets.text("review_queue_fqn", "main.dqx_studio._dq_review_queue")
dbutils.widgets.text("report_fqn", "main.dqx_studio._dq_dry_run_report")
dbutils.widgets.text("env", "dev")
dbutils.widgets.text("rules_table", "")  # override; blank => derived from env
dbutils.widgets.text("lib_path", "/Workspace/dqx-agentic-reingest/notebooks/lib")
dbutils.widgets.text("playbook_dir", "/Volumes/main/dqx_studio/remediation_playbooks")
dbutils.widgets.text("run_id", "")

quarantine_fqn = dbutils.widgets.get("quarantine_fqn")
review_queue_fqn = dbutils.widgets.get("review_queue_fqn")
report_fqn = dbutils.widgets.get("report_fqn")
env = dbutils.widgets.get("env")
rules_table = dbutils.widgets.get("rules_table").strip() or None
lib_path = dbutils.widgets.get("lib_path")
playbook_dir = dbutils.widgets.get("playbook_dir").rstrip("/")
run_id = dbutils.widgets.get("run_id")

# COMMAND ----------
import sys
if lib_path not in sys.path:
    sys.path.append(lib_path)
from remediation_strategies import STRATEGY_REGISTRY
from reingest_common import resolve_reingest_target

import yaml
from pyspark.sql import functions as F
from pyspark.sql import Window
from dqx_validator import Validator

_pb_file = f"{playbook_dir}/{quarantine_fqn.split('.')[-1]}.yaml"
try:
    with open(_pb_file) as f:
        playbook_config = (yaml.safe_load(f) or {}).get("playbooks") or []
    playbook_config.sort(key=lambda e: e.get("priority", 1_000_000))
except FileNotFoundError:
    playbook_config = []
print(f"{len(playbook_config)} playbook entries for this dataset ({_pb_file})")

TERMINAL_STATUSES = ("reingested", "resolved_duplicate", "resolved_removed", "rejected")

# COMMAND ----------
q = spark.table(quarantine_fqn).withColumn("row_id", F.col("_row_id"))

latest = Window.partitionBy("_row_id").orderBy(F.col("_generated_at").desc_nulls_last())
deduped = q.withColumn("_rn", F.row_number().over(latest)).filter(F.col("_rn") == 1).drop("_rn")

failed_names = F.array_union(
    F.coalesce(F.transform(F.col("_error"), lambda c: c["name"]), F.array()),
    F.coalesce(F.transform(F.col("_warning"), lambda c: c["name"]), F.array()),
)
deduped = deduped.withColumn("rule_violations", failed_names)

quarantined_rows = deduped.count()

# --- reingestability / target from the quarantine bookkeeping columns
meta = deduped.select("_data_source", "_row_id_keys", "_is_cross_table").distinct().collect()
reingestable, silver_fqn, merge_key_cols, reason = resolve_reingest_target(
    r.asDict() for r in meta
)

# COMMAND ----------
# --- rows already finished in the review queue (so the estimate is "remaining work")
try:
    resolved_ids = (
        spark.table(review_queue_fqn)
        .filter(F.col("quarantine_fqn") == quarantine_fqn)
        .filter(F.col("status").isin(*TERMINAL_STATUSES))
        .select("row_id").distinct()
    )
    already_resolved = resolved_ids.count()
    pending = deduped.join(resolved_ids, on="row_id", how="left_anti")
except Exception:
    already_resolved = 0
    pending = deduped

pending_rows = pending.count()

# COMMAND ----------
# --- deterministic half: playbook match -> strategy -> re-validate
def matches_fqn(entry):
    scope = entry.get("match_fqns", ["*"])
    return "*" in scope or quarantine_fqn in scope

keep_patch_parts, drop_ids, handled_ids = [], [], set()

for entry in playbook_config:
    if not matches_fqn(entry):
        continue
    fn = STRATEGY_REGISTRY.get(entry["strategy"])
    if fn is None:
        continue
    rows = pending.filter(
        F.array_contains(F.col("rule_violations"), entry["rule_name"])
        & ~F.col("row_id").isin(list(handled_ids) or [""])
    )
    if rows.limit(1).count() == 0:
        continue
    out = fn(rows, entry["params"])
    handled_ids.update(r.row_id for r in rows.select("row_id").collect())
    drop_ids.extend(
        r.row_id for r in out.filter(F.col("remediation_action") == "drop").select("row_id").collect()
    )
    keep_patch_parts.append(out.filter(F.col("remediation_action") != "drop"))

playbook_would_drop = len(set(drop_ids))

# COMMAND ----------
playbook_would_pass = playbook_would_fail = 0
est_insert = est_update = 0
playbook_keep_or_patch = 0

if keep_patch_parts and reingestable:
    from functools import reduce
    kp = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), keep_patch_parts)

    data_columns = [c for c in q.columns if not c.startswith("_") and c != "row_id"]
    patch_columns = [
        r["k"] for r in
        kp.select(F.explode(F.map_keys(F.coalesce(F.col("action_payload"), F.create_map()))).alias("k"))
          .distinct().collect()
    ]
    patched = kp
    for col in patch_columns:
        if col in q.columns:
            dtype = q.schema[col].dataType.simpleString()
            patched = patched.withColumn(
                col,
                F.coalesce(F.element_at(F.col("action_payload"), F.lit(col)).cast(dtype), F.col(col)),
            )
    candidate = patched.select(*data_columns, "row_id")
    playbook_keep_or_patch = candidate.count()

    try:
        checks = Validator(env=env, rules_table=rules_table).approved_checks_for(silver_fqn)
        from databricks.labs.dqx.engine import DQEngine
        from databricks.sdk import WorkspaceClient
        good, bad = DQEngine(WorkspaceClient()).apply_checks_and_split(candidate, checks)
        passed = good.select("row_id", *merge_key_cols)
        playbook_would_pass = passed.count()
        playbook_would_fail = playbook_keep_or_patch - playbook_would_pass

        if playbook_would_pass and merge_key_cols:
            silver_keys = spark.table(silver_fqn).select(*merge_key_cols).distinct()
            matched = passed.join(silver_keys, on=merge_key_cols, how="left_semi").count()
            est_update = matched
            est_insert = playbook_would_pass - matched
    except Exception as e:
        print(f"re-validation estimate skipped: {e}")

# COMMAND ----------
not_reingestable = pending_rows if not reingestable else 0
agent_eligible = 0 if not reingestable else max(
    pending_rows - len(handled_ids), 0
)

# COMMAND ----------
from pyspark.sql import Row

report = spark.createDataFrame([Row(
    run_id=run_id,
    quarantine_fqn=quarantine_fqn,
    silver_fqn=silver_fqn,
    quarantined_rows=int(quarantined_rows),
    already_resolved=int(already_resolved),
    pending_rows=int(pending_rows),
    playbook_keep_or_patch=int(playbook_keep_or_patch),
    playbook_would_drop=int(playbook_would_drop),
    playbook_would_pass=int(playbook_would_pass),
    playbook_would_fail=int(playbook_would_fail),
    agent_eligible=int(agent_eligible),
    not_reingestable=int(not_reingestable),
    est_reingest_insert=int(est_insert),
    est_reingest_update=int(est_update),
)]).withColumn("run_at", F.current_timestamp())

report.write.mode("append").saveAsTable(report_fqn)

# COMMAND ----------
row = report.collect()[0].asDict()
print("DRY RUN —", quarantine_fqn)
for k, val in row.items():
    print(f"  {k:24} {val}")
print(
    f"\n  => ~{est_insert + est_update} rows would reingest via playbook "
    f"({est_insert} new, {est_update} updates); "
    f"{agent_eligible} more are agent-eligible (run 03 to find out)."
)

dbutils.jobs.taskValues.set(key="est_reingest_total", value=int(est_insert + est_update))
dbutils.jobs.taskValues.set(key="agent_eligible", value=int(agent_eligible))
dbutils.notebook.exit(str(est_insert + est_update))
