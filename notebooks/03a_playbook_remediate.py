# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC ## 03a · Playbook remediate
# MAGIC Runs **before** the LLM agent. Reads this dataset's playbook YAML from
# MAGIC the Volume — `<playbook_dir>/<quarantine table name>.yaml` — and for
# MAGIC every pending row whose failed rule matches an entry (and whose `where`
# MAGIC matches, if set) applies the matching deterministic `strategy`. First
# MAGIC matching entry by `priority` wins; the agent never sees that row.
# MAGIC A dataset with no playbook file → every failing row falls through to
# MAGIC `03_agent_curate`.
# MAGIC
# MAGIC Playbook fixes get `confidence = 1.0` and skip the confidence gate.

# COMMAND ----------
dbutils.widgets.text("quarantine_fqn", "")
dbutils.widgets.text("review_queue_fqn", "main.dqx_studio._dq_review_queue")
dbutils.widgets.text("playbook_dir", "/Volumes/main/dqx_studio/remediation_playbooks")
dbutils.widgets.text("lib_path", "/Workspace/dqx-agentic-reingest/notebooks/lib")

quarantine_fqn = dbutils.widgets.get("quarantine_fqn")
review_queue_fqn = dbutils.widgets.get("review_queue_fqn")
playbook_dir = dbutils.widgets.get("playbook_dir").rstrip("/")
lib_path = dbutils.widgets.get("lib_path")

# COMMAND ----------
import sys
if lib_path not in sys.path:
    sys.path.append(lib_path)
from remediation_strategies import STRATEGY_REGISTRY

import yaml

dataset = quarantine_fqn.split(".")[-1]
playbook_file = f"{playbook_dir}/{dataset}.yaml"
try:
    with open(playbook_file) as f:
        playbook_config = (yaml.safe_load(f) or {}).get("playbooks") or []
    playbook_config.sort(key=lambda e: e.get("priority", 1_000_000))
    print(f"loaded {len(playbook_config)} playbook entries from {playbook_file}")
except FileNotFoundError:
    print(f"no playbook file for '{dataset}' ({playbook_file}); all failing rows -> 03_agent_curate")
    playbook_config = []

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql import Window

pending = (
    spark.table(review_queue_fqn)
    .filter(F.col("quarantine_fqn") == quarantine_fqn)
    .filter(F.col("status") == "pending")
)

# One quarantine table can hold several append passes for the same _row_id;
# keep only the latest so the join below is 1:1 (else the MERGE sees multiple
# source rows per target).
_latest = Window.partitionBy("_row_id").orderBy(F.col("_generated_at").desc_nulls_last())
quarantined_rows = (
    spark.table(quarantine_fqn)
    .withColumn("_rn", F.row_number().over(_latest))
    .filter(F.col("_rn") == 1).drop("_rn")
    .withColumn("row_id", F.col("_row_id"))
)

print(f"{pending.count()} pending rows to check against {len(playbook_config)} playbook entries")

# COMMAND ----------
def matches_fqn(entry, fqn):
    scope = entry.get("match_fqns", ["*"])
    return "*" in scope or fqn in scope


results = []
remaining = pending            # first matching entry wins — a row is handled once
n_pending = pending.count()

for entry in playbook_config:
    if not matches_fqn(entry, quarantine_fqn):
        continue

    strategy_fn = STRATEGY_REGISTRY.get(entry["strategy"])
    if strategy_fn is None:
        print(f"WARNING: unknown strategy '{entry['strategy']}' for rule "
              f"'{entry['rule_name']}' — skipping, will fall through to agent")
        continue

    # A half-filled entry (REPLACE_ME left in params / where) must not break the
    # run — skip it so those rows fall through to the agent until it's completed.
    if "REPLACE_ME" in str(entry.get("params")) + str(entry.get("where", "")):
        print(f"WARNING: playbook entry for '{entry['rule_name']}' still has "
              f"REPLACE_ME — skipping until filled in")
        continue

    # Rows still unhandled whose failed-rule list includes this playbook's rule.
    rule_rows = (
        remaining
        .filter(F.array_contains(F.col("rule_violations"), entry["rule_name"]))
        .join(quarantined_rows, on="row_id")
    )

    # Optional `where`: scope the entry to a subset of the rule's rows by value
    # (e.g. classification = 'Duplicate'). Rows not matching fall through to the
    # agent. Two entries for one rule_name can route different values differently.
    if entry.get("where"):
        rule_rows = rule_rows.filter(F.expr(entry["where"]))

    if rule_rows.limit(1).count() == 0:
        continue

    remediated = strategy_fn(rule_rows, entry["params"])
    remediated = remediated.withColumn("playbook_rule", F.lit(entry["rule_name"]))
    results.append(remediated)
    # take these rows out of contention for later entries (avoids a row being
    # remediated twice -> MERGE "multiple source rows match one target").
    remaining = remaining.join(rule_rows.select("row_id").distinct(), on="row_id", how="left_anti")

handled_count = n_pending - remaining.count()

# COMMAND ----------
if results:
    from functools import reduce
    remediated_all = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), results)

    remediated_all.createOrReplaceTempView("playbook_remediated")

    # 'keep' / 'patch_fields' rows proceed to deterministic re-validation.
    # 'drop'   -> resolved_duplicate (dedup loser).  'reject' -> rejected
    # (documented-bad row). Both terminal — never reingested, no retry.
    spark.sql(f"""
        MERGE INTO {review_queue_fqn} AS target
        USING playbook_remediated AS source
        ON target.row_id = source.row_id
           AND target.quarantine_fqn = '{quarantine_fqn}'
           AND target.status = 'pending'
        WHEN MATCHED THEN UPDATE SET
            target.agent_decision = NULL,
            target.remediation_source = 'playbook',
            target.playbook_strategy = source.playbook_rule,
            target.remediation_action = source.remediation_action,
            target.proposed_fix = source.action_payload,
            target.confidence = 1.0,
            target.status = CASE
                WHEN source.remediation_action = 'drop' THEN 'resolved_duplicate'
                WHEN source.remediation_action = 'reject' THEN 'rejected'
                ELSE 'curated'
            END,
            target.updated_at = current_timestamp()
    """)

print(f"Playbook resolved {handled_count} rows deterministically; "
      f"remainder falls through to 03_agent_curate")

# COMMAND ----------
dbutils.jobs.taskValues.set(key="playbook_resolved_count", value=handled_count)
dbutils.notebook.exit(str(handled_count))
