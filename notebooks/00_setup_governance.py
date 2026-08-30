# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC ## 00 · Governance table setup
# MAGIC Creates the control / audit tables the reingestion job MERGEs into, with
# MAGIC explicit schemas so downstream `MERGE` / `UPDATE` statements don't depend
# MAGIC on `saveAsTable` schema inference. Idempotent — safe to re-run; it never
# MAGIC drops data. Run once per environment before the first job run (or wire it
# MAGIC as an on-demand task).

# COMMAND ----------
dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("governance_schema", "dqx_studio")
dbutils.widgets.text("playbook_volume", "remediation_playbooks")  # Volume name for the per-dataset playbook YAMLs

catalog = dbutils.widgets.get("catalog")
governance_schema = dbutils.widgets.get("governance_schema")
playbook_volume = dbutils.widgets.get("playbook_volume")
base = f"{catalog}.{governance_schema}"

# COMMAND ----------
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {base}")

# COMMAND ----------
# MAGIC %md
# MAGIC ### `_dq_review_queue` — one row per quarantined row per pass
# MAGIC `status` domain: `pending` · `curated` · `escalated` · `rejected` ·
# MAGIC `resolved_duplicate` · `resolved_removed` · `revalidated_pass` ·
# MAGIC `revalidated_fail` · `reingested`

# COMMAND ----------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {base}._dq_review_queue (
    review_id               STRING       NOT NULL,
    quarantine_fqn          STRING       NOT NULL,
    row_id                  STRING       NOT NULL,
    rule_violations         ARRAY<STRING>,
    remediation_source      STRING,               -- 'playbook' | 'agent'
    playbook_strategy       STRING,               -- rule_name matched, null if agent
    agent_decision          STRING,               -- 'fix' | 'reject' | 'escalate', null if playbook
    remediation_action      STRING,               -- 'patch_fields' | 'keep' | 'drop'
    proposed_fix            MAP<STRING, STRING>,   -- column -> value; empty for keep/drop
    confidence              DOUBLE,               -- 1.0 for playbook, 0.0-1.0 from agent
    status                  STRING       NOT NULL,
    retry_count             INT          NOT NULL,
    data_source             STRING,               -- carried from quarantine _data_source (Silver target)
    quarantine_generated_at TIMESTAMP,            -- carried from quarantine _generated_at
    run_id                  STRING,
    created_at              TIMESTAMP,
    updated_at              TIMESTAMP
)
USING DELTA
TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ### `_dq_reingest_audit` — one row per (run_id, quarantine_fqn)

# COMMAND ----------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {base}._dq_reingest_audit (
    run_id            STRING,
    quarantine_fqn    STRING,
    triaged_count     BIGINT,
    playbook_resolved_count BIGINT,
    reingested_count  BIGINT,
    escalated_count   BIGINT,
    reingest_rate     DOUBLE,
    run_at            TIMESTAMP
)
USING DELTA
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ### `dq_summary_metrics` — feeds the shared DQX Monitoring dashboard
# MAGIC Column set mirrors the DQX summary-metrics convention so agentic reingest
# MAGIC shows up alongside pre-commit check results rather than in a silo. Adjust
# MAGIC to match your workspace's actual DQX metrics table if it already exists —
# MAGIC in that case skip this `CREATE` and just append in `06`.

# COMMAND ----------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {base}.dq_summary_metrics (
    metric_source     STRING,     -- 'dqx_agentic_reingest'
    table_fqn         STRING,     -- the quarantine FQN processed
    run_id            STRING,
    input_row_count   BIGINT,     -- rows triaged this run
    passed_row_count  BIGINT,     -- rows reingested this run
    failed_row_count  BIGINT,     -- rows still failing / escalated
    error_row_count   BIGINT,
    warning_row_count BIGINT,
    run_at            TIMESTAMP
)
USING DELTA
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ### `_dq_dry_run_report` — read-only estimate, one row per (run, FQN)
# MAGIC Written by `07_dry_run_report.py`. No row here ever caused a write to the
# MAGIC review queue, a staged table, or Silver.

# COMMAND ----------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {base}._dq_dry_run_report (
    run_id                     STRING,
    quarantine_fqn             STRING,
    silver_fqn                 STRING,       -- resolved reingest target (_data_source)
    quarantined_rows           BIGINT,       -- distinct latest _row_id in the table
    already_resolved           BIGINT,       -- terminal in the review queue already
    pending_rows               BIGINT,       -- quarantined_rows - already_resolved
    playbook_keep_or_patch     BIGINT,       -- rows a playbook would send to re-validation
    playbook_would_drop        BIGINT,       -- rows a playbook would drop as duplicates
    playbook_would_pass        BIGINT,       -- of keep/patch: pass re-validation (deterministic)
    playbook_would_fail        BIGINT,       -- of keep/patch: still fail re-validation
    agent_eligible             BIGINT,       -- no playbook match -> would go to the LLM (outcome unknown)
    not_reingestable           BIGINT,       -- cross-table / keyless -> would escalate
    est_reingest_insert        BIGINT,       -- playbook_would_pass whose key is NOT in Silver
    est_reingest_update        BIGINT,       -- playbook_would_pass whose key IS already in Silver
    run_at                     TIMESTAMP
)
USING DELTA
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ### `remediation_playbooks` Volume — the deterministic-fix rules (03a)
# MAGIC `03a_playbook_remediate` reads ONE YAML per dataset from this Volume:
# MAGIC `/Volumes/<catalog>/<gov>/remediation_playbooks/<quarantine_table_name>.yaml`
# MAGIC DQX Studio uploads them here. A dataset with no file → every failing row
# MAGIC goes to the LLM agent (03). See `docs/remediation_playbook.md` and the
# MAGIC template `config/remediation_playbook.sample.yaml`.

# COMMAND ----------
spark.sql(f"CREATE VOLUME IF NOT EXISTS {base}.{playbook_volume}")
vol_path = f"/Volumes/{catalog}/{governance_schema}/{playbook_volume}"
print(f"playbook Volume: {vol_path}")
try:
    existing = [f.name for f in dbutils.fs.ls(vol_path) if f.name.endswith(".yaml")]
    print(f"  existing playbook files: {existing or '(none — upload per-dataset YAMLs via DQX Studio)'}")
except Exception:
    print("  (empty)")

# COMMAND ----------
print(f"Governance objects ready in {base}:")
for t in ("_dq_review_queue", "_dq_reingest_audit", "dq_summary_metrics",
          "_dq_dry_run_report"):
    print(f"  {base}.{t}")
print(f"  {base}.{playbook_volume}  (Volume: {vol_path})")
