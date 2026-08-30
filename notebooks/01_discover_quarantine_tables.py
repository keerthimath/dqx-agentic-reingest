# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC ## 01 · Discover quarantine tables
# MAGIC `dqx-validator` writes every quarantine table into a single `quarantine`
# MAGIC schema per catalog, named `<src_schema>_<src_table>` (or
# MAGIC `<src_schema>_cross_table__…` when rules span tables). This task lists
# MAGIC them and emits the FQN list as a task value for the downstream For Each.

# COMMAND ----------
dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("quarantine_schema", "quarantine")
dbutils.widgets.text("table_prefix", "")  # e.g. "identity_" to scope to one source schema

catalog = dbutils.widgets.get("catalog")
quarantine_schema = dbutils.widgets.get("quarantine_schema")
table_prefix = dbutils.widgets.get("table_prefix")

# COMMAND ----------
schema_fqn = f"{catalog}.{quarantine_schema}"

if not spark.catalog.databaseExists(schema_fqn):
    print(f"Quarantine schema {schema_fqn} does not exist yet — nothing to process.")
    quarantine_fqns = []
else:
    quarantine_fqns = [
        f"{schema_fqn}.{t.tableName}"
        for t in spark.sql(f"SHOW TABLES IN {schema_fqn}").collect()
        if not t.isTemporary and t.tableName.startswith(table_prefix)
    ]

print(f"Found {len(quarantine_fqns)} quarantine tables in {schema_fqn}:")
for fqn in quarantine_fqns:
    print(f"  {fqn}")

# COMMAND ----------
# MAGIC %md
# MAGIC Emit as a task value (JSON-serializable list) for the Workflow's
# MAGIC For Each task to consume as `{{tasks.discover.values.quarantine_fqns}}`.

# COMMAND ----------
import json

dbutils.jobs.taskValues.set(key="quarantine_fqns", value=quarantine_fqns)
dbutils.notebook.exit(json.dumps(quarantine_fqns))
