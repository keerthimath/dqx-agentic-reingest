# dqx-validator

Standalone library: apply approved [DQX Studio](https://databricks.github.io/dqx/)
rules to a DataFrame and quarantine failing rows. This is the **write** side of
the dead-letter pattern — the `dqx-agentic-reingest` bundle around it is the read side.

It has no dependency on the bundle. Teams that only want quarantining install
the wheel and never deploy the workflow:

```python
%pip install /Volumes/<catalog>/<schema>/artifacts/dqx_validator-1.1.1-py3-none-any.whl
```

```python
from dqx_validator import Validator

v = Validator(env=dbutils.widgets.get("env"))          # 'dev' / 'prod' / ...
good_df = v.run_validation(                             # bad rows -> quarantine
    df, "main.silver.orders", row_id_columns=["order_id"],
)
```

> The import package is `dqx_validator` (underscore); the distribution / wheel
> name is `dqx-validator`. The directory is `libs/dqx-validator/dqx_validator/`.

## What it does

- Loads `status = 'approved'` checks from the rules tables authored in DQX
  Studio (`dq_quality_rules`, `dq_universal_quality_rules`), resolved per-env
  from [`dqx_validator/tables.yaml`](dqx_validator/tables.yaml). The same
  loader is exposed as `Validator.approved_checks_for(table_fqns)` so the
  reingest side can re-validate against the exact same checks.
- Splits the DataFrame with `DQEngine.apply_checks_by_metadata_and_split` and
  appends failing rows to a quarantine table.
- Quarantine FQN follows the source table: `<source_catalog>.<quarantine_schema>.<source_schema>_<source_table>`
  (single source) or `..._cross_table__<t1>__<t2>` when rules span tables.
- Quarantine schema = the input schema unchanged + `_row_id`, `_error`,
  `_warning`, `_generated_at`, `_data_source`, `_rule_sources`,
  `_is_cross_table`.
- `row_id_columns=` (str or list) names the business/natural key columns; the
  validator writes their hash into `_row_id`, which the reingest review queue
  keys every quarantined row on. Omitted → `_row_id` is a hash of every input
  column and a warning is logged.

## Config

Edit [`dqx_validator/tables.yaml`](dqx_validator/tables.yaml) (bundled in the
wheel) and rebuild:

```bash
python -m build --wheel
```

## Build

The parent `dqx-agentic-reingest` bundle builds this automatically via the `artifacts`
block in `../../databricks.yml`. To build by hand: `python -m build --wheel`
(output in `dist/`).

## Tests

`pip install -e .[test]` then `python -m pytest` (or from the repo root). The
suite is hermetic — pyspark and the Databricks SDK are stubbed, DQX's split and
the quarantine write are recording fakes, so nothing connects to a workspace or
writes a table. Covers approved-rule retrieval / monitor-only filtering,
quarantine-FQN routing, `_row_id` key selection, universal-rule binding, and the
`run_validation` flow.
