# DQX Agentic Quarantine Re-ingestion

Two halves of the dead-letter pattern:

- **`libs/dqx-validator/`** — the **write** side. `dqx_validator.Validator` applies
  `status='approved'` DQX Studio rules to a DataFrame and appends failing rows to a
  quarantine table. Standalone wheel, no dependency on the bundle.
- **`notebooks/` + `resources/` + `databricks.yml`** — the **read** side. A serverless
  Databricks job that triages quarantined rows, remediates them (documented playbook
  fixes first, LLM agent as fallback), re-validates every proposed fix through the
  *same* DQX checks, and merges only the passing rows back into Silver.

See [`AGENT.md`](AGENT.md) for the full architecture and design principles.

## Tests

Two hermetic suites — no Spark, no Databricks connection (pyspark / SDK are
stubbed; DQX's split and the quarantine write are recording fakes):

```bash
python -m venv .venv && .venv/bin/pip install pytest pyyaml
.venv/bin/python -m pytest
```

- `libs/dqx-validator/tests/` — approved-rule retrieval and monitor-only
  filtering, quarantine-FQN routing, `_row_id` / `_row_id_keys` selection,
  universal-rule placeholder binding, and the full `run_validation` flow
  asserted to route the quarantine write **without touching storage**.
- `tests/` — static checks on `config/remediation_playbook.yaml` (every
  `strategy` is registered, params match what each strategy reads).

## Dry run — estimate before committing

**Zero side effects** (nothing written but the report table, LLM never called):

```bash
databricks bundle run dqx_dry_run -t dev
```

`01_discover` → For Each → `07_dry_run_report`, which per quarantine table
computes the deterministic playbook path *for real* (match → strategy → re-run
the exact DQX checks) and reports the agent path as a count only. One row per
(run, FQN) lands in `<governance_schema>._dq_dry_run_report`:

| column | meaning |
|---|---|
| `quarantined_rows` / `already_resolved` / `pending_rows` | table size, minus what the queue already finished |
| `playbook_would_pass` / `playbook_would_fail` | keep/patch rows that (don't) pass re-validation |
| `playbook_would_drop` | rows a playbook would drop as duplicates |
| `agent_eligible` | no playbook match → would go to the LLM (outcome unknown until a real run) |
| `not_reingestable` | cross-table / keyless → would escalate |
| `est_reingest_insert` / `est_reingest_update` | of `playbook_would_pass`, split by whether the key already exists in Silver |

**Whole pipeline, hold the write**: run the real job with
`--params dry_run=true`. 02–04 still populate the review queue + staged table
and the agent *is* called, but `05` reports the Silver insert/update split and
skips the `MERGE`, the `reingested` status flip, and the retry-cap escalation.

---

## Deployment

Deployed as a [Databricks Asset Bundle](https://docs.databricks.com/dev-tools/bundles/).
`databricks.yml` holds the structure; **environment-specific values (workspace
hosts, service principals, catalogs, permission groups) live in one
`target.<name>.yml` file per environment, each git-ignored.** `databricks.yml`
pulls them in with `include: [target.*.yml]` — that glob does **not** match the
tracked `targets.example.yml` template — so the bundle won't validate until at
least one `target.*.yml` exists.

Example target layout (`var.env` / `var.catalog` set per file):

| target file → name | mode | `var.catalog` | rules schema (from the wheel) | governance schema | trigger on deploy |
|---|---|---|---|---|---|
| `target.dev.yml` → `dev`   | development | `main`  | `dqx_app`    | `dqx_studio` | paused (dev mode) |
| `target.qa.yml` → `qa`     | production  | `main`   | `dqx_app`    | `dqx_studio`     | **paused** (preset) |
| `target.prod.yml` → `prod` | production  | `main` | `dqx_studio` | `dqx_studio`     | live |

`var.catalog` must equal `dqx-validator`'s `catalog_template` for that
`var.env` (`<catalog>`) — the two halves resolve the same catalog
independently.

### 0. One-time, before the first deploy to a target

1. **Create the target file**: `cp targets.example.yml target.<name>.yml`, keep
   only that env's block, and fill in the `REPLACE_WITH_*` values — workspace
   `host`, `catalog`, the `run_as` service principal application id (prod), and
   the `permissions` group. `target.*.yml` is git-ignored; the template stays.
   Permission `level` is `CAN_MANAGE` / `CAN_RUN` / `CAN_VIEW`.
2. **Auth**: `databricks auth login --host <workspace-url>` (or a configured
   profile / `DATABRICKS_CONFIG_PROFILE`).
3. **Unity Catalog** in that workspace — grants below are for the **`run_as`
   identity** (the SP for prod), not the deploying user:
   - `USE CATALOG` on `<catalog>`;
   - `SELECT` on `<catalog>.quarantine` (where `dqx-validator` writes);
   - the rules schema exists with the DQX Studio tables — `dqx_app` for
     dev/qa, `dqx_studio` for prod — holding `dq_quality_rules` /
     `dq_universal_quality_rules` with the enforced checks set to
     `status = 'approved'`, and the run-as identity has `SELECT` on it;
   - **governance schema**: either `GRANT CREATE SCHEMA ON CATALOG
     <catalog> TO <run_as>` (so `00_setup_governance` can create it), or an
     admin pre-creates `<catalog>.dqx_studio` and grants the run-as
     identity `USE SCHEMA, CREATE TABLE, MODIFY, SELECT` on it;
   - `MODIFY` on every Silver table that is a reingest target.
4. **Local build tool**: `pip install build` (or edit `artifacts.dqx_validator.build`
   in `databricks.yml` to `uv build --wheel`).
5. `python -m pytest` — 78 hermetic tests, no workspace needed.

### 1. Deploy to QA

```bash
databricks bundle validate -t qa
databricks bundle deploy   -t qa      # builds the wheel, uploads notebooks,
                                      # creates all jobs, wires the child job id
```

`deploy` creates: `dqx-setup-governance`, `dqx-agentic-reingest` (parent),
`dqx-agentic-reingest-process` (child), `dqx-dry-run-report`. The QA parent
job's table-update trigger is deployed **paused**
(`presets.trigger_pause_status: PAUSED`).

```bash
databricks bundle run dqx_setup_governance -t qa   # create the 4 governance tables
databricks bundle run dqx_dry_run          -t qa   # read-only estimate, no writes, no LLM
```

Inspect `main.dqx_studio._dq_dry_run_report`. When the numbers look
right, either do a held run of the full pipeline:

```bash
databricks bundle run dqx_agentic_reingest -t qa --params dry_run=true
```

(check `_dq_review_queue` and the `*_staged_for_reingest` tables), then unpause
the parent job's **table-update trigger** in the Databricks UI (or remove the
`presets` block and redeploy) to go live in QA.

### 2. Deploy to Prod

Same sequence, `-t prod`. If the `prod` target has no `presets` block its
trigger deploys **live**, so run the estimate first — or add
`presets: { trigger_pause_status: PAUSED }` to the target during the first
rollout and remove it once verified.

```bash
databricks bundle validate -t prod
databricks bundle deploy   -t prod
databricks bundle run dqx_setup_governance -t prod
databricks bundle run dqx_dry_run          -t prod          # estimate
# optional held run:
databricks bundle run dqx_agentic_reingest -t prod --params dry_run=true
# then let the table-update trigger drive it, or:
databricks bundle run dqx_agentic_reingest -t prod
```

Prod runs as the service principal in `databricks.yml`; make sure it (not your
user) holds the UC grants from step 0.

### Redeploy / rollback

- **Redeploy**: `databricks bundle deploy -t <target>` after any code change.
  The wheel version is in `libs/dqx-validator/pyproject.toml` (currently
  `1.1.1`) — bump it when the `dqx_validator` package changes so serverless
  picks up the new artifact, and keep `libs/dqx-validator/dist/` holding only
  that one wheel (the env dep is a `*.whl` glob).
- **Governance DDL change**: re-run `dqx_setup_governance` (the DDL is
  `CREATE TABLE IF NOT EXISTS` — additive only; a column drop/type change needs
  a manual `ALTER`).
- **Rollback**: `git checkout <previous>` then `databricks bundle deploy`, or
  `databricks bundle deploy -t <target>` from the previous tag. Destroy a
  target entirely with `databricks bundle destroy -t <target>`.
- **Raw REST / Terraform** instead of the bundle: see
  [`workflow/README.md`](workflow/README.md).

---

## Validator ↔ reingest reconciliation

The two halves were built in separate sessions and disagreed on the quarantine
contract. This section records the agreed convention for each point of
disagreement and tracks the work to make them line up. **Nothing runs end to end
until Part 3 is complete.**

### Part 1 — Contract decisions (which side moves)

| # | Concern | Decision | Side that changes | Why |
|---|---|---|---|---|
| 1 | Quarantine location — validator writes `<src_catalog>.quarantine.<src_schema>_<src_table>` | Adopt the `quarantine` **schema** convention | reingest | Validator's scheme is deliberate (keeps `bronze.foo`/`silver.foo` apart, handles cross-table); reingest just needs to look in the right place |
| 2 | Failed-rule column — validator writes `_error` / `_warning` as `array<struct{name,message,…}>` | Read `x.name` from the structs, union error + warning into `rule_violations array<string>` | reingest (`02_triage` only) | Everything downstream of triage already expects `array<string>` |
| 3 | Row identity — validator emits none | Add `row_id_columns=` param to `Validator.run_validation` / `run_universal_rule_validation`; it writes a `_row_id` hash column | validator | Natural key must be captured at quarantine time; reconstructing it later is guesswork |
| 4 | Retry bookkeeping — AGENT.md said "back to quarantine with `retry_count += 1`" | Retry state stays **queue-only** (`_dq_review_queue.retry_count`); nothing is written back into the append-only quarantine table | reingest (doc + `04`) | The queue is already the per-pass ledger; `04`/`05` already bump it there |
| 5 | Validator audit cols — `_generated_at`, `_data_source`, `_rule_sources`, `_is_cross_table` | `02` keeps `_data_source` (→ Silver target) + `_generated_at` (→ audit); `04` strips all `_`-prefixed cols before re-validation and before staging for Silver | reingest | Useful lineage; Silver just shouldn't receive them |
| 6 | Catalog/schema naming — validator: `<catalog>` + `dqx_app`/`dqx_studio`; bundle: `var.catalog` overridden to `main` | Bundle `catalog` var = `main` / `main` per target; add an `env` base-parameter to every task | reingest (bundle + JSON) | Validator naming is baked into the shipped wheel's `tables.yaml` |
| 7 | Re-validation checks source — `04` loaded a `checks_config_path` YAML | `04` reuses the rules table via `Validator.approved_checks_for(table_fqn)` | reingest | "Re-run the *exact* checks that quarantined it" only holds if both sides read the same source |

### Part 2 — Structural decision

`03a`, `03`, `04`, `05` each took a single `quarantine_fqn` widget but ran **once,
after** the For Each loop — so they'd only ever process one table. **Decision:
move `03a`→`05` inside the For Each** (one iteration per quarantine table), matching
how `02` is already wired. `06` aggregates across iterations by reading
`_dq_review_queue` filtered on `run_id`.

### Part 3 — Work tracker

Sequenced so each step is independently testable.

- [x] **0. Governance DDL** — `notebooks/00_setup_governance.py` creates
  `_dq_review_queue`, `_dq_reingest_audit`, `dq_summary_metrics` with explicit
  schemas (idempotent).
- [x] **1a. Rename** `libs/dqx-validator/dqxvalidator/` → `libs/dqx-validator/dqx_validator/`
  (import package `dqx_validator`, distribution stays `dqx-validator`). pyproject,
  MANIFEST, READMEs updated; version bumped `1.0.0` → `1.1.0`.
- [x] **1b. Validator** — `row_id_columns=` param writing `_row_id` + `_row_id_keys`;
  public `approved_checks_for(table_fqns)`; wheel rebuilt into `dist/`.
- [x] **1c. Validator (1.1.1)** — `input_df` type check accepts Spark Connect
  DataFrames (serverless), not only classic `pyspark.sql.DataFrame`.
- [x] **2. `01_discover`** — enumerates `SHOW TABLES IN {catalog}.{quarantine_schema}`.
- [x] **3. `02_triage`** — reads `_row_id`; `rule_violations` from
  `_error[].name` ∪ `_warning[].name`; dedupes to latest `_generated_at` per
  row; carries `data_source` + `quarantine_generated_at`; writes the full
  queue schema.
- [x] **4. Job YAML + JSON** — parent (`01` + For Each `run_job_task`) / child
  (`02`→`06`) split; `catalog`/`env`/`quarantine_schema`/`governance_schema`
  bundle vars; trigger `${var.catalog}.${var.quarantine_schema}.*`; `lib_path`
  param for `03a`. Raw JSON re-split under `workflow/` + `workflow/README.md`.
- [x] **5. `04_apply_and_validate`** — checks from `approved_checks_for(silver_fqn)`;
  re-selects retryable `revalidated_fail`; strips `_*` cols; derives Silver
  target + merge key from `_data_source` / `_row_id_keys`; escalates
  cross-table / keyless rows.
- [x] **6. `05_reingest`** — `silver_fqn` / `merge_key` from `04` task values;
  dynamic composite-key MERGE with target-column intersection.
- [x] **7. `03_agent_curate`** — emits `min_batch_confidence`; explicit
  `createDataFrame` schema; sub-threshold `fix` → `escalated`; FQN-scoped.
- [x] **8. `06_audit_metrics`** — per-(run, FQN) audit row + DQX-summary-metrics
  row; counts from task values + queue snapshot.
- [x] **8b. Tests** — hermetic pytest suites for rule retrieval, FQN routing,
  row-id keys, the `run_validation` flow (no storage IO), and playbook config
  validation. Pure logic extracted to `checks_from_rule_rows` /
  `resolve_row_id_keys` in `engine.py`.
- [x] **8c. Dry run** — `07_dry_run_report.py` + `dqx_dry_run` job (read-only
  estimate, LLM never called) and a `dry_run=true` job parameter that holds
  `05`'s Silver MERGE. New `_dq_dry_run_report` table (DDL in `00`).
- [ ] **9.** Full dev run against a seeded quarantine table (not yet done).

### Known gaps after this pass (for the dev run)

- `dq_summary_metrics` schema is a best guess at the DQX convention — reconcile
  with the real DQX metrics table before wiring the dashboard.
- Silver is assumed to already carry the lineage columns `agent_curated`,
  `confidence`, `reingest_run_id`, `reingested_at`; `05` drops any staged
  column Silver lacks, so absent lineage columns are silently skipped.
- `06` counts retried rows under their original `run_id`, so a row triaged in
  an earlier run but reingested now isn't counted in this run's audit.
- Dataset-level re-validation (PK uniqueness vs. what's already in Silver) is
  still batch-local — see AGENT.md open items.
- `notebooks/lib/remediation_strategies.py` is deployed as a workspace file;
  `03a` adds `lib_path` to `sys.path`. Confirm the path resolves under
  `mode: development` bundle roots, or package `lib/` into the wheel.

### Quarantine table schema (agreed)

Input columns unchanged, plus:

| column | type | added by |
|---|---|---|
| `_row_id` | string | hash of `row_id_columns` (or all columns) |
| `_error` | `array<struct>` | DQX — failed error-level checks (`.name`, `.message`, …) |
| `_warning` | `array<struct>` | DQX — failed warn-level checks |
| `_generated_at` | timestamp | validator |
| `_data_source` | string | validator — source `table_fqn` (CSV if cross-table); the reingest Silver target |
| `_rule_sources` | string | validator — all rule `table_fqn`s applied (CSV) |
| `_is_cross_table` | boolean | validator |
