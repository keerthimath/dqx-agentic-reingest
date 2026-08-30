# DQX Agentic Quarantine Re-ingestion System

## Purpose

DQX quarantines rows that fail pre-commit data quality checks into a
`<catalog>.quarantine.<src_schema>_<src_table>` table per source FQN (the
"Dead-Letter" pattern). Left alone, that quarantine table is a graveyard. This system closes
the loop: an LLM agent triages and proposes fixes for quarantined rows, DQX
re-validates the proposed fix deterministically, and only rows that pass are
merged back into Silver. Low-confidence or still-failing rows escalate to a
human review queue instead of looping forever.

**Design principle: the agent proposes, DQX disposes.** The agent never writes
directly to Silver/Gold. Every agent-curated row is re-run through the same
DQX checks that quarantined it in the first place before it's allowed back in.
This keeps the system auditable and keeps a hallucinated "fix" from silently
corrupting Silver.

**Design principle: documented fixes beat guessed fixes.** If a rule owner
has documented exactly how a violation should be remediated (e.g. "PK
collision → row-number over `customer_id` ordered by `updated_at` desc, keep
latest"), that documented fix is applied deterministically and the LLM never
sees the row. The agent is a fallback for rules nobody has written a
playbook for yet — not the default path.

## Architecture

```
DQX Profiler → Quality rules → Bronze → DQX Quality Checking → Silver
                                              │
                                              ▼
                          <catalog>.quarantine.<schema>_<table>
                                              │
                     ┌────────────────────────┴────────────────────────┐
                     │       dqx-agentic-reingest Databricks Workflow     │
                     │                                                    │
                     │  parent: 01_discover ──► For Each FQN ──► run_job   │
                     │                                            │       │
                     │  child (per FQN):                          ▼       │
                     │  02_triage → 03a_playbook_remediate →│             │
                     │              (documented fixes)      │             │
                     │                      │ (no playbook match)         │
                     │                      ▼                             │
                     │              03_agent_curate  →  gate              │
                     │                                      │             │
                     │                low confidence ───────┤             │
                     │                      │          high confidence    │
                     │                      ▼                │           │
                     │           escalated (human)     04_apply_validate  │
                     │                                       │           │
                     │                                       ▼           │
                     │                              pass → 05_reingest    │
                     │                              fail → revalidated_   │
                     │                                     fail in queue  │
                     │                                     (retry_count++)│
                     │                                        │           │
                     │                                        ▼           │
                     │                              06_audit_metrics      │
                     └────────────────────────────────────────────────────┘
```

## Repository layout

```
dqx-agentic-reingest/
  databricks.yml                 # DAB: bundle config, variables, targets, artifacts
  resources/dqx_agentic_reingest_job.yml # parent + child jobs (serverless)
  workflow/*.json                # raw Jobs API equivalents (+ workflow/README.md)
  notebooks/                     # 00..07 tasks (07 = read-only dry run) + notebooks/lib
  config/remediation_playbook.sample.yaml  # template for a per-dataset playbook (03a)
  config/agent_curation_policy.yaml    # fix/reject/escalate policy + cross-table probe (03)
  config/README.md                     # how the playbooks + policy drive remediation
  docs/remediation_playbook.md         # per-dataset playbook YAML spec (for the DQX Studio piece)
  tests/                         # repo-side hermetic tests (playbook config)
  pytest.ini                     # discovers tests/ + libs/dqx-validator/tests/
  libs/dqx-validator/            # standalone quarantine library -> wheel
    dqx_validator/ (engine.py, config.py, tables.yaml)
    tests/  (hermetic — pyspark + SDK stubbed)
    pyproject.toml, README.md
```

`libs/dqx-validator` is the **write** side of the dead-letter pattern and is
deliberately decoupled: it has no import of anything in the bundle, and teams
that only want quarantining install its wheel without deploying this bundle.
The bundle builds it (`artifacts.dqx_validator` in `databricks.yml`) and the
job installs `./libs/dqx-validator/dist/*.whl` as a serverless dependency.

### Contract with dqx-validator — reconciled

The two halves were built in separate sessions and disagreed on the quarantine
contract. The reconciliation and its work tracker live in [`README.md`](README.md);
the agreed state:

| Concern | Convention | Enforced by |
|---|---|---|
| Quarantine FQN | `<catalog>.quarantine.<src_schema>_<src_table>` (a `quarantine` *schema*) | `01_discover` lists `SHOW TABLES IN <catalog>.quarantine`; trigger is `<catalog>.quarantine.*` |
| Failed rule names | `_error` / `_warning` = `array<struct{name,message,…}>` | `02_triage` flattens `.name` from both into `rule_violations array<string>` |
| Row identity | validator writes `_row_id` (sha2 of `row_id_columns`) + `_row_id_keys` (CSV of those key cols) | `run_validation(df, fqn, row_id_columns=[...])`; `02` keys the queue on `_row_id`, `04`/`05` MERGE Silver on `_row_id_keys` |
| Retry bookkeeping | queue-only — `_dq_review_queue.retry_count`; nothing is written back to the append-only quarantine table | `04` re-selects `revalidated_fail AND retry_count < max_retries`; `05` escalates at the cap |
| Re-validation checks | same rules table the validator quarantined from | `04` calls `Validator.approved_checks_for(silver_fqn)` |
| Audit columns | `_data_source` (Silver target), `_generated_at`, `_rule_sources`, `_is_cross_table` | `02` carries `_data_source`/`_generated_at` into the queue; `04` strips all `_*` before re-validation and staging |

## Control table

`<catalog>.<governance_schema>._dq_review_queue` — one row per quarantined row
per pass. DDL: `notebooks/00_setup_governance.py`.

| column              | type      | notes                                              |
|---------------------|-----------|-----------------------------------------------------|
| review_id            | string    | uuid, primary key                                  |
| quarantine_fqn        | string    | source quarantine table                            |
| row_id               | string    | the quarantine row's `_row_id` (sha2 of the business key) |
| rule_violations      | array<string> | DQX check names that failed (`_error`∪`_warning` `.name`) |
| remediation_source   | string    | `playbook` \| `agent`                               |
| playbook_strategy    | string    | rule_name matched in the playbook, null if `agent`  |
| agent_decision       | string    | `fix` \| `reject` \| `escalate`, null if `playbook` |
| remediation_action   | string    | `patch_fields` \| `keep` \| `drop`                  |
| proposed_fix         | map<string,string> | column → value (`patch_fields`); empty for `keep`/`drop` |
| confidence           | double    | 1.0 for playbook (documented, not guessed); 0.0–1.0 from the agent otherwise |
| status               | string    | `pending`/`curated`/`escalated`/`rejected`/`resolved_duplicate`/`resolved_removed`/`revalidated_pass`/`revalidated_fail`/`reingested` |
| retry_count          | int       | incremented each failed reingest attempt            |
| data_source          | string    | carried from quarantine `_data_source` — the Silver MERGE target |
| quarantine_generated_at | timestamp | carried from quarantine `_generated_at`          |
| run_id               | string    | Databricks parent job run id, for lineage           |
| created_at / updated_at | timestamp | |

Retry cap is 3 (`max_retries` widget / bundle var) — beyond that, a row
auto-escalates regardless of confidence, so nothing loops forever.
`revalidated_pass` is the transient state between `04` and `05` in one run.

`resolved_duplicate` / `resolved_removed` are terminal, non-error states: a
playbook decided the row should be dropped (e.g. superseded by a newer
duplicate), and it's neither reingested nor retried nor escalated.

## Remediation config — per-dataset YAMLs in a Volume + one file (`config/README.md`)

**`/Volumes/<catalog>/<gov>/remediation_playbooks/<dataset>.yaml`** — one file
per dataset (filename = the quarantine table's last name segment). `03a` reads
this dataset's file at run time; DQX Studio uploads it. **Adding/changing a rule
needs no deploy.** No file → every failing row goes to the agent. Full spec:
`docs/remediation_playbook.md`; template `config/remediation_playbook.sample.yaml`.
The Volume is created by `00_setup_governance`.
Entries run in `priority` order (first match per row wins). Strategies:
`dedupe_keep_latest` (row-number over the key, keep row 1, drop the rest →
`resolved_duplicate`; `ci_columns` = case-insensitive partition cols),
`standardize_value`, `fill_default`, `reject_rows` (matched rows →
terminal-`rejected`). **Dedup is configured only here** — the agent never dedups.
An entry may carry a `where` SQL predicate to route by value (one `rule_name`,
several entries; put `reject_rows` before `dedupe` via `priority`). Strategy
*functions* live in `notebooks/lib/remediation_strategies.py` / `STRATEGY_REGISTRY`
— a new *kind* of fix needs a deploy; a new entry for an existing strategy is
just an upload. `03a` skips entries still containing `REPLACE_ME`.

**`config/agent_curation_policy.yaml`** — governs `03_agent_curate` (the fallback
for rules with no playbook entry). `decision_policy` (`fix`/`reject`/`escalate`
prose) is rendered into the model's system prompt. `cross_table_probe` handles
`sql_query` (cross-table) checks *without* the LLM: on a sample it tests whether
normalizing the compared column (`trim`/`lower_trim`/`upper_trim`) clears the
mismatch against `other_table`; resolves → propose that transform, else escalate.
The transform vocabulary is code (`build_col_expr` in `03`); the YAML lists it
for reference.

A `drop` action (e.g. a duplicate that lost the "keep latest" comparison)
is a terminal resolution, not a failure — it never goes through DQX
re-validation and never reaches the agent or the retry loop.

## Notebooks

- `00_setup_governance.py` — idempotent DDL for `_dq_review_queue`,
  `_dq_reingest_audit`, `dq_summary_metrics`, `_dq_dry_run_report`. Run once
  per env before the first job run.
- `01_discover_quarantine_tables.py` — `SHOW TABLES IN <catalog>.quarantine`,
  emits the FQN list as a task value for the parent job's For Each task.
- `02_triage.py` — pulls unprocessed rows from one quarantine table, dedupes
  to the latest quarantined version per `_row_id`, flattens `_error`/`_warning`
  `.name` into `rule_violations`, seeds `_dq_review_queue`.
- `03a_playbook_remediate.py` — checks each pending row's failed rule(s)
  against the documented playbook; applies the matching deterministic
  strategy and marks those rows resolved. Only rows with no playbook match
  stay `pending` and reach the agent.
- `03_agent_curate.py` — fallback for rules with no documented fix. **One LLM
  call per distinct violation signature** (not per row): the model sees the
  rule(s), the flagged column(s), a sample of failing values and passing
  examples, and returns a single deterministic **transform per column** from a
  fixed vocabulary (`trim`, `lower_trim`, `regexp_replace`, `to_date`,
  `set_default`, …). Spark applies that transform to every row in the
  signature to compute the corrected values, which go into
  `_dq_review_queue.proposed_fix`. `fix` below the confidence threshold →
  `escalated`. Writes to `_dq_review_queue` only. Emits `min_batch_confidence`
  (the min `fix` confidence) for the gate. Cost is ~1 model call per
  rule-failure class regardless of row count.
- `04_apply_and_validate.py` — picks up `curated` (confidence ≥ threshold) and
  retryable `revalidated_fail` rows. `drop` → resolved by removal; `keep`/
  `patch_fields` → patch applied, `_*` bookkeeping columns stripped, re-run
  through `Validator.approved_checks_for(silver_fqn)`. Passing rows staged +
  `revalidated_pass`; failures `revalidated_fail`, `retry_count += 1`.
  Cross-table quarantine or a keyless `_row_id` → `escalated`.
- `05_reingest.py` — dynamic `MERGE INTO` Silver (on `_row_id_keys`, columns
  intersected with the target) for staged rows, tagged with lineage
  (`agent_curated`, `confidence`, `reingest_run_id`, `reingested_at`). Queue
  rows → `reingested`. `revalidated_fail` at the retry cap → `escalated`.
- `06_audit_metrics.py` — one row per (run, FQN) into `_dq_reingest_audit`
  plus a DQX-summary-metrics-shaped row into `dq_summary_metrics` for the
  shared dashboard. Counts from upstream task values + a queue snapshot.
- `07_dry_run_report.py` — **read-only estimate**. Per FQN: computes the
  deterministic (playbook) half for real — match → strategy → re-run the exact
  DQX checks — and reports the agent half as a count only (the model is never
  called). Appends one row per (run, FQN) to `_dq_dry_run_report` with the
  estimated insert/update split against Silver. Writes nothing to the review
  queue, a staged table, or Silver.

## Workflow orchestration

A Databricks For Each task wraps a single task, not a subgraph, so the
per-FQN pipeline is a **child job**:

- **Parent** (`dqx_agentic_reingest`): `01_discover` → For Each over the FQN
  list → one `run_job_task` per FQN into the child. Carries the trigger.
- **Child** (`dqx_agentic_reingest_process`): `02` → `03a` → `03` →
  `confidence_gate` → (`04` → `05`) with `06` on
  `AT_LEAST_ONE_SUCCESS` of `reingest` or `confidence_gate == false`.
- **Confidence gate** between `03_agent_curate` and `04_apply_and_validate`:
  `min_batch_confidence >= confidence_threshold` → auto-curate path; below →
  straight to `06`, skipping auto-reingest for that FQN this run.
- **Trigger**: table-update trigger on `<catalog>.quarantine.*`. A job takes
  exactly one of `trigger` / `schedule` / `continuous`; if your tier lacks
  table triggers, swap the `trigger` block for a `schedule` (see the comment
  in `resources/dqx_agentic_reingest_job.yml`).
- **Retries**: enabled only on `03_agent_curate` (transient API failures).
  `04_apply_and_validate` never retries automatically — a validation failure
  is a real signal, not a flake.

### Dry runs

Two ways to see how much would reingest without committing it:

- **`dqx_dry_run` job** (`databricks bundle run dqx_dry_run -t dev`) — `01` +
  For Each → `07_dry_run_report`. Zero side effects except the
  `_dq_dry_run_report` table. Never calls the LLM. Use this to estimate before
  turning the system on.
- **`dry_run=true` job parameter** on `dqx_agentic_reingest`
  (`--params dry_run=true`) — runs the whole pipeline (queue + staged table
  *are* written by 02–04, the agent *is* called) but `05` computes the Silver
  insert/update split and holds the `MERGE`, the queue flip to `reingested`,
  and the retry-cap escalation. Use this in staging to inspect exactly what
  would land.

## Deployment

**Step-by-step deploy runbook for `qa` and `prod` is in [`README.md`](README.md)
("Deployment").** In short:

- **`databricks.yml` + `resources/dqx_agentic_reingest_job.yml`** — a Databricks Asset
  Bundle (DAB), the recommended path. Targets are **not** in `databricks.yml`:
  each env is a git-ignored `target.<name>.yml` (`include: [target.*.yml]`),
  copied from the tracked `targets.example.yml` template. `qa` is deployed in
  production mode with the trigger/schedule paused via a preset. `databricks
  bundle deploy -t <target>` builds the wheel, uploads notebooks, and creates
  four jobs — `dqx_setup_governance` (run once per target to create the
  governance tables), `dqx_agentic_reingest` (parent),
  `dqx_agentic_reingest_process` (child, id wired into the parent's
  `run_job_task` by DAB), `dqx_dry_run`. Bundle variables `env` / `catalog` /
  `quarantine_schema` / `governance_schema` drive per-env names; `var.catalog`
  must match `dqx-validator`'s `catalog_template` (`<catalog>`).
  `governance_schema` is commonly set to the DQX Studio rules schema
  (`dqx_studio` / `dqx_app`) so the review queue, audit, metrics and the
  `remediation_playbooks` Volume sit next to `dq_quality_rules`.
  **The governance tables + Volume are created by the `00_setup_governance`
  notebook, not declared as DAB `resources`** — so `databricks bundle destroy`
  removes the four jobs and the uploaded files but leaves the governance schema
  (queue, audit, metrics, playbooks) intact. Compute is serverless; the reingest
  jobs pin an `environment` adding `databricks-labs-dqx` + the `dqx-validator`
  wheel. `build: python -m build --wheel` runs first (needs `pip install build`,
  or swap for `uv build --wheel`).
- **`workflow/*.json`** — the equivalent raw Jobs API payloads
  (`dqx_agentic_reingest_process_job.json`, then `dqx_agentic_reingest_parent_job.json` with the
  child job id filled in; `dqx_dry_run_job.json` standalone), for REST /
  Terraform deploys. See `workflow/README.md`. Mirror DAG changes here or drop
  the directory once the bundle is the only deploy path.

## Guardrails

- Agent is called **once per distinct violation signature**, never per row —
  one rule-failure class in, one transform spec out, applied by Spark to every
  row in the class. Token cost is independent of quarantine volume.
- Agent prompt always includes: the DQX rule name / function / message, a
  sample of failing values, and up to 5 passing example values per flagged
  column. The agent may only choose a **transform from a fixed vocabulary**
  for a column DQX flagged — no free-form expressions, no unflagged columns,
  no schema changes.
- Every agent decision is logged with its full input/output for audit —
  required before this touches production PII-adjacent identity data.
- `agent_curated=true` and `confidence` are carried into Silver as lineage
  columns so downstream consumers can filter agent-curated rows if needed.

## Open items / not yet built

- Human review UI for `curated_pending_review` (currently just a table;
  could be a Databricks App or routed to Slack).
- Per-rule confidence threshold tuning (currently one global threshold).
- Cost tracking for the agent calls per pipeline run.
- Dataset-level playbook strategies (like `dedupe_keep_latest`) currently
  only dedupe within the candidate quarantine batch. If the surviving row's
  key can also collide with something already in Silver, `04` needs a lookup
  against the target table before trusting the re-validation pass — flagged
  in `04_apply_and_validate.py` but not yet implemented.
- Playbooks are per-dataset YAML files in a Volume, uploaded by DQX Studio; the
  Studio upload/validate piece is not built here — see
  `docs/remediation_playbook.md` for the file spec + the validation it should
  enforce. The repo keeps only `config/remediation_playbook.sample.yaml`.
- `agent_curation_policy.yaml` (decision prose + cross-table-probe config) is
  still bundle-deployed; could move to the Volume too if it grows per-dataset.
