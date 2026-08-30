# Remediation config

How a quarantined row gets fixed is decided by **per-dataset playbook YAMLs in a
Volume** + one policy file (plus the strategy code they call). Nothing is
hardcoded in the job.

```
02_triage            seeds _dq_review_queue (status = pending)
        │
        ▼
03a_playbook_remediate   ── reads ─▶  /Volumes/<cat>/<gov>/remediation_playbooks/<dataset>.yaml
        │                              (+ notebooks/lib/remediation_strategies.py)
        │   entry matches (by priority) ─▶ apply strategy deterministically, row leaves 'pending'
        │
        ▼   (no entry / no file)
03_agent_curate          ── reads ─▶  agent_curation_policy.yaml
        │                              one LLM call per violation signature
        │   fix | reject | escalate
        ▼
04_apply_and_validate     re-runs the exact DQX checks; only passes reach Silver
```

## Playbook YAMLs — deterministic, documented fixes

**Full file spec: [`../docs/remediation_playbook.md`](../docs/remediation_playbook.md).**
**Template: [`remediation_playbook.sample.yaml`](remediation_playbook.sample.yaml).**

One YAML per dataset, in the Volume
`/Volumes/<catalog>/<governance_schema>/remediation_playbooks/`, filename =
the quarantine table's last name segment (e.g. `silver_orders.yaml`).
DQX Studio uploads them there; `03a` reads this dataset's file at run time.
**Adding or changing a rule needs no deploy.** A dataset with no file → every
failing row goes to the LLM agent.

Each entry maps a DQX `rule_name` to a `strategy` + params:

| strategy | params | what it does |
|---|---|---|
| `dedupe_keep_latest` | `partition_by`, `order_by`, `order_direction?`, `ci_columns?` | `row_number()` over the key ordered by the tie-breaker; keep row 1 (`curated`), drop the rest (`resolved_duplicate`, terminal). `ci_columns` = partition columns compared case-insensitively |
| `standardize_value` | `column`, `transform` (`lower_trim`/`trim`/`upper_trim`) | normalize one column, propose as a patch |
| `fill_default` | `column`, `default_expr` | fill null/missing with a fixed expression |
| `reject_rows` | `reason?` | mark every matched row `rejected` (terminal — no re-validate, no agent, no reingest) |

- Entries run in `priority` order; the **first** entry matching a row wins.
  Put `reject_rows` before `dedupe_keep_latest`.
- Strategy **functions** are code (`notebooks/lib/remediation_strategies.py`,
  `STRATEGY_REGISTRY`) — a new *kind* of fix needs a deploy; a new entry for an
  existing strategy is just an upload.
- **`where`** (optional SQL predicate) scopes an entry by value — one
  `rule_name` can have several entries (`classification = 'Duplicate'` → dedupe,
  `'Invalid'` → no entry → agent rejects).
- **Deduplication is only configured here** — the agent never dedups.
- `03a` skips any entry that still contains the literal `REPLACE_ME`.

## `agent_curation_policy.yaml` — the LLM fallback's rules

Governs `03_agent_curate` for rules with **no** playbook entry. Sections:

- **`decision_policy`** — `fix` / `reject` / `escalate` definitions, rendered
  verbatim into the model's system prompt. Edit these to change how the agent
  triages.
  - **fix** — one transform from the vocabulary that genuinely changes the value
    (typos, column shifts, date formats, casing, padding, regex-strippable junk).
  - **reject** — drop the row: required value empty/null, value can't satisfy a
    regex/allowed-list, or an excluded business segment (e.g. a check that flags
    records outside the in-scope region or product line).
  - **escalate** — cross-table mismatches the probe can't resolve, lookup-table
    cases, true duplicates with no tie-breaker, anything ambiguous.
- **`allowed_transforms`** — reference list; the real implementation is
  `build_col_expr` in `notebooks/03_agent_curate.py`. Add there first.
- **`cross_table_probe`** — for `sql_query` (cross-table) checks, `03` first
  tests on a sample whether normalizing the compared column (`trim` /
  `lower_trim` / `upper_trim`) removes the mismatch against `other_table`. If a
  normalization clears ≥ `resolve_threshold` of the sample → propose it as the
  fix; otherwise escalate. The LLM is not called for these. One entry per
  cross-table rule: `compare_column`, `other_table`, `join_keys`.

## Getting the exact rule names

Run a dry run and read `03_agent_curate`'s output (`--params dry_run=true`):
each `signatures[].signature` and `signatures[].rules[].rule` is an exact DQX
check name you can paste into either YAML.
