# Remediation playbook — file spec

> Reference for building / maintaining the **DQX remediation playbooks**: the
> per-dataset YAML files that say how a quarantined data-quality failure is
> fixed deterministically. Hand this to an agent building the DQX Studio piece
> that uploads these files, or use it to author them by hand.

---

## Context

A DQX pipeline validates data against approved quality rules and drops failing
rows into a per-source **quarantine table**
(`<catalog>.quarantine.<src_schema>_<src_table>`). Downstream, an agentic
re-ingestion workflow tries to fix those rows and merge the good ones back:

```
quarantine row  ─▶  03a_playbook_remediate  ─▶  reads THIS dataset's playbook YAML
                          │  an entry matches ─▶ apply its strategy deterministically ─▶ row resolved
                          │  no entry / no file ─▶ 03_agent_curate (an LLM proposes fix / reject / escalate)
                          ▼
                    04_apply_and_validate   ─▶ re-run the exact DQX checks; only passing rows reach Silver
```

The playbook is the **documented, deterministic half** — "a rule owner said
exactly how this failure is fixed, so don't ask the LLM".

## Where the files live

**One YAML per dataset, in a Unity Catalog Volume:**

```
/Volumes/<catalog>/<governance_schema>/remediation_playbooks/<dataset>.yaml
```

`<dataset>` = the quarantine table's **last name segment**. e.g. for
`main.quarantine.silver_orders` →
`/Volumes/main/dqx_studio/remediation_playbooks/silver_orders.yaml`

DQX Studio uploads the file to that path. `03a` reads it at run time for that
dataset — **no bundle deploy** to add or change a rule. A dataset with no file
(or an empty `playbooks:` list) sends every failing row to the LLM agent.

The Volume is created by `00_setup_governance`. The repo keeps only a template:
`config/remediation_playbook.sample.yaml`.

---

## File format

```yaml
playbooks:

  - rule_name: <exact DQX check name>   # the `.name` of the structs in the
                                        # quarantine _error / _warning columns
    priority: 10                        # int; lower runs first; first match wins
    where: "some_col rlike '^X'"        # optional SQL boolean over the row's data columns
    strategy: reject_rows               # dedupe_keep_latest | standardize_value
                                        #  | fill_default | reject_rows
    params: { ... }                     # per strategy (below)
    description: >                      # optional, free text
      why this fix

  - rule_name: ...
    priority: 20
    ...
```

- Entries are sorted by `priority`; the **first** entry whose `rule_name` is in
  a row's failed-rule list — and whose `where` matches, if set — handles the
  row. Later entries don't see it. **Put `reject_rows` before `dedupe_keep_latest`**
  for rows that could match both.
- The same `rule_name` may appear several times with different `where` — route
  by value (`classification = 'Duplicate'` → dedupe; `'Invalid'` → no entry →
  agent). A row matching no entry's `where` falls through to the agent.
- `03a` skips any entry that still contains the literal `REPLACE_ME` (use it as
  a placeholder while an entry is incomplete).
- `match_fqns` is accepted but redundant now (the file already scopes to one
  dataset); omit it.

### Strategies

| strategy | required params | optional params | effect | resulting status |
|---|---|---|---|---|
| `dedupe_keep_latest` | `partition_by` (list), `order_by` | `order_direction` (`desc` default \| `asc`), `ci_columns` (list) | `row_number()` over `partition_by` ordered by `order_by` `order_direction`; keep row 1, drop the rest. `ci_columns` = partition cols compared case-insensitively (`Open` == `open`). | keep → `curated` (re-validated by `04`); drop → `resolved_duplicate` (terminal) |
| `standardize_value` | `column`, `transform` | — | patch `column` with `transform` (`lower_trim` \| `trim` \| `upper_trim`), then re-validate | `curated` |
| `fill_default` | `column`, `default_expr` | — | fill null/missing `column` with the SQL expression (`current_timestamp()`, `'UNKNOWN'`, …), then re-validate | `curated` |
| `reject_rows` | — | `reason` (audit string) | drop every matched row | `rejected` (terminal — never re-validated, never reingested, never seen by the agent) |

---

## Example playbooks

These are illustrative — swap in your own dataset names, rule names, and
columns. Each file is named for the quarantine table's last segment.

### `silver_orders.yaml`
| priority | rule_name | where | strategy | params |
|---|---|---|---|---|
| 10 | `code_not_matching_regex` | `code rlike '^[A-Za-z]'` | `reject_rows` | `reason` = "code has an alphabetic prefix; not a valid numeric code" |
| 20 | `struct_customer_id_order_ts_is_not_unique` | — | `dedupe_keep_latest` | `partition_by`=`[customer_id, order_ts]`, `order_by`=`updated_at`, `order_direction`=`desc` |

### `silver_customers.yaml`
| priority | rule_name | where | strategy | params |
|---|---|---|---|---|
| 10 | `code_not_matching_regex` | `code rlike '^[A-Za-z]'` | `reject_rows` | `reason` = … |
| 20 | `struct_customer_id_signup_ts_is_not_unique` | — | `dedupe_keep_latest` | `partition_by`=`[customer_id, signup_ts]`, `order_by`=`created_at`, `order_direction`=`asc` |

### `silver_shipments.yaml`
| priority | rule_name | where | strategy | params |
|---|---|---|---|---|
| 10 | `code_not_matching_regex` | `code rlike '^[A-Za-z]'` | `reject_rows` | `reason` = … |
| 20 | `status_is_not_in_the_list` | `status = 'Duplicate'` | `dedupe_keep_latest` | `partition_by`=`[shipment_id, status_reported_on, category, sub_category]`, `ci_columns`=`[category, sub_category]`, `order_by`=`event_ts`, `order_direction`=`desc` |

---

## Source-table columns the params can reference

`partition_by`, `order_by`, `ci_columns`, `column`, and `where` reference the
**source table's own columns** — every column on the quarantine table except
the `_`-prefixed bookkeeping ones. The columns below are a sample shape, not a
required schema.

### `silver_orders`
`order_id string`, `customer_id string`, `order_ts timestamp`,
`updated_at timestamp`, `code string`, `status string`, `amount decimal`,
`city string`, `state string`, `zip_code string`, `ingest_date string`,
`uuid string`, …
- `code` — valid: `09022`, `06762`; failing: `A1005`, `B3218` (alpha prefix)
- `status` — allowed list e.g. `Open`, `Closed`; failing seen: `Duplicate`, `Invalid`
- `(customer_id, order_ts)` is the uniqueness key for
  `struct_customer_id_order_ts_is_not_unique`

### `silver_customers`
`customer_id string`, `signup_ts timestamp`, `created_at timestamp`,
`code string`, `email string`, `region string`, `ingest_date string`,
`uuid string`, …
- `(customer_id, signup_ts)` is the uniqueness key for
  `struct_customer_id_signup_ts_is_not_unique`

### `silver_shipments`
`shipment_id string`, `code string`, `carrier string`, `category string`,
`sub_category string`, `status string`, `status_reported_on timestamp`,
`event_ts string`, `ingest_date string`, `uuid string`, …
- `(shipment_id, status_reported_on, category, sub_category)` is the uniqueness key
- `category` — e.g. `Open`, `Closed`; `sub_category` — e.g. `Standard`, `Merged`, `Expedited`
- ⚠️ if a tie-breaker column like `event_ts` is stored as a **string**,
  `order_direction: desc` sorts it lexically — correct only if the string is ISO
  (`yyyy-MM-dd…`). If it's `MM/dd/yyyy`, order by a real timestamp column instead.

---

## Validation the DQX Studio upload should enforce

Before writing a file to the Volume:

- valid YAML with a top-level `playbooks:` list.
- each entry: `rule_name` non-empty; `priority` an int (unique within the file);
  `strategy` ∈ {`dedupe_keep_latest`, `standardize_value`, `fill_default`, `reject_rows`}.
- `dedupe_keep_latest`: `partition_by` a non-empty list, `order_by` set,
  `order_direction` ∈ {`asc`, `desc`}, `ci_columns` ⊆ `partition_by`.
- `standardize_value`: `column` set, `transform` ∈ {`lower_trim`, `trim`, `upper_trim`}.
- `fill_default`: `column` and `default_expr` set.
- every column named in `partition_by` / `order_by` / `ci_columns` / `column`
  exists on this dataset's source table.
- `where`, if set, parses as a boolean SQL expression over those columns.
- `(rule_name, where)` unique within the file.
- **filename** = `<quarantine-table-last-segment>.yaml` (this is how `03a` finds it).
- an entry containing the literal `REPLACE_ME` is allowed (it's skipped at
  runtime) — warn, don't block.
