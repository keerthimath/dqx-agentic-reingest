# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC ## 03 · Agent curate
# MAGIC Runs **after** `03a_playbook_remediate`, once per quarantine FQN. Only
# MAGIC handles rows whose failed rule has no documented playbook fix (still
# MAGIC `status = 'pending'`).
# MAGIC
# MAGIC **One LLM call per distinct violation signature**, not per row. For each
# MAGIC signature the model sees the rule(s) (name / function / message), the
# MAGIC flagged column(s), a small SAMPLE of failing values, and passing example
# MAGIC values — and returns a **single deterministic transform per column** from
# MAGIC a fixed vocabulary. That transform is then applied by Spark to **every**
# MAGIC row in the signature to compute the corrected values, which land in
# MAGIC `_dq_review_queue.proposed_fix`. `04_apply_and_validate` re-checks them.
# MAGIC
# MAGIC Cost is ~1 model call per rule-failure class regardless of row count.
# MAGIC
# MAGIC Decision → status:
# MAGIC   `fix` & confidence ≥ threshold → `curated`   (flows to 04)
# MAGIC   `fix` & confidence < threshold → `escalated` (human review)
# MAGIC   `reject`                       → `rejected`
# MAGIC   `escalate`                     → `escalated`

# COMMAND ----------
dbutils.widgets.text("quarantine_fqn", "")
dbutils.widgets.text("review_queue_fqn", "main.dqx_studio._dq_review_queue")
dbutils.widgets.text("model_endpoint", "system.ai.claude-sonnet-4-6")
dbutils.widgets.text("confidence_threshold", "0.8")
dbutils.widgets.text("sample_size", "15")  # failing rows per signature shown to the model
dbutils.widgets.text("policy_path", "/Workspace/dqx-agentic-reingest/config/agent_curation_policy.yaml")

quarantine_fqn = dbutils.widgets.get("quarantine_fqn")
review_queue_fqn = dbutils.widgets.get("review_queue_fqn")
model_endpoint = dbutils.widgets.get("model_endpoint")
confidence_threshold = float(dbutils.widgets.get("confidence_threshold"))
sample_size = int(dbutils.widgets.get("sample_size"))
policy_path = dbutils.widgets.get("policy_path")

# COMMAND ----------
import yaml
try:
    with open(policy_path) as f:
        POLICY = yaml.safe_load(f) or {}
    print(f"loaded curation policy from {policy_path}")
except Exception as e:
    print(f"WARNING: could not load {policy_path} ({e}); using built-in defaults")
    POLICY = {}

DECISION_POLICY = POLICY.get("decision_policy") or {}
PROBE = POLICY.get("cross_table_probe") or {}

# COMMAND ----------
from pyspark.sql import functions as F
import json
import re

pending = (
    spark.table(review_queue_fqn)
    .filter(F.col("quarantine_fqn") == quarantine_fqn)
    .filter(F.col("status") == "pending")
    .select("row_id", "rule_violations")
    .withColumn("_sig", F.concat_ws("|", F.array_sort(F.coalesce(F.col("rule_violations"), F.array()))))
)
n_pending = pending.count()

if n_pending == 0:
    print(f"No pending rows for {quarantine_fqn} — playbook covered everything or nothing to triage.")
    dbutils.jobs.taskValues.set(key="min_batch_confidence", value=1.0)
    dbutils.notebook.exit("0")

from pyspark.sql import Window
_latest = Window.partitionBy("_row_id").orderBy(F.col("_generated_at").desc_nulls_last())
q = (
    spark.table(quarantine_fqn)
    .withColumn("_rn", F.row_number().over(_latest))
    .filter(F.col("_rn") == 1).drop("_rn")
    .withColumn("row_id", F.col("_row_id"))   # latest pass per _row_id -> 1:1 join
)
data_columns = [c for c in q.columns if not c.startswith("_") and c != "row_id"]
pend = q.join(pending.drop("rule_violations"), on="row_id")

signatures = [r["_sig"] for r in pend.select("_sig").distinct().collect()]
data_sources = sorted({r["_data_source"] for r in pend.select("_data_source").distinct().collect() if r["_data_source"]})
silver_fqn = data_sources[0] if len(data_sources) == 1 and "," not in data_sources[0] else None
print(f"{n_pending} pending rows across {len(signatures)} violation signature(s); Silver source: {silver_fqn}")

# COMMAND ----------
def _to_dict(x):
    if isinstance(x, dict):
        return x
    if hasattr(x, "asDict"):
        return x.asDict()
    try:
        return dict(x)
    except Exception:
        return {}


def _jsonable(v):
    return v if v is None or isinstance(v, (str, int, float, bool)) else str(v)


def passing_examples_for(cols):
    out = {}
    if not (silver_fqn and cols):
        return out
    try:
        s = spark.table(silver_fqn)
        for c in cols:
            if c in s.columns:
                out[c] = [
                    r["v"] for r in
                    s.select(F.col(c).cast("string").alias("v")).filter(F.col("v").isNotNull())
                     .distinct().limit(5).collect()
                ]
    except Exception as e:
        print(f"  passing examples unavailable ({silver_fqn}): {e}")
    return out


# --- safe transform vocabulary (no arbitrary expressions) -------------------
def apply_transform(fix: dict, c):
    """Apply one transform to the running column expression `c`."""
    t = (fix.get("transform") or "").strip()
    if t == "trim":             return F.trim(c)
    if t == "lower":            return F.lower(c)
    if t == "upper":            return F.upper(c)
    if t == "lower_trim":       return F.trim(F.lower(c))
    if t == "upper_trim":       return F.trim(F.upper(c))
    if t == "regexp_replace":   return F.regexp_replace(c, fix["pattern"], fix.get("replacement", ""))
    if t == "regexp_extract":   return F.regexp_extract(c, fix["pattern"], int(fix.get("group", 0)))
    if t == "to_date":          return F.date_format(F.to_date(c, fix["from_format"]), "yyyy-MM-dd")
    if t == "to_timestamp":     return F.date_format(F.to_timestamp(c, fix["from_format"]), "yyyy-MM-dd HH:mm:ss")
    if t == "left_pad":         return F.lpad(c, int(fix["length"]), str(fix.get("pad", "0")))
    if t == "right_pad":        return F.rpad(c, int(fix["length"]), str(fix.get("pad", "0")))
    if t == "substring":        return F.substring(c, int(fix["pos"]), int(fix.get("len", 1_000_000)))
    if t == "set_default":      return F.coalesce(c, F.lit(str(fix["value"])))
    if t == "set_null":         return F.lit(None).cast("string")
    raise ValueError(f"unsupported transform: {t!r}")


def build_column_exprs(fixes):
    """
    fixes is a list of {column, transform, ...params}. Multiple entries for the
    same column are chained in order (e.g. regexp_replace then left_pad).
    Returns {column: Column expr (cast to string)}.
    """
    exprs = {}
    for fx in fixes:
        col = fx.get("column")
        if col not in data_columns:
            continue
        exprs[col] = apply_transform(fx, exprs.get(col, F.col(col)))
    return {c: e.cast("string") for c, e in exprs.items()}


ALLOWED_TRANSFORMS = (
    "trim, lower, upper, lower_trim, upper_trim, "
    "regexp_replace{pattern,replacement}, regexp_extract{pattern,group}, "
    "to_date{from_format}, to_timestamp{from_format}, "
    "left_pad{length,pad}, right_pad{length,pad}, substring{pos,len}, "
    "set_default{value}, set_null"
)

# COMMAND ----------
def _policy_block(key, fallback):
    txt = (DECISION_POLICY.get(key) or "").strip()
    return txt if txt else fallback


AGENT_SYSTEM_PROMPT = f"""You are a data quality remediation assistant. You are
given ONE class of failure: the DQX rule(s) that failed together, the flagged
column(s), a SAMPLE of failing values, and a SAMPLE of passing values from the
good data. Propose deterministic transforms (from the allowed vocabulary only)
that will be applied to EVERY row in this class.

You MAY chain transforms on the same column by giving several `fixes` entries
with the same `column` — they are applied in order. e.g. strip an alpha prefix
then zero-pad:
  [{{"column":"code","transform":"regexp_replace","pattern":"^[A-Za-z]+","replacement":""}},
   {{"column":"code","transform":"left_pad","length":5,"pad":"0"}}]
The chained result must actually conform to the rule (match the passing-value
shape). If it can't, choose reject or escalate instead.

Allowed transforms (params in braces): {ALLOWED_TRANSFORMS}

Decide "fix", "reject", or "escalate" per this policy:

FIX:
{_policy_block("fix", "Transforms that genuinely make the value conform.")}

REJECT:
{_policy_block("reject", "The value is unrecoverable, or the row is an excluded segment.")}

ESCALATE:
{_policy_block("escalate", "Anything not clearly fix or reject.")}

Never propose transforms that leave the value unchanged.

Respond with ONLY the JSON object below — nothing before or after it, no
markdown fences:
{{"decision":"fix|reject|escalate",
  "fixes":[{{"column":str,"transform":str,"<param>":"<value>"}}],
  "confidence":0.0-1.0,"reasoning":str}}
"""


def _extract_json_obj(text: str):
    """First balanced {...} block in text (handles prose / fences around it)."""
    start = text.find("{")
    if start < 0:
        return None
    depth, in_str, esc = 0, False, False
    for i in range(start, len(text)):
        ch = text[i]
        if in_str:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == '"':
                in_str = False
        elif ch == '"':
            in_str = True
        elif ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return text[start:i + 1]
    return None


def _parse_one(text: str) -> dict:
    blob = _extract_json_obj((text or "").strip())
    if not blob:
        raise ValueError(f"no JSON object in model response: {(text or '')[:200]!r}")
    try:
        return json.loads(blob)
    except json.JSONDecodeError:
        import ast  # python-dict style (single quotes etc.)
        return ast.literal_eval(blob)


def _ai_query_one(target: str, prompt: str) -> str:
    return spark.sql(
        f"SELECT ai_query('{target}', :p, "
        f"modelParameters => named_struct('max_tokens', 2000, 'temperature', 0.0)) AS r",
        args={"p": prompt},
    ).first()["r"]


def ai_query(prompt: str) -> str:
    try:
        return _ai_query_one(model_endpoint, prompt)
    except Exception as e:
        if model_endpoint.startswith("system.ai."):
            alt = "databricks-" + model_endpoint.split(".")[-1]
            print(f"  ai_query('{model_endpoint}') failed ({type(e).__name__}); retrying '{alt}'")
            return _ai_query_one(alt, prompt)
        raise


def ask_model(prompt: str) -> dict:
    """ai_query + parse, with one corrective retry if the reply won't parse."""
    try:
        return _parse_one(ai_query(prompt))
    except Exception as e1:
        print(f"  parse failed ({type(e1).__name__}: {str(e1)[:120]}); one retry")
        retry = prompt + ("\n\nYour previous reply could not be parsed. Reply with "
                          "ONLY the JSON object — no prose, no code fences.")
        return _parse_one(ai_query(retry))


# COMMAND ----------
_NORM = {
    "trim": lambda c: F.trim(c),
    "lower_trim": lambda c: F.trim(F.lower(c)),
    "upper_trim": lambda c: F.trim(F.upper(c)),
}


def cross_table_probe(rule_name, grp):
    """
    For a `sql_query` (cross-table) rule with an agent_curation_policy.yaml
    entry: on a sample, test whether normalizing `compare_column` removes the
    mismatch against `other_table`.
    Returns (decision_dict_or_None, note). None -> fall through to the LLM.
    """
    rc = (PROBE.get("rules") or {}).get(rule_name)
    if not rc:
        return None, f"no policy entry for '{rule_name}'"
    col, other, jk = rc["compare_column"], rc["other_table"], list(rc["join_keys"])
    if col not in data_columns:
        return None, f"compare_column '{col}' not on the quarantine table"
    sample_n = int(PROBE.get("sample_rows", 200))
    thr = float(PROBE.get("resolve_threshold", 0.9))
    norms = [nm for nm in (PROBE.get("normalizations") or []) if nm in _NORM]

    try:
        left = grp.limit(sample_n).select(
            *[F.col(k).cast("string").alias("__j_" + k) for k in jk],
            F.col(col).cast("string").alias("__lv"),
        )
        right = (
            spark.table(other)
            .select(
                *[F.col(k).cast("string").alias("__j_" + k) for k in jk],
                F.col(col).cast("string").alias("__rv"),
            )
            .dropDuplicates(["__j_" + k for k in jk])
        )
        j = left.join(right, on=["__j_" + k for k in jk], how="inner")
        total = j.count()
    except Exception as e:
        return None, f"probe error: {type(e).__name__}: {str(e)[:200]}"
    if total == 0:
        return None, f"0 quarantine rows joined to {other} on {jk}"

    for nm in norms:
        f = _NORM[nm]
        matched = j.filter(f(F.col("__lv")).eqNullSafe(f(F.col("__rv")))).count()
        frac = matched / total
        print(f"  probe {rule_name}: {nm}({col}) resolves {matched}/{total} ({frac:.2f})")
        if frac >= thr:
            return ({"decision": "fix", "confidence": round(0.6 + 0.35 * frac, 2),
                     "fixes": [{"column": col, "transform": nm}],
                     "reasoning": (f"cross-table probe: {nm}({col}) matches {other} "
                                   f"for {matched}/{total} sampled rows")},
                    f"{nm} resolved {matched}/{total}")
    return ({"decision": "escalate", "confidence": 0.85, "fixes": [],
             "reasoning": (f"cross-table probe: no normalization resolved the {col} "
                           f"mismatch vs {other} ({total} sampled) — real value conflict")},
            f"no normalization resolved ({total} sampled)")


# COMMAND ----------
results = []       # (row_id, decision, proposed_fix|None, confidence, reasoning)
fix_confidences = []
sig_summaries = []  # one per signature, for the notebook exit value

for sig in signatures:
    grp = pend.filter(F.col("_sig") == sig)
    n = grp.count()
    sample = grp.limit(sample_size).collect()

    rule_info = {}
    for r in sample:
        d = _to_dict(r)
        for e in (list(d.get("_error") or []) + list(d.get("_warning") or [])):
            e = _to_dict(e)
            rule_info.setdefault(e.get("name"), {
                "function": e.get("function"),
                "message": e.get("message"),
                "columns": list(e.get("columns") or []),
            })
    flagged = sorted({c for ri in rule_info.values() for c in ri["columns"] if c in data_columns})
    failing_samples = {c: [_jsonable(_to_dict(r).get(c)) for r in sample] for c in flagged}
    pex = passing_examples_for(flagged)

    user = {
        "rules": [{"rule": k, **v} for k, v in rule_info.items()],
        "flagged_columns": flagged,
        "n_rows_with_this_signature": n,
        "failing_value_samples": failing_samples,
        "passing_value_examples": pex,
    }
    print(f"\n=== signature: {sig}  ({n} rows, flagged={flagged}) ===")

    # Cross-table (sql_query) rule? Try the deterministic join-tweak probe first;
    # only fall through to the LLM if there's no probe config / no matching rows.
    decision = None
    probe_note = None
    for rn, ri in rule_info.items():
        if ri.get("function") == "sql_query":
            decision, why = cross_table_probe(rn, grp)
            probe_note = f"{rn}: {why}"
            print(f"  [cross-table probe] {rn}: {why}")
            if decision:
                print(f"  [cross-table probe] -> {decision['decision']}: {decision['reasoning']}")
                break

    if decision is None:
        try:
            decision = ask_model(AGENT_SYSTEM_PROMPT + "\n\nINPUT:\n" + json.dumps(user, default=str))
        except Exception as e:
            print(f"  agent call/parse failed: {type(e).__name__}: {e}")
            decision = {"decision": "escalate", "fixes": [], "confidence": 0.0,
                        "reasoning": f"agent_unavailable: {type(e).__name__}: {str(e)[:200]}"}

    act = decision.get("decision")
    conf = float(decision.get("confidence") or 0.0)
    reasoning = (decision.get("reasoning") or "")[:4000]
    fixes = decision.get("fixes") or []
    print(f"  decision={act} confidence={conf}")
    print(f"  reasoning: {reasoning[:300]}")
    for fx in fixes:
        print(f"  fix: {fx}")

    sig_sum = {"signature": sig, "n_rows": n, "flagged": flagged,
               "rules": user["rules"], "decision": act, "confidence": conf,
               "reasoning": reasoning, "fixes": fixes, "examples": [],
               "cross_table_probe": probe_note}

    if act == "fix" and fixes:
        fix_confidences.append(conf)
        fixed = grp
        try:
            col_exprs = build_column_exprs(fixes)
        except Exception as e:
            print(f"  transform build failed ({e}); escalating this signature")
            fix_confidences.pop()
            sig_sum.update(decision="escalate", confidence=0.0, reasoning=f"bad_transform: {e}", fixes=[])
            sig_summaries.append(sig_sum)
            for ro in grp.select("row_id").collect():
                results.append((ro["row_id"], "escalate", None, 0.0, f"bad_transform: {e}"))
            continue

        fix_cols = list(col_exprs)
        for col, expr in col_exprs.items():
            fixed = fixed.withColumn("__fx__" + col, expr)

        if not fix_cols:
            print("  no valid flagged column in fixes; escalating this signature")
            fix_confidences.pop()
            sig_sum.update(decision="escalate", confidence=0.0,
                           reasoning="no valid flagged column in fixes: " + reasoning)
            sig_summaries.append(sig_sum)
            for ro in grp.select("row_id").collect():
                results.append((ro["row_id"], "escalate", None, 0.0, sig_sum["reasoning"]))
            continue

        # A transform that changes nothing is not a fix — the value would just
        # fail re-validation again. Downgrade to escalate.
        from functools import reduce as _reduce
        changed_pred = _reduce(
            lambda a, b: a | b,
            [~F.col("__fx__" + c).eqNullSafe(F.col(c).cast("string")) for c in fix_cols],
        )
        n_changed = fixed.filter(changed_pred).count()
        if n_changed == 0:
            reasoning = "proposed transform was a no-op (before == after): " + reasoning
            print(f"  -> no-op transform, escalating {n} rows")
            sig_sum.update(decision="escalate", confidence=0.0, reasoning=reasoning)
            fix_confidences.pop()
            sig_summaries.append(sig_sum)
            for ro in grp.select("row_id").collect():
                results.append((ro["row_id"], "escalate", None, 0.0, reasoning))
            continue

        outs = fixed.select("row_id", *[F.col("__fx__" + c).alias(c) for c in fix_cols]).collect()
        after_by_id = {_to_dict(ro)["row_id"]: _to_dict(ro) for ro in outs}
        for r in sample[:5]:
            d = _to_dict(r)
            a = after_by_id.get(d["row_id"], {})
            ex = {c: {"before": _jsonable(d.get(c)), "after": a.get(c)} for c in fix_cols}
            sig_sum["examples"].append({"row_id": d["row_id"], **ex})
            print("  eg " + d["row_id"] + ": " +
                  "; ".join(f"{c}: {d.get(c)!r} -> {a.get(c)!r}" for c in fix_cols))
        for ro in outs:
            d = _to_dict(ro)
            proposed = {c: d.get(c) for c in fix_cols if d.get(c) is not None}
            results.append((d["row_id"], "fix", proposed or None, conf, reasoning))
    else:
        for ro in grp.select("row_id").collect():
            results.append((ro["row_id"], act if act in ("reject", "escalate") else "escalate",
                            None, conf, reasoning))

    sig_summaries.append(sig_sum)

min_batch_confidence = min(fix_confidences) if fix_confidences else 1.0
print(f"\n{len(results)} row decisions; min fix-confidence = {min_batch_confidence}")

# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, MapType

schema = StructType([
    StructField("row_id", StringType(), False),
    StructField("decision", StringType(), True),
    StructField("proposed_fix", MapType(StringType(), StringType()), True),
    StructField("confidence", DoubleType(), True),
    StructField("reasoning", StringType(), True),
])
decisions_df = spark.createDataFrame(
    [(rid, dec, pf, cf, rs) for (rid, dec, pf, cf, rs) in results], schema=schema
)
decisions_df.createOrReplaceTempView("agent_decisions")

spark.sql(f"""
    MERGE INTO {review_queue_fqn} AS target
    USING agent_decisions AS source
    ON target.row_id = source.row_id
       AND target.quarantine_fqn = '{quarantine_fqn}'
       AND target.status = 'pending'
    WHEN MATCHED THEN UPDATE SET
        target.remediation_source = 'agent',
        target.agent_decision = source.decision,
        target.remediation_action = CASE WHEN source.decision = 'fix' THEN 'patch_fields' ELSE NULL END,
        target.proposed_fix = source.proposed_fix,
        target.confidence = source.confidence,
        target.status = CASE
            WHEN source.decision = 'fix' AND source.confidence >= {confidence_threshold} THEN 'curated'
            WHEN source.decision = 'reject' THEN 'rejected'
            ELSE 'escalated'
        END,
        target.updated_at = current_timestamp()
""")

# COMMAND ----------
summary = {
    "quarantine_fqn": quarantine_fqn,
    "pending_rows": n_pending,
    "row_decisions": len(results),
    "min_fix_confidence": min_batch_confidence,
    "signatures": sig_summaries,
}
print("\n=== SUMMARY ===")
print(json.dumps(summary, indent=2, default=str))

dbutils.jobs.taskValues.set(key="min_batch_confidence", value=min_batch_confidence)
dbutils.jobs.taskValues.set(key="agent_summary", value=json.dumps(summary, default=str)[:9000])
dbutils.notebook.exit(json.dumps(summary, default=str))
