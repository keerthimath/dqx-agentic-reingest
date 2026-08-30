"""
Registry of deterministic remediation strategies referenced by
config/remediation_playbook.yaml. Each strategy takes the candidate rows
for one violation-signature batch plus its `params` dict, and returns a
DataFrame with (at minimum) columns:

    row_id                 — matches the quarantine table's row_id
    remediation_action     — 'patch_fields' | 'keep' | 'drop' | 'reject'
    action_payload         — map<string,string>:
                                patch_fields -> {"<col>": "<new_value>", ...}
                                drop         -> {"reason": "..."}  (dedup loser -> resolved_duplicate)
                                reject       -> {"reason": "..."}  (bad row     -> rejected)
                                keep         -> {} (row passes through as-is)

Import this module from 03a_playbook_remediate.py (add it to the notebook's
sys.path, or package it as a workspace file / repo module — whichever your
existing IID system's notebook layout already uses).
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def dedupe_keep_latest(df: DataFrame, params: dict) -> DataFrame:
    """Row-number over partition_by columns, keep the row with the max
    order_by value per partition, mark the rest as resolved duplicates.

    Caveat: PK/uniqueness checks are usually dataset-level, not row-level.
    This only dedupes *within the candidate quarantine batch*. If the
    surviving row's key can also collide with something already in Silver,
    add a lookup against the target table before finalizing `keep` — not
    included here to keep this a sketch, but flag it before relying on this
    in production.
    """
    partition_cols = params["partition_by"]
    # Columns in `ci_columns` are compared case-insensitively for the purpose of
    # grouping duplicates (e.g. "Open" == "open" == "OPEN").
    ci_columns = set(params.get("ci_columns") or [])
    order_col = params["order_by"]
    descending = params.get("order_direction", "desc").lower() == "desc"

    order_expr = F.col(order_col).desc() if descending else F.col(order_col).asc()
    partition_exprs = [
        F.lower(F.col(c)) if c in ci_columns else F.col(c) for c in partition_cols
    ]
    w = Window.partitionBy(*partition_exprs).orderBy(order_expr)

    ranked = df.withColumn("_rn", F.row_number().over(w))

    kept = ranked.filter(F.col("_rn") == 1).withColumn(
        "remediation_action", F.lit("keep")
    ).withColumn("action_payload", F.create_map())

    dropped = ranked.filter(F.col("_rn") > 1).withColumn(
        "remediation_action", F.lit("drop")
    ).withColumn(
        "action_payload",
        F.create_map(F.lit("reason"), F.lit("duplicate_key_superseded_by_latest")),
    )

    return kept.unionByName(dropped, allowMissingColumns=True).drop("_rn")


def standardize_value(df: DataFrame, params: dict) -> DataFrame:
    """Apply a named transform to a single column and propose it as a patch."""
    column = params["column"]
    transform = params["transform"]

    transforms = {
        "lower_trim": lambda c: F.trim(F.lower(c)),
        "trim": lambda c: F.trim(c),
        "upper_trim": lambda c: F.trim(F.upper(c)),
    }
    if transform not in transforms:
        raise ValueError(f"Unknown transform '{transform}' for standardize_value")

    new_val = transforms[transform](F.col(column))

    return (
        df.withColumn("_new_val", new_val)
        .withColumn("remediation_action", F.lit("patch_fields"))
        .withColumn("action_payload", F.create_map(F.lit(column), F.col("_new_val")))
        .drop("_new_val")
    )


def fill_default(df: DataFrame, params: dict) -> DataFrame:
    """Fill a null/missing column with a fixed default expression."""
    column = params["column"]
    default_expr = params["default_expr"]

    return (
        df.withColumn("_default", F.expr(default_expr).cast("string"))
        .withColumn("remediation_action", F.lit("patch_fields"))
        .withColumn("action_payload", F.create_map(F.lit(column), F.col("_default")))
        .drop("_default")
    )


def reject_rows(df: DataFrame, params: dict) -> DataFrame:
    """Mark every row as rejected (dropped, terminal). For rule owners who know
    a class of violation is unrecoverable — the row never re-validates, never
    reaches the agent, never reingests. Scope with the playbook entry's `where`.
    """
    reason = params.get("reason", "rejected_by_playbook")
    return (
        df.withColumn("remediation_action", F.lit("reject"))
        .withColumn("action_payload", F.create_map(F.lit("reason"), F.lit(reason)))
    )


STRATEGY_REGISTRY = {
    "dedupe_keep_latest": dedupe_keep_latest,
    "standardize_value": standardize_value,
    "fill_default": fill_default,
    "reject_rows": reject_rows,
}
