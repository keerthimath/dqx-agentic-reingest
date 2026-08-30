"""Small pure helpers shared by 04_apply_and_validate and 07_dry_run_report.

Kept free of pyspark so they can be unit-tested without a SparkSession — the
notebooks pass in already-collected rows.
"""


def resolve_reingest_target(meta_rows):
    """
    Decide whether a quarantine table's rows can be MERGEd back into Silver.

    ``meta_rows`` is an iterable of mappings (typically ``df.select(
    "_data_source", "_row_id_keys", "_is_cross_table").distinct().collect()``).

    Returns ``(reingestable, silver_fqn, merge_key_cols, reason)``:
      - reingestable: bool
      - silver_fqn: the single ``_data_source`` FQN, else None
      - merge_key_cols: list[str] parsed from ``_row_id_keys``
      - reason: None when reingestable, else why not
        ('cross_table_quarantine' | 'ambiguous_data_source' |
         'no_business_key_for_merge' | 'no_data_source')
    """
    rows = list(meta_rows)
    data_sources = sorted({r["_data_source"] for r in rows if r.get("_data_source")})
    row_id_keys = sorted({
        r["_row_id_keys"] for r in rows if r.get("_row_id_keys") is not None
    })
    is_cross_table = any(bool(r.get("_is_cross_table")) for r in rows)

    silver_fqn = data_sources[0] if len(data_sources) == 1 else None
    merge_key_cols = []
    if row_id_keys and row_id_keys != [""]:
        merge_key_cols = [c.strip() for c in row_id_keys[0].split(",") if c.strip()]

    if is_cross_table:
        reason = "cross_table_quarantine"
    elif len(data_sources) == 0:
        reason = "no_data_source"
    elif len(data_sources) != 1:
        reason = "ambiguous_data_source"
    elif len(row_id_keys) != 1 or row_id_keys == [""] or not merge_key_cols:
        reason = "no_business_key_for_merge"
    else:
        reason = None

    return (reason is None), silver_fqn, merge_key_cols, reason
