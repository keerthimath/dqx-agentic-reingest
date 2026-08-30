"""End-to-end run_validation flow, Spark faked out.

Nothing here touches storage: DQX's split is stubbed, the quarantine write goes
to a recording fake, and the quarantine table is reported as already existing so
no DDL runs. We assert on *what would have been written*, not on any IO.
"""
import json

import pytest

APPROVED_RULE = {
    "rule_id": "r1",
    "table_fqn": "main.silver.orders",
    "check_json": json.dumps({"name": "orders_id_nn", "check": {"function": "is_not_null"}}),
}
INPUT_COLS = ["order_id", "email", "amount"]
AUDIT_COLS = {"_row_id", "_row_id_keys", "_generated_at", "_data_source",
              "_rule_sources", "_is_cross_table"}


def test_flow_quarantines_without_touching_storage(flow):
    v, input_df, valid_df, _ = flow(
        columns=INPUT_COLS, rule_rows=[APPROVED_RULE],
        invalid_empty=False, invalid_count=3,
    )

    result = v.run_validation(input_df, "main.silver.orders", row_id_columns=["order_id"])

    assert result is valid_df                                   # good split returned as-is
    assert v.spark.saved_tables == ["main.quarantine.silver_orders"]   # one routed write
    assert AUDIT_COLS.issubset(set(v.spark.last_saved_columns))        # _row_id + audit cols attached


def test_flow_noop_when_nothing_invalid(flow):
    v, input_df, valid_df, _ = flow(
        columns=INPUT_COLS, rule_rows=[APPROVED_RULE], invalid_empty=True,
    )

    result = v.run_validation(input_df, "main.silver.orders", row_id_columns=["order_id"])

    assert result is valid_df
    assert v.spark.saved_tables == []
    assert v.spark.last_saved_columns is None


def test_flow_passes_loaded_checks_to_dqx(flow):
    v, input_df, _, _ = flow(
        columns=INPUT_COLS, rule_rows=[APPROVED_RULE], invalid_empty=True,
    )
    v.run_validation(input_df, "main.silver.orders", row_id_columns=["order_id"])

    (_df, checks) = v.dq_engine.calls[0]
    assert checks == [{"name": "orders_id_nn", "check": {"function": "is_not_null"}}]


def test_flow_rejects_non_dataframe_input(flow):
    v, *_ = flow(columns=INPUT_COLS, rule_rows=[APPROVED_RULE], invalid_empty=True)
    with pytest.raises(TypeError):
        v.run_validation("not a dataframe", "main.silver.orders")


def test_flow_rejects_empty_rules(flow):
    v, input_df, *_ = flow(
        columns=INPUT_COLS, rule_rows=[APPROVED_RULE], invalid_empty=False, invalid_count=1,
    )
    with pytest.raises(ValueError):
        v.run_validation(input_df, [])


def test_flow_bad_row_id_column_raises_before_write(flow):
    v, input_df, *_ = flow(
        columns=INPUT_COLS, rule_rows=[APPROVED_RULE], invalid_empty=False, invalid_count=1,
    )
    with pytest.raises(ValueError, match="not present in input_df"):
        v.run_validation(input_df, "main.silver.orders", row_id_columns=["does_not_exist"])
    assert v.spark.saved_tables == []


def test_flow_without_row_id_columns_still_runs_but_marks_unkeyed(flow):
    v, input_df, _, _ = flow(
        columns=INPUT_COLS, rule_rows=[APPROVED_RULE], invalid_empty=False, invalid_count=1,
    )
    v.run_validation(input_df, "main.silver.orders")  # no row_id_columns
    assert v.spark.saved_tables == ["main.quarantine.silver_orders"]
    assert "_row_id_keys" in v.spark.last_saved_columns
