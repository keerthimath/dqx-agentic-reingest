"""_row_id / _row_id_keys column-selection logic."""
import pytest

from dqx_validator.engine import resolve_row_id_keys

AVAILABLE = ["order_id", "customer_id", "email", "amount"]


def test_explicit_single_key():
    cols, keys = resolve_row_id_keys("order_id", AVAILABLE, AVAILABLE)
    assert cols == ["order_id"]
    assert keys == "order_id"


def test_explicit_composite_key_preserves_order():
    cols, keys = resolve_row_id_keys(["customer_id", "order_id"], AVAILABLE, AVAILABLE)
    assert cols == ["customer_id", "order_id"]
    assert keys == "customer_id,order_id"


def test_missing_key_column_raises():
    with pytest.raises(ValueError, match="not present in input_df"):
        resolve_row_id_keys(["order_id", "nope"], AVAILABLE, AVAILABLE)


def test_blank_key_list_raises():
    with pytest.raises(ValueError, match="at least one column name"):
        resolve_row_id_keys(["  ", ""], AVAILABLE, AVAILABLE)


def test_no_key_hashes_scope_and_marks_unreingestable():
    scope = ["a", "b", "c"]
    cols, keys = resolve_row_id_keys(None, scope, scope)
    assert cols == scope
    assert keys == ""  # empty => 04/05 can't build a MERGE key => escalate


def test_scope_and_available_can_differ():
    # invalid_df carries extra _error/_warning cols; scope is the original input
    cols, keys = resolve_row_id_keys(
        "order_id", ["order_id", "email"], ["order_id", "email", "_error", "_warning"]
    )
    assert cols == ["order_id"] and keys == "order_id"
