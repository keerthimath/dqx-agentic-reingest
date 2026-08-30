"""Retrieving approved rules: parsing collected rows + the Spark query path."""
import json

import pytest

from dqx_validator.engine import checks_from_rule_rows


def _row(rule_id, check, table_fqn="main.silver.orders"):
    return {"rule_id": rule_id, "table_fqn": table_fqn, "check_json": json.dumps(check)}


# --------------------------------------------------------------------------- #
# Pure parsing / filtering
# --------------------------------------------------------------------------- #
def test_parses_single_and_list_payloads():
    rows = [
        _row("r1", {"check": {"function": "is_not_null"}, "name": "orders_id_nn"}),
        _row("r2", [{"name": "a"}, {"name": "b"}]),
    ]
    checks = checks_from_rule_rows(rows)
    assert checks[0]["name"] == "orders_id_nn"
    assert checks[1] == [{"name": "a"}, {"name": "b"}]


def test_monitor_only_rules_are_excluded():
    rows = [
        _row("enforced", {"name": "enforced_rule"}),
        _row("watch", {"name": "watch_rule", "user_metadata": {"monitor_only": "true"}}),
        _row("watch2", {"name": "w2", "user_metadata": {"monitor_only": "TRUE"}}),
    ]
    checks = checks_from_rule_rows(rows)
    assert [c["name"] for c in checks] == ["enforced_rule"]


def test_empty_payload_raises():
    with pytest.raises(ValueError, match="empty 'check' payload"):
        checks_from_rule_rows([{"rule_id": "r1", "check_json": ""}])


def test_unparseable_payload_raises():
    with pytest.raises(ValueError, match="invalid check payload"):
        checks_from_rule_rows([{"rule_id": "r1", "check_json": "{not json"}])


def test_empty_rows_yield_no_checks():
    assert checks_from_rule_rows([]) == []


# --------------------------------------------------------------------------- #
# approved_checks_for — input validation (no Spark needed)
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("bad", ["", "   ", [], ["  ", ""], None])
def test_approved_checks_for_rejects_empty(make_validator, bad):
    v = make_validator()
    with pytest.raises(ValueError):
        v.approved_checks_for(bad)


# --------------------------------------------------------------------------- #
# approved_checks_for — full path against a recording fake Spark
# --------------------------------------------------------------------------- #
def test_approved_checks_for_reads_and_parses(make_validator):
    rows = [
        _row("r1", {"name": "orders_id_nn"}, "main.silver.orders"),
        _row("r2", {"name": "orders_amt_pos"}, "main.silver.orders"),
        _row("r3", {"name": "watch", "user_metadata": {"monitor_only": "true"}},
             "main.silver.orders"),
    ]
    v = make_validator(rule_rows=rows)

    checks = v.approved_checks_for("main.silver.orders")

    assert sorted(c["name"] for c in checks) == ["orders_amt_pos", "orders_id_nn"]
    # status='approved' + table_fqn membership => two .filter() calls were issued
    assert v.spark.query_calls["filters"] == 2
    assert v.spark.query_calls["selects"] == 1


def test_approved_checks_for_dedupes_fqn_list(make_validator):
    rows = [_row("r1", {"name": "x"})]
    v = make_validator(rule_rows=rows)
    # duplicates + blanks collapse to a single lookup, no crash
    assert v.approved_checks_for(["main.silver.orders", "main.silver.orders", " "])


def test_approved_checks_for_raises_when_nothing_approved(make_validator):
    v = make_validator(rule_rows=[])
    with pytest.raises(ValueError, match="No approved rules found"):
        v.approved_checks_for("main.silver.orders")
