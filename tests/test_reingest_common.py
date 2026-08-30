"""resolve_reingest_target — shared by 04 and 07, decides if a quarantine
table's rows can be MERGEd back into Silver."""
import importlib.util
import pathlib

import pytest

_MOD = pathlib.Path(__file__).resolve().parents[1] / "notebooks" / "lib" / "reingest_common.py"
_spec = importlib.util.spec_from_file_location("reingest_common", _MOD)
reingest_common = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(reingest_common)
resolve_reingest_target = reingest_common.resolve_reingest_target


def _meta(data_source, row_id_keys, is_cross_table=False):
    return [{"_data_source": data_source, "_row_id_keys": row_id_keys,
             "_is_cross_table": is_cross_table}]


def test_happy_single_source_single_key():
    ok, silver, keys, reason = resolve_reingest_target(
        _meta("main.silver.orders", "order_id")
    )
    assert ok is True
    assert silver == "main.silver.orders"
    assert keys == ["order_id"]
    assert reason is None


def test_composite_key_split():
    ok, silver, keys, reason = resolve_reingest_target(
        _meta("main.silver.orders", "customer_id,order_id")
    )
    assert ok and keys == ["customer_id", "order_id"]


def test_cross_table_not_reingestable():
    ok, silver, keys, reason = resolve_reingest_target(
        _meta("main.silver.a,main.silver.b", "id", is_cross_table=True)
    )
    assert ok is False
    assert reason == "cross_table_quarantine"


def test_empty_row_id_keys_not_reingestable():
    ok, _, keys, reason = resolve_reingest_target(_meta("main.silver.orders", ""))
    assert ok is False
    assert keys == []
    assert reason == "no_business_key_for_merge"


def test_multiple_data_sources_ambiguous():
    rows = _meta("main.silver.orders", "order_id") + _meta("main.silver.other", "order_id")
    ok, silver, _, reason = resolve_reingest_target(rows)
    assert ok is False
    assert silver is None
    assert reason == "ambiguous_data_source"


def test_no_data_source():
    ok, silver, _, reason = resolve_reingest_target(_meta(None, "order_id"))
    assert ok is False
    assert reason == "no_data_source"


def test_mixed_row_id_keys_across_passes_is_rejected():
    rows = _meta("main.silver.orders", "order_id") + _meta("main.silver.orders", "id2")
    ok, _, _, reason = resolve_reingest_target(rows)
    assert ok is False
    assert reason == "no_business_key_for_merge"


def test_deduplicates_identical_meta_rows():
    rows = _meta("main.silver.orders", "order_id") * 5
    ok, silver, keys, reason = resolve_reingest_target(rows)
    assert ok and silver == "main.silver.orders" and keys == ["order_id"]
