"""_resolve_quarantine_table_name — pure FQN routing."""
import re

import pytest


@pytest.fixture
def resolve(make_validator):
    return make_validator()._resolve_quarantine_table_name


def test_single_source(resolve):
    assert resolve("main.silver.orders") == "main.quarantine.silver_orders"
    assert resolve("other_cat.bronze.events") == "other_cat.quarantine.bronze_events"


def test_cross_table_same_schema(resolve):
    out = resolve(["main.silver.accounts", "main.silver.leads"])
    assert out == "main.quarantine.silver_cross_table__accounts__leads"


def test_cross_table_different_schemas(resolve):
    out = resolve(["main.bronze.a", "main.silver.b"])
    assert out == "main.quarantine.cross_table__bronze_a__silver_b"


def test_source_order_does_not_matter(resolve):
    a = resolve(["main.silver.leads", "main.silver.accounts"])
    b = resolve(["main.silver.accounts", "main.silver.leads"])
    assert a == b


def test_cross_catalog_rejected(resolve):
    with pytest.raises(ValueError, match="one catalog"):
        resolve(["cat_a.s.t", "cat_b.s.t"])


@pytest.mark.parametrize("bad", ["a.b", "a", "a..c", "a.b.c.d", "  ", ""])
def test_malformed_fqn_rejected(resolve, bad):
    with pytest.raises(ValueError):
        resolve(bad)


def test_empty_input_rejected(resolve):
    with pytest.raises(ValueError, match="at least one source"):
        resolve([])


def test_quarantine_override_pins_catalog_and_schema(make_validator):
    v = make_validator()
    v._quarantine_override = "dq_cat.dq_zone"
    assert v._resolve_quarantine_table_name("main.silver.orders") == "dq_cat.dq_zone.silver_orders"


def test_bad_override_rejected(make_validator):
    v = make_validator()
    v._quarantine_override = "only_catalog"
    with pytest.raises(ValueError, match="quarantine_table must be"):
        v._resolve_quarantine_table_name("main.silver.orders")


def test_very_long_name_is_hashed(resolve):
    sources = [f"main.silver.table_{i:03d}_with_a_longish_name" for i in range(30)]
    out = resolve(sources)
    name = out.rsplit(".", 1)[1]
    assert len(name) <= 200
    assert re.match(r"^silver_cross_table__[0-9a-f]{8}$", name)
