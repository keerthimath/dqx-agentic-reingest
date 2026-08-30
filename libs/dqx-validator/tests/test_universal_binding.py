"""Universal-rule helpers: column-name normalization, placeholder binding,
plan-text FQN extraction."""
import pytest


@pytest.fixture
def v(make_validator):
    return make_validator()


# --------------------------------------------------------------------------- #
# _normalize_column_names
# --------------------------------------------------------------------------- #
def test_normalize_single_string(v):
    assert v._normalize_column_names("email") == ["email"]


def test_normalize_dedupes_and_keeps_order(v):
    assert v._normalize_column_names(["a", "b", "a", " c "]) == ["a", "b", "c"]


def test_normalize_empty_raises(v):
    with pytest.raises(ValueError):
        v._normalize_column_names([" ", ""])


def test_normalize_wrong_type_raises(v):
    with pytest.raises(TypeError):
        v._normalize_column_names(123)


# --------------------------------------------------------------------------- #
# _bind_universal_check_to_columns
# --------------------------------------------------------------------------- #
def test_bind_scalar_placeholders(v):
    tmpl = {"function": "is_not_null", "column": "{{column}}"}
    assert v._bind_universal_check_to_columns(tmpl, "email", ["email", "name"])["column"] == "email"


def test_bind_all_tokens_map_to_column(v):
    for token in ("{{column}}", "${column}", "__COLUMN__"):
        out = v._bind_universal_check_to_columns({"column": token}, "email", ["email"])
        assert out["column"] == "email"


def test_bind_columns_token_expands_to_list(v):
    tmpl = {"arguments": {"cols": "{{columns}}"}}
    out = v._bind_universal_check_to_columns(tmpl, "email", ["email", "name"])
    assert out["arguments"]["cols"] == ["email", "name"]


def test_bind_columns_token_inside_string_is_csv(v):
    tmpl = {"msg": "compare {{columns}} now"}
    out = v._bind_universal_check_to_columns(tmpl, "email", ["email", "name"])
    assert out["msg"] == "compare email,name now"


def test_bind_defaults_placeholder_column_key(v):
    for placeholder in (None, "", "*", "column"):
        out = v._bind_universal_check_to_columns({"column": placeholder}, "email", ["email"])
        assert out["column"] == "email"


def test_bind_does_not_mutate_template(v):
    tmpl = {"column": "{{column}}"}
    v._bind_universal_check_to_columns(tmpl, "email", ["email"])
    assert tmpl == {"column": "{{column}}"}


# --------------------------------------------------------------------------- #
# _extract_table_fqns_from_text
# --------------------------------------------------------------------------- #
def test_extract_fqns_from_plan_text(v):
    text = "Scan main.silver.orders join main.silver.customers"
    assert v._extract_table_fqns_from_text(text) == [
        "main.silver.orders",
        "main.silver.customers",
    ]


def test_extract_excludes_rules_and_information_schema(v):
    text = (
        "read main.dqx_app.dq_quality_rules and "
        "main.information_schema.tables and main.silver.orders"
    )
    assert v._extract_table_fqns_from_text(text) == ["main.silver.orders"]


def test_extract_dedupes(v):
    text = "main.silver.orders main.silver.orders"
    assert v._extract_table_fqns_from_text(text) == ["main.silver.orders"]
