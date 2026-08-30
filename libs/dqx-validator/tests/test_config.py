"""tables.yaml resolution — the per-env catalog / rules / quarantine names."""
import pytest

from dqx_validator import config


@pytest.mark.parametrize("raw,expected", [("dev", "dev"), (" PROD ", "prod"), ("Dev", "dev")])
def test_normalize_env(raw, expected):
    assert config.normalize_env(raw) == expected


@pytest.mark.parametrize("bad", ["", "   ", None, 5])
def test_normalize_env_rejects_empty(bad):
    with pytest.raises(ValueError):
        config.normalize_env(bad)


def test_catalog_template():
    assert config.get_catalog("dev") == "main"
    assert config.get_catalog("prod") == "main"


def test_rules_table_is_env_specific():
    # non-prod envs use the dqx_app schema, prod uses dqx_studio (the catalog
    # comes from tables.yaml's catalog_template — "main" in the shipped default)
    assert config.get_rules_table("dev") == "main.dqx_app.dq_quality_rules"
    assert config.get_rules_table("staging") == "main.dqx_app.dq_quality_rules"
    assert config.get_rules_table("prod") == "main.dqx_studio.dq_quality_rules"


def test_universal_rules_table():
    assert config.get_universal_rules_table("dev").endswith(
        ".dqx_app.dq_universal_quality_rules"
    )
    assert config.get_universal_rules_table("prod").startswith("main.dqx_studio.")


def test_quarantine_names():
    assert config.get_quarantine_schema() == "quarantine"
    assert config.get_quarantine_table("dev") == "main.quarantine"
    assert config.get_quarantine_table("prod") == "main.quarantine"
