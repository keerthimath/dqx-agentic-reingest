"""Table configuration for the DQX validator.

All catalog / schema / table names are resolved per-env from ``tables.yaml``
(bundled in the wheel). Nothing is hard-coded here and there are no
environment defaults: callers must pass ``env`` explicitly, typically
sourced from a Databricks job or notebook parameter.
"""
from functools import lru_cache
from importlib import resources

import yaml

_CONFIG_FILE = "tables.yaml"


@lru_cache(maxsize=1)
def _cfg() -> dict:
    """Load and cache tables.yaml from inside the package."""
    text = resources.files(__package__).joinpath(_CONFIG_FILE).read_text(encoding="utf-8")
    cfg = yaml.safe_load(text)
    if not isinstance(cfg, dict):
        raise ValueError(f"{_CONFIG_FILE} must contain a mapping at the top level")
    return cfg


def normalize_env(env: str) -> str:
    """Validate and normalize an env string (e.g. 'dev', 'prod')."""
    if not isinstance(env, str) or not env.strip():
        raise ValueError("env must be a non-empty string, e.g. 'dev' or 'prod'")
    return env.strip().lower()


def get_catalog(env: str) -> str:
    """Return the catalog for a given env by filling {env} into catalog_template."""
    return _cfg()["catalog_template"].format(env=normalize_env(env))


def get_rules_schema(env: str) -> str:
    """Return the rules schema for a given env (the 'prod' vs 'non_prod' entry in tables.yaml)."""
    schemas = _cfg()["rules_schema"]
    return schemas["prod"] if normalize_env(env) == "prod" else schemas["non_prod"]


def get_rules_table(env: str) -> str:
    """Return the env-specific rules table FQN."""
    return f"{get_catalog(env)}.{get_rules_schema(env)}.{_cfg()['table_names']['rules']}"


def get_universal_rules_table(env: str) -> str:
    """Return the env-specific universal rules table FQN."""
    return (
        f"{get_catalog(env)}.{get_rules_schema(env)}."
        f"{_cfg()['table_names']['universal_rules']}"
    )


def get_quarantine_schema() -> str:
    """Return the schema quarantine tables live in (same for every env)."""
    return _cfg()["quarantine_schema"]


def get_quarantine_table(env: str) -> str:
    """Return the env-specific quarantine table prefix (<catalog>.<schema>)."""
    return f"{get_catalog(env)}.{get_quarantine_schema()}"
