"""Static checks on remediation playbook YAML — no Spark, no Databricks.

Validates config/remediation_playbook.sample.yaml (the tracked template) and any
per-dataset files staged under config/remediation_playbooks/ (git-ignored; the
real ones live in the Volume).
"""
import ast
import pathlib

import pytest
import yaml

ROOT = pathlib.Path(__file__).resolve().parents[1]
SAMPLE = ROOT / "config" / "remediation_playbook.sample.yaml"
DATASET_DIR = ROOT / "config" / "remediation_playbooks"
STRATEGIES_PY = ROOT / "notebooks" / "lib" / "remediation_strategies.py"

STRATEGY_REQUIRED_PARAMS = {
    "dedupe_keep_latest": {"partition_by", "order_by"},
    "standardize_value": {"column", "transform"},
    "fill_default": {"column", "default_expr"},
    "reject_rows": set(),
}
KNOWN_TRANSFORMS = {"lower_trim", "trim", "upper_trim"}


def _registered_strategy_names():
    tree = ast.parse(STRATEGIES_PY.read_text())
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and any(
            getattr(t, "id", None) == "STRATEGY_REGISTRY" for t in node.targets
        ):
            return {k.value for k in node.value.keys}
    raise AssertionError("STRATEGY_REGISTRY not found")


def _playbook_files():
    files = [SAMPLE]
    if DATASET_DIR.is_dir():
        files += sorted(DATASET_DIR.glob("*.yaml"))
    return files


def _entries(path):
    return yaml.safe_load(path.read_text())["playbooks"]


def test_registry_matches_documented_strategies():
    assert _registered_strategy_names() == set(STRATEGY_REQUIRED_PARAMS)


@pytest.mark.parametrize("path", _playbook_files(), ids=lambda p: p.name)
def test_every_entry_is_well_formed(path):
    registered = _registered_strategy_names()
    seen = set()
    for e in _entries(path):
        assert e.get("rule_name"), f"{path.name}: entry missing rule_name"
        assert e["strategy"] in registered, f"{path.name}: unknown strategy {e['strategy']!r}"
        assert isinstance(e.get("params", {}), dict)
        assert isinstance(e.get("match_fqns", ["*"]), list) and e.get("match_fqns", ["*"])

        missing = STRATEGY_REQUIRED_PARAMS[e["strategy"]] - set(e.get("params", {}))
        assert not missing, f"{path.name}: {e['rule_name']} missing params {missing}"

        if e["strategy"] == "standardize_value":
            assert e["params"]["transform"] in KNOWN_TRANSFORMS

        key = (e["rule_name"], e.get("where"))
        assert key not in seen, f"{path.name}: duplicate entry {key}"
        seen.add(key)


@pytest.mark.parametrize("path", _playbook_files(), ids=lambda p: p.name)
def test_priorities_present_and_unique(path):
    prios = [e.get("priority") for e in _entries(path)]
    assert all(p is not None for p in prios), f"{path.name}: every entry needs a priority"
    assert len(prios) == len(set(prios)), f"{path.name}: duplicate priority"
