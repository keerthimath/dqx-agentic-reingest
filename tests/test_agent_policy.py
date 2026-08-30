"""Static checks on config/agent_curation_policy.yaml."""
import ast
import pathlib

import pytest
import yaml

ROOT = pathlib.Path(__file__).resolve().parents[1]
POLICY = ROOT / "config" / "agent_curation_policy.yaml"
NB = ROOT / "notebooks" / "03_agent_curate.py"


@pytest.fixture(scope="module")
def policy():
    return yaml.safe_load(POLICY.read_text())


def test_decision_policy_has_all_three(policy):
    dp = policy["decision_policy"]
    assert set(dp) == {"fix", "reject", "escalate"}
    assert all(isinstance(v, str) and v.strip() for v in dp.values())


def test_allowed_transforms_match_code(policy):
    """Every transform the YAML advertises must be handled by apply_transform."""
    src = NB.read_text()
    fn = next(
        n for n in ast.walk(ast.parse(src))
        if isinstance(n, ast.FunctionDef) and n.name == "apply_transform"
    )
    handled = {
        c.value
        for node in ast.walk(fn)
        for c in ([node.comparators[0]] if isinstance(node, ast.Compare) else [])
        if isinstance(c, ast.Constant) and isinstance(c.value, str)
    }
    advertised = set(policy["allowed_transforms"])
    missing = advertised - handled
    assert not missing, f"policy lists transforms not in build_col_expr: {missing}"


def test_cross_table_probe_entries_well_formed(policy):
    ctp = policy.get("cross_table_probe", {})
    assert 0 < float(ctp["resolve_threshold"]) <= 1
    assert ctp["normalizations"], "need at least one normalization to try"
    for rule, rc in (ctp.get("rules") or {}).items():
        assert rc["compare_column"]
        assert rc["other_table"].count(".") == 2, f"{rule}: other_table must be a 3-part FQN"
        assert isinstance(rc["join_keys"], list) and rc["join_keys"]
