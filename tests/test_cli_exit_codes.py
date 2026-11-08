"""Tests for CLI exit-code behavior (metacrafter/__main__.py)."""
import subprocess
import sys


def _run(args):
    return subprocess.run(
        [sys.executable, "-m", "metacrafter", *args],
        capture_output=True,
        text=True,
    )


def test_help_exits_zero():
    """`--help` should exit successfully."""
    result = _run(["--help"])
    assert result.returncode == 0


def test_invalid_command_exits_nonzero():
    """An unknown command must produce a non-zero exit code for scripting."""
    result = _run(["definitely-not-a-real-command"])
    assert result.returncode != 0


def test_missing_file_exits_nonzero():
    """Scanning a nonexistent file must exit non-zero rather than 0."""
    result = _run(["scan", "file", "/nonexistent/path/does_not_exist.csv"])
    assert result.returncode != 0


FIELD_RULES = """
format_version: 2
name: t
description: t
context: common
lang: common
rules:
  nopriority:
    key: okogu
    match: text
    type: field
    rule: okogu
  firstpriority:
    key: okfs
    match: text
    type: field
    priority: 1
    rule: okfs
  lowpriority:
    key: okopf
    match: text
    type: field
    priority: 3
    rule: okopf
  datarule:
    key: okopf
    match: text
    type: data
    rule: 75104,75203
"""


def test_field_rule_warnings(tmp_path):
    from metacrafter.product_tools import field_rule_warnings

    path = tmp_path / "rules.yaml"
    path.write_text(FIELD_RULES, encoding="utf-8")
    # Only the field rule without priority: an explicit priority 3 is a choice.
    assert [w["ruleid"] for w in field_rule_warnings(path)] == ["nopriority"]
    assert field_rule_warnings(path, confidence=30) == []


def test_rules_lint_strict_fails_on_warnings(tmp_path):
    import json

    path = tmp_path / "rules.yaml"
    path.write_text(FIELD_RULES, encoding="utf-8")
    plain = _run(["rules", "lint", str(path)])
    assert plain.returncode == 0, plain.stderr
    payload = json.loads(plain.stdout[plain.stdout.index("{"):])
    assert payload["passed"] is True
    assert [w["ruleid"] for w in payload["warnings"]] == ["nopriority"]
    assert _run(["rules", "lint", str(path), "--strict"]).returncode == 1
