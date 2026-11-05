"""CLI: metacrafter rules check --registry."""

import json
from pathlib import Path

from typer.testing import CliRunner

from metacrafter.cli import commands  # noqa: F401 - registers commands
from metacrafter.core import app

SNAPSHOT = Path(__file__).parent / "fixtures" / "registry_snapshot"


def _rules(tmp_path):
    rules = tmp_path / "rules"
    rules.mkdir()
    (rules / "r.yaml").write_text(
        """
name: t
description: t
context: common
lang: common
rules:
  emailbyname:
    key: email
    name: Email
    match: text
    type: field
    rule: email
  ssnbyname:
    key: ssn
    name: SSN
    match: text
    type: field
    rule: ssn
  oldbyname:
    key: legacy_contact
    name: Legacy
    match: text
    type: field
    rule: legacycontact
  unknownbyname:
    key: foo_bar
    name: Unknown
    match: text
    type: field
    rule: foobar
""",
        encoding="utf-8",
    )
    (tmp_path / ".metacrafter").write_text(f"auto_rules: false\nrulepath: [{rules}]\n", encoding="utf-8")
    return rules


def test_rules_check_reports_unknown_alias_and_deprecated(tmp_path, monkeypatch):
    _rules(tmp_path)
    monkeypatch.chdir(tmp_path)
    result = CliRunner().invoke(
        app, ["rules", "check", "--registry-path", str(SNAPSHOT), "--output-format", "json"]
    )
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output[result.output.index("{"):])
    assert [e["key"] for e in payload["unknown"]] == ["foo_bar"]
    assert payload["alias"][0]["canonical"] == "usssn"
    assert payload["deprecated"][0]["replaced_by"] == "email"


def test_rules_check_strict_fails_on_unknown(tmp_path, monkeypatch):
    _rules(tmp_path)
    monkeypatch.chdir(tmp_path)
    result = CliRunner().invoke(app, ["rules", "check", "--registry-path", str(SNAPSHOT), "--strict"])
    assert result.exit_code == 1
    assert "foo_bar" in result.output
