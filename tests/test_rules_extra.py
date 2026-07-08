"""Tests for extended rules auto-discovery and rulepath composition."""

import os
from unittest.mock import patch

import pytest

from metacrafter.config import (
    ConfigLoader,
    DEFAULT_RULEPATH,
    MetacrafterConfig,
    build_rulepath,
    discover_extended_rules_path,
)


def test_discover_extended_rules_path_not_installed():
    import builtins

    real_import = builtins.__import__

    def selective_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name == "metacrafterext.rules":
            raise ImportError("no metacrafter-rules")
        return real_import(name, globals, locals, fromlist, level)

    with patch("builtins.__import__", side_effect=selective_import):
        assert discover_extended_rules_path() is None


def test_build_rulepath_without_extended():
    with patch("metacrafter.config.discover_extended_rules_path", return_value=None):
        assert build_rulepath() == list(DEFAULT_RULEPATH)


def test_build_rulepath_with_extended(tmp_path):
    extended = tmp_path / "extended_rules"
    extended.mkdir()
    (extended / "sample.yaml").write_text("rules: {}\n", encoding="utf-8")

    with patch("metacrafter.config.discover_extended_rules_path", return_value=str(extended)):
        paths = build_rulepath()
    assert paths[0] == "rules"
    assert paths[1] == str(extended)


def test_build_rulepath_auto_rules_disabled():
    custom = ["/tmp/custom-rules"]
    with patch("metacrafter.config.discover_extended_rules_path") as discover:
        paths = build_rulepath(user_paths=custom, auto_rules=False)
    discover.assert_not_called()
    assert paths == custom


def test_build_rulepath_appends_cli_paths(tmp_path):
    extended = tmp_path / "extended_rules"
    extended.mkdir()
    cli_dir = tmp_path / "cli_rules"
    cli_dir.mkdir()

    with patch("metacrafter.config.discover_extended_rules_path", return_value=str(extended)):
        paths = build_rulepath(user_paths=[str(cli_dir)], auto_rules=True)

    assert paths == ["rules", str(extended), str(cli_dir)]


def test_get_rulepath_without_config(monkeypatch):
    monkeypatch.setattr(ConfigLoader, "load_config", staticmethod(lambda: None))
    with patch("metacrafter.config.discover_extended_rules_path", return_value=None):
        assert ConfigLoader.get_rulepath() == list(DEFAULT_RULEPATH)


def test_get_rulepath_auto_rules_false_from_config(monkeypatch, tmp_path):
    rule_dir = tmp_path / "only_custom"
    rule_dir.mkdir()
    monkeypatch.setattr(
        ConfigLoader,
        "load_config",
        staticmethod(
            lambda: {
                "auto_rules": False,
                "rulepath": [str(rule_dir)],
            }
        ),
    )
    with patch("metacrafter.config.discover_extended_rules_path") as discover:
        paths = ConfigLoader.get_rulepath()
    discover.assert_not_called()
    assert paths == [str(rule_dir)]


def test_get_rulepath_composes_cli_override(monkeypatch, tmp_path):
    cli_dir = tmp_path / "cli_rules"
    cli_dir.mkdir()
    monkeypatch.setattr(ConfigLoader, "load_config", staticmethod(lambda: None))
    with patch("metacrafter.config.discover_extended_rules_path", return_value=None):
        paths = ConfigLoader.get_rulepath(extra_paths=[str(cli_dir)])
    assert paths == ["rules", str(cli_dir)]


def test_metacrafter_config_auto_rules_default():
    config = MetacrafterConfig(rulepath=["rules"])
    assert config.auto_rules is True


def test_duplicate_rule_keys_do_not_crash(tmp_path, caplog):
    """Loading built-in and extended paths with duplicate rulekeys only warns."""
    from metacrafter.classify.processor import RulesProcessor

    builtin = tmp_path / "builtin"
    extended = tmp_path / "extended"
    builtin.mkdir()
    extended.mkdir()

    rule_yaml = """
name: test
description: test
context: common
lang: common
rules:
  shared_rule:
    key: shared_datatype
    name: Shared
    match: text
    type: field
    rule: foo
"""
    (builtin / "a.yaml").write_text(rule_yaml, encoding="utf-8")
    (extended / "b.yaml").write_text(rule_yaml, encoding="utf-8")

    processor = RulesProcessor()
    processor.import_rules_path(str(builtin), recursive=True)
    processor.import_rules_path(str(extended), recursive=True)
    assert processor._RulesProcessor__rule_keys == ["shared_rule"]
    assert any("Duplicate rulekey 'shared_rule'" in record.message for record in caplog.records)


@pytest.mark.skipif(
    discover_extended_rules_path() is None,
    reason="metacrafter-rules not installed",
)
def test_discover_extended_rules_path_when_installed():
    path = discover_extended_rules_path()
    assert path is not None
    assert os.path.isdir(path)
    assert any(name.endswith(".yaml") for _, _, files in os.walk(path) for name in files)
