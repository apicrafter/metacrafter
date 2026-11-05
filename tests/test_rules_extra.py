"""Tests for extended rules auto-discovery and rulepath composition."""

import os
from unittest.mock import patch

import pytest

from metacrafter.exceptions import ConfigurationError

from metacrafter.config import (
    ConfigLoader,
    DEFAULT_RULEPATH,
    MetacrafterConfig,
    build_rulepath,
    discover_extended_rules_path,
)


def test_discover_extended_rules_path_offline_without_pack(monkeypatch, tmp_path):
    """No rules_dir, no installed package, no cache and offline: no default pack."""
    import metacrafter.rules.pack as pack

    monkeypatch.delenv("METACRAFTER_RULES_DIR", raising=False)
    monkeypatch.setenv("METACRAFTER_RULES_OFFLINE", "1")
    monkeypatch.setattr(pack, "_installed_pack", lambda: None)
    monkeypatch.setattr(ConfigLoader, "load_config", staticmethod(lambda: None))
    assert discover_extended_rules_path() is None


def test_build_rulepath_without_extended():
    with patch("metacrafter.config.discover_extended_rules_path", return_value=None):
        assert build_rulepath() == []


def test_build_rulepath_with_extended(tmp_path):
    extended = tmp_path / "extended_rules"
    extended.mkdir()
    (extended / "sample.yaml").write_text("rules: {}\n", encoding="utf-8")

    with patch("metacrafter.config.discover_extended_rules_path", return_value=str(extended)):
        paths = build_rulepath()
    assert paths == [str(extended)]


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

    assert paths == [str(extended), str(cli_dir)]


def test_get_rulepath_without_config_uses_default_pack(monkeypatch, tmp_path):
    pack = tmp_path / "pack"
    pack.mkdir()
    monkeypatch.setattr(ConfigLoader, "load_config", staticmethod(lambda: None))
    with patch("metacrafter.config.discover_extended_rules_path", return_value=str(pack)):
        assert ConfigLoader.get_rulepath() == [str(pack)]


def test_get_rulepath_fails_when_nothing_resolves(monkeypatch):
    monkeypatch.setattr(ConfigLoader, "load_config", staticmethod(lambda: None))
    with patch("metacrafter.config.discover_extended_rules_path", return_value=None):
        with pytest.raises(ConfigurationError, match="No rule directories"):
            ConfigLoader.get_rulepath()


def test_local_rules_folder_is_not_loaded_implicitly(monkeypatch, tmp_path):
    (tmp_path / "rules").mkdir()
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(ConfigLoader, "load_config", staticmethod(lambda: None))
    pack = tmp_path / "pack"
    pack.mkdir()
    with patch("metacrafter.config.discover_extended_rules_path", return_value=str(pack)):
        paths = ConfigLoader.get_rulepath()
    assert "rules" not in paths
    assert str(tmp_path / "rules") not in paths


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
    assert paths == [str(cli_dir)]


def test_metacrafter_config_auto_rules_default():
    config = MetacrafterConfig()
    assert config.auto_rules is True
    assert config.rulepath == []


def test_later_rule_overrides_earlier_definition(tmp_path, caplog):
    """A later rulepath entry overrides an earlier rule with the same id."""
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

    caplog.set_level("INFO")
    processor = RulesProcessor()
    processor.import_rules_path(str(builtin), recursive=True)
    processor.import_rules_path(str(extended), recursive=True)
    assert processor._RulesProcessor__rule_keys == ["shared_rule"]
    assert len(processor.field_rules) == 1
    assert processor.field_rules[0]["source"].endswith("b.yaml")
    assert any("overrides" in record.message for record in caplog.records)


@pytest.mark.skipif(
    discover_extended_rules_path() is None,
    reason="metacrafter-rules not installed",
)
def test_discover_extended_rules_path_when_installed():
    path = discover_extended_rules_path()
    assert path is not None
    assert os.path.isdir(path)
    assert any(name.endswith(".yaml") for _, _, files in os.walk(path) for name in files)
