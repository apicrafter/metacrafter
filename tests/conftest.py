"""Keep tests independent of a developer's home configuration."""

import os
from pathlib import Path

import pytest


REGISTRY_SNAPSHOT = Path(__file__).parent / "fixtures" / "registry_snapshot"


def _session_rule_pack():
    """Resolve the default rule pack once, before tests isolate HOME and block the network.

    Uses METACRAFTER_RULES_DIR, an installed metacrafterext, the cached release, or the
    release archive bundled with the package (no network access).
    """
    from metacrafter.rules.pack import RulePackError, resolve_pack

    try:
        pack = resolve_pack()
    except RulePackError:
        return None
    if pack is None:
        return None
    # A release pack root holds rules/ and metacrafterext/; otherwise use the YAML directory.
    return pack.python_path or pack.rules_dir


_RULE_PACK_DIR = None
if not os.environ.get("METACRAFTER_RULES_DIR"):
    _resolved = _session_rule_pack()
    if _resolved is not None:
        _RULE_PACK_DIR = str(_resolved)
        os.environ["METACRAFTER_RULES_DIR"] = _RULE_PACK_DIR


@pytest.fixture(autouse=True)
def isolated_home(monkeypatch, tmp_path):
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / ".cache"))
    monkeypatch.delenv("METACRAFTER_REGISTRY_URL", raising=False)
    monkeypatch.delenv("METACRAFTER_API_TOKEN", raising=False)
    # Resolve datatypes against a small offline registry snapshot.
    monkeypatch.setenv("METACRAFTER_REGISTRY_PATH", str(REGISTRY_SNAPSHOT))
    monkeypatch.setenv("METACRAFTER_REGISTRY_OFFLINE", "1")


@pytest.fixture(autouse=True)
def no_external_network(monkeypatch):
    import socket

    def deny_network(*args, **kwargs):
        raise AssertionError("Tests must mock external network clients")

    monkeypatch.setattr(socket.socket, "connect", deny_network)
