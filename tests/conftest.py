"""Keep tests independent of a developer's home configuration."""

from pathlib import Path

import pytest


REGISTRY_SNAPSHOT = Path(__file__).parent / "fixtures" / "registry_snapshot"


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
