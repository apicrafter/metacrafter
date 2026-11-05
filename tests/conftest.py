"""Keep tests independent of a developer's home configuration."""

import pytest


@pytest.fixture(autouse=True)
def isolated_home(monkeypatch, tmp_path):
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.delenv("METACRAFTER_REGISTRY_URL", raising=False)
    monkeypatch.delenv("METACRAFTER_API_TOKEN", raising=False)


@pytest.fixture(autouse=True)
def no_external_network(monkeypatch):
    import socket

    def deny_network(*args, **kwargs):
        raise AssertionError("Tests must mock external network clients")

    monkeypatch.setattr(socket.socket, "connect", deny_network)
