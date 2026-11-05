# -*- coding: utf-8 -*-
"""Tests for the registry client.

These tests mock the HTTP layer so they never depend on live network access to
registry.apicrafter.io (previously a source of flaky/offline CI failures).
"""
import pytest
from unittest.mock import patch, MagicMock

from metacrafter.registry.client import RegistryClient


SAMPLE_REGISTRY = {
    "year": {"id": "year", "name": "Year", "is_pii": False},
    "month": {"id": "month", "name": "Month", "is_pii": False},
    "url": {"id": "url", "name": "URL", "is_pii": False},
    "birthday": {"id": "birthday", "name": "Birthday", "is_pii": True},
    "inn": {"id": "inn", "name": "INN", "is_pii": False},
}


def _mock_response(json_data, status_code=200):
    resp = MagicMock()
    resp.json.return_value = json_data
    resp.status_code = status_code
    return resp


@pytest.fixture
def mock_requests():
    """Patch requests.get in the client module with registry-aware responses."""
    with patch("metacrafter.registry.client.requests") as mock_req:
        def _get(url, timeout=None):
            if url.endswith("/registry.json"):
                return _mock_response(SAMPLE_REGISTRY)
            # /datatype/<id>.json
            datatype_id = url.rsplit("/", 1)[-1].replace(".json", "")
            if datatype_id in SAMPLE_REGISTRY:
                return _mock_response(SAMPLE_REGISTRY[datatype_id])
            return _mock_response({}, status_code=404)

        mock_req.get.side_effect = _get
        yield mock_req


class TestRegistry:
    def test_registry_connect(self, mock_requests):
        client = RegistryClient()
        assert client is not None

    def test_registry_preload_init(self, mock_requests):
        client = RegistryClient(preload=True)
        assert client.cached is not None

    def test_registry_not_preload_init(self, mock_requests):
        client = RegistryClient(preload=False)
        assert client.cached is None

    def test_registry_preload(self, mock_requests):
        client = RegistryClient(preload=False)
        assert client.cached is None
        client.preload()
        assert client.cached is not None

    def test_registry_list(self, mock_requests):
        client = RegistryClient(preload=False)
        assert len(client.getlist()) > 0

    def test_registry_has(self, mock_requests):
        client = RegistryClient(preload=True)
        assert client.has("year")
        assert client.has("month")
        assert not client.has("notexists")

    def test_registry_get(self, mock_requests):
        client = RegistryClient(preload=True)
        assert client.get("year")["name"] == "Year"
        assert client.get("url")["id"] == "url"
        assert client.get("birthday")["is_pii"] is True
        assert client.get("inn")["is_pii"] is False

    def test_registry_get_error(self, mock_requests):
        client = RegistryClient(preload=True)
        with pytest.raises(KeyError):
            client.get("notexists")


class TestRegistryUrlConfig:
    def test_default_url(self, monkeypatch):
        monkeypatch.delenv("METACRAFTER_REGISTRY_URL", raising=False)
        assert RegistryClient().connstr == "https://registry.apicrafter.io"

    def test_env_override(self, monkeypatch):
        monkeypatch.setenv("METACRAFTER_REGISTRY_URL", "http://localhost:8089/")
        # trailing slash normalized away
        assert RegistryClient().connstr == "http://localhost:8089"

    def test_explicit_arg_takes_precedence(self, monkeypatch):
        monkeypatch.setenv("METACRAFTER_REGISTRY_URL", "http://localhost:8089")
        assert RegistryClient("http://example.com/reg/").connstr == "http://example.com/reg"
