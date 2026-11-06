"""Bundled registry snapshot, compressed downloads and the download deadline."""

import gzip
import io
import os
import shutil
import time
from pathlib import Path

import pytest
import yaml

from metacrafter.registry import adapter as adapter_mod
from metacrafter.registry.adapter import RegistryAdapter, clear_adapter_cache, verify_snapshot

SNAPSHOT = Path(__file__).parent / "fixtures" / "registry_snapshot"
LOCK = Path(__file__).parent.parent / "ecosystem.lock"


@pytest.fixture(autouse=True)
def _fresh_adapter_cache():
    clear_adapter_cache()
    yield
    clear_adapter_cache()


def _no_network(url, timeout, deadline=None):
    raise AssertionError(f"network must not be used: {url}")


def _fixture_server():
    files = {name: (SNAPSHOT / name).read_bytes() for name in ("datatypes.jsonl", "aliases.json", "categories.json")}
    files["index.json"] = (SNAPSHOT / "index.json").read_bytes()
    calls = []

    def fake_get(url, timeout, deadline=None):
        calls.append(url)
        return files[url.rsplit("/", 1)[-1]]

    return fake_get, calls


def test_bundled_snapshot_matches_ecosystem_lock():
    lock = yaml.safe_load(LOCK.read_text())["metacrafter-registry"]
    manifest = adapter_mod.bundled_manifest()
    assert manifest is not None, "run scripts/bundle_registry_snapshot.py"
    assert (manifest["version"], manifest["git_sha"]) == (lock["version"], lock["git_sha"]), (
        f"bundled registry {manifest['version']} ({manifest['git_sha']}) is not ecosystem.lock "
        f"{lock['version']} ({lock['git_sha']}); run scripts/bundle_registry_snapshot.py"
    )
    assert manifest["base_url"].rstrip("/") == lock["base_url"].rstrip("/") == adapter_mod.DEFAULT_REGISTRY_URL
    files = adapter_mod.read_bundled_snapshot()  # raises on a hash mismatch
    assert set(files) == {"index.json", "datatypes.jsonl", "aliases.json", "categories.json"}


def test_cold_cache_uses_bundled_snapshot_without_network(tmp_path, monkeypatch):
    monkeypatch.setattr(adapter_mod, "_http_get", _no_network)
    cache = tmp_path / "cache"
    reg = RegistryAdapter.load(cache_dir=str(cache))
    lock = yaml.safe_load(LOCK.read_text())["metacrafter-registry"]
    assert reg.provenance.source.startswith("bundled:")
    assert reg.provenance.version == lock["version"]
    assert reg.resolve("email").canonical_id == "email"
    assert verify_snapshot(cache)
    assert not list(tmp_path.glob("cache.partial*"))

    # The seeded cache counts as checked now: the next load uses it without network.
    again = RegistryAdapter.load(cache_dir=str(cache))
    assert again.provenance.source.startswith("cache:")


def test_offline_cold_cache_uses_bundled_snapshot(tmp_path, monkeypatch):
    monkeypatch.setattr(adapter_mod, "_http_get", _no_network)
    reg = RegistryAdapter.load(offline=True, cache_dir=str(tmp_path / "cache"))
    assert reg.provenance.source.startswith("bundled:")


def test_corrupted_cache_is_replaced_by_bundled_snapshot(tmp_path, monkeypatch):
    monkeypatch.setattr(adapter_mod, "_http_get", _no_network)
    cache = tmp_path / "cache"
    shutil.copytree(SNAPSHOT, cache)
    (cache / "datatypes.jsonl").write_text("{}\n", encoding="utf-8")
    reg = RegistryAdapter.load(cache_dir=str(cache))
    assert reg.provenance.source.startswith("bundled:")


def test_self_hosted_registry_ignores_bundled_snapshot(tmp_path, monkeypatch):
    fake_get, calls = _fixture_server()
    monkeypatch.setattr(adapter_mod, "_http_get", fake_get)
    reg = RegistryAdapter.load(base_url="http://localhost:8089", cache_dir=str(tmp_path / "cache"))
    assert reg.provenance.source.startswith("network:")
    assert reg.provenance.version == "1.3.0.dev0-test"
    assert calls and all(url.startswith("http://localhost:8089/") for url in calls)


def test_broken_bundle_falls_back_to_network(tmp_path, monkeypatch):
    bundle = tmp_path / "bundle"
    shutil.copytree(adapter_mod.BUNDLED_DIR, bundle)
    (bundle / "datatypes.jsonl.gz").write_bytes(gzip.compress(b'{"id": "tampered"}\n'))
    monkeypatch.setattr(adapter_mod, "BUNDLED_DIR", bundle)
    fake_get, calls = _fixture_server()
    monkeypatch.setattr(adapter_mod, "_http_get", fake_get)
    reg = RegistryAdapter.load(cache_dir=str(tmp_path / "cache"))
    assert reg.provenance.source.startswith("network:")


def test_failed_refresh_uses_stale_cache_and_waits_for_ttl(tmp_path, monkeypatch):
    cache = tmp_path / "cache"
    shutil.copytree(SNAPSHOT, cache)
    old = time.time() - 2 * 86400
    os.utime(cache / "index.json", (old, old))
    calls = []

    def slow(url, timeout, deadline=None):
        calls.append(url)
        raise TimeoutError("download did not finish before the deadline")

    monkeypatch.setattr(adapter_mod, "_http_get", slow)
    reg = RegistryAdapter.load(cache_dir=str(cache), fetch_timeout=5)
    assert reg.provenance.source.startswith("cache (stale):")
    assert len(calls) == 1

    reg = RegistryAdapter.load(cache_dir=str(cache), fetch_timeout=5)
    assert reg.provenance.source.startswith("cache:")
    assert len(calls) == 1


def test_fetch_passes_one_deadline_to_every_request(tmp_path, monkeypatch):
    deadlines = []
    fake_get, _ = _fixture_server()

    def recording_get(url, timeout, deadline=None):
        deadlines.append(deadline)
        return fake_get(url, timeout)

    monkeypatch.setattr(adapter_mod, "_http_get", recording_get)
    adapter_mod.fetch_snapshot("http://localhost:8089", tmp_path / "a", deadline_seconds=30)
    assert len(deadlines) == 4 and len(set(deadlines)) == 1 and deadlines[0] > time.monotonic()

    deadlines.clear()
    adapter_mod.fetch_snapshot("http://localhost:8089", tmp_path / "b")
    assert deadlines == [None, None, None, None]


def test_failed_fetch_leaves_no_staging_directory(tmp_path, monkeypatch):
    fake_get, _ = _fixture_server()

    def failing(url, timeout, deadline=None):
        if url.endswith("categories.json"):
            raise TimeoutError("late")
        return fake_get(url, timeout)

    monkeypatch.setattr(adapter_mod, "_http_get", failing)
    with pytest.raises(TimeoutError):
        adapter_mod.fetch_snapshot("http://localhost:8089", tmp_path / "cache")
    assert not (tmp_path / "cache" / "datatypes.jsonl").exists()
    assert not list(tmp_path.glob("cache.partial*"))


class _FakeResponse:
    def __init__(self, body, headers, delay=0.0):
        self._stream = io.BytesIO(body)
        self.headers = headers
        self.delay = delay

    def read(self, size=-1):
        if self.delay:
            time.sleep(self.delay)
        return self._stream.read(size)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def test_http_get_requests_and_decompresses_gzip(monkeypatch):
    body = b'{"id": "email"}\n' * 1000
    seen = {}

    def fake_urlopen(request, timeout):
        seen["encoding"] = request.get_header("Accept-encoding")
        return _FakeResponse(gzip.compress(body), {"Content-Encoding": "gzip"})

    monkeypatch.setattr(adapter_mod.urllib.request, "urlopen", fake_urlopen)
    assert adapter_mod._http_get("https://registry.example/api/v1/datatypes.jsonl", 10) == body
    assert seen["encoding"] == "gzip"


def test_http_get_plain_response(monkeypatch):
    monkeypatch.setattr(
        adapter_mod.urllib.request, "urlopen", lambda request, timeout: _FakeResponse(b"plain", {})
    )
    assert adapter_mod._http_get("https://registry.example/x", 10) == b"plain"


def test_http_get_stops_at_deadline(monkeypatch):
    body = b"x" * (adapter_mod._CHUNK * 50)
    monkeypatch.setattr(
        adapter_mod.urllib.request, "urlopen", lambda request, timeout: _FakeResponse(body, {}, delay=0.02)
    )
    started = time.monotonic()
    with pytest.raises(TimeoutError):
        adapter_mod._http_get("https://registry.example/slow", 10, deadline=time.monotonic() + 0.1)
    assert time.monotonic() - started < 0.5


def test_fetch_timeout_setting(tmp_path, monkeypatch):
    from metacrafter.config import ConfigLoader

    assert ConfigLoader.get_registry_settings()["fetch_timeout"] == 30.0
    (tmp_path / ".metacrafter").write_text("registry_fetch_timeout: 5\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    assert ConfigLoader.get_registry_settings()["fetch_timeout"] == 5.0
