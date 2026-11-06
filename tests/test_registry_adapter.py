"""Registry adapter: source resolution, canonical ids, enrichment and filters."""

import hashlib
import json
import shutil
from pathlib import Path

import pytest

from metacrafter.registry import adapter as adapter_mod
from metacrafter.registry.adapter import RegistryAdapter, clear_adapter_cache, verify_snapshot

SNAPSHOT = Path(__file__).parent / "fixtures" / "registry_snapshot"


@pytest.fixture(autouse=True)
def _fresh_adapter_cache():
    clear_adapter_cache()
    yield
    clear_adapter_cache()


@pytest.fixture
def reg():
    return RegistryAdapter.from_directory(SNAPSHOT)


def test_snapshot_loads_offline(reg):
    assert reg.available
    assert reg.provenance.source.startswith("path:")
    assert reg.provenance.version == "1.3.0.dev0-test"
    assert reg.resolve("email").canonical_id == "email"


def test_alias_key_resolves_to_canonical_id(reg):
    res = reg.resolve("ssn")
    assert res.canonical_id == "usssn"
    assert res.via_alias
    assert reg.url_for("ssn").endswith("/datatype/usssn")


def test_unknown_key(reg):
    res = reg.resolve("foo_bar")
    assert res.canonical_id is None
    match = reg.enrich_match({"dataclass": "foo_bar", "is_pii": False})
    assert match["datatype"] is None
    assert match["rule_key"] == "foo_bar"


def test_deprecated_record_reports_replaced_by(reg):
    match = reg.enrich_match({"dataclass": "legacy_contact", "is_pii": False})
    assert match["datatype"] == "legacy_contact"
    assert match["replaced_by"] == "email"


def test_base_url_override_rebases_manifest_template():
    reg = RegistryAdapter.from_directory(SNAPSHOT, base_url="http://localhost:8089")
    assert reg.url_for("email") == "http://localhost:8089/datatype/email"


def test_enrichment_adds_registry_metadata(reg):
    match = reg.enrich_match({"dataclass": "email", "is_pii": False, "pii_source": None})
    assert match["registry"]["name"]
    assert match["registry"]["status"] == "stable"
    assert "pii" in match["registry"]["categories"]
    assert match["is_pii"] is True
    assert match["pii_source"] == "registry"


def test_rule_flag_wins_pii_source(reg):
    match = reg.enrich_match({"dataclass": "email", "is_pii": True, "pii_source": "rule"})
    assert match["pii_source"] == "rule"


def test_non_pii_registry_keeps_context_pii(reg):
    match = reg.enrich_match({"dataclass": "iban", "is_pii": True, "pii_source": "context"})
    assert match["is_pii"] is True
    assert match["pii_source"] == "context"


def test_unavailable_adapter_builds_urls():
    reg = RegistryAdapter.unavailable("http://example.test")
    assert not reg.available
    assert reg.url_for("email") == "http://example.test/datatype/email"
    assert reg.provenance.source == "unavailable"


def test_offline_without_sources_is_unavailable(tmp_path, monkeypatch):
    monkeypatch.setattr(adapter_mod, "BUNDLED_DIR", tmp_path / "no-bundle")
    reg = RegistryAdapter.load(offline=True, cache_dir=str(tmp_path / "cache"))
    assert reg.provenance.source == "unavailable"


def test_corrupted_cache_is_rejected(tmp_path):
    cache = tmp_path / "cache"
    shutil.copytree(SNAPSHOT, cache)
    assert verify_snapshot(cache)
    (cache / "datatypes.jsonl").write_text("{}\n", encoding="utf-8")
    assert not verify_snapshot(cache)


def test_fetch_verifies_hashes_and_falls_back(tmp_path, monkeypatch):
    files = {name: (SNAPSHOT / name).read_bytes() for name in ("datatypes.jsonl", "aliases.json", "categories.json")}
    manifest = json.loads((SNAPSHOT / "index.json").read_text())

    def fake_get(url, timeout, deadline=None):
        name = url.rsplit("/", 1)[-1]
        if name == "index.json":
            return json.dumps(manifest).encode()
        return files[name]

    monkeypatch.setattr(adapter_mod, "_http_get", fake_get)
    reg = RegistryAdapter.load(base_url="https://registry.example", cache_dir=str(tmp_path / "c1"))
    assert reg.provenance.source.startswith("network:")
    assert reg.resolve("guid").canonical_id == "uuid"

    files["datatypes.jsonl"] = b'{"id": "tampered"}\n'
    reg2 = RegistryAdapter.load(base_url="https://registry.example", cache_dir=str(tmp_path / "c2"))
    assert reg2.provenance.source == "unavailable"


def test_fresh_cache_skips_network(tmp_path, monkeypatch):
    cache = tmp_path / "cache"
    shutil.copytree(SNAPSHOT, cache)

    def no_network(url, timeout, deadline=None):
        raise AssertionError("network must not be used with a fresh cache")

    monkeypatch.setattr(adapter_mod, "_http_get", no_network)
    reg = RegistryAdapter.load(cache_dir=str(cache))
    assert reg.provenance.source.startswith("cache:")


def test_scan_report_carries_canonical_ids_and_provenance():
    from metacrafter.core import CrafterCmd

    cmd = CrafterCmd(quiet=True)
    report = cmd.scan_data(
        [{"contact_email": "a@example.com"}, {"contact_email": "b@example.org"}],
        parse_dates=False,
    )
    assert report["registry"]["version"] == "1.3.0.dev0-test"
    email = next(f for f in report["data"] if f["field"] == "contact_email")
    match = next(m for m in email["matches"] if m["dataclass"] == "email")
    assert match["datatype"] == "email"
    assert match["registry"]["status"] == "stable"
    assert match["is_pii"] is True
    assert email["datatype_url"].endswith("/datatype/email")


def test_registry_filters_pii_only_and_status():
    from metacrafter.core import CrafterCmd

    cmd = CrafterCmd(quiet=True, pii_only=True, status="stable")
    report = cmd.scan_data(
        [{"contact_email": "a@example.com", "id": "3f2504e0-4f89-11d3-9a0c-0305e82c3301"}],
        parse_dates=False,
    )
    for field in report["data"]:
        for match in field["matches"]:
            assert match["is_pii"]
            assert match["registry"]["status"] == "stable"


def test_env_url_controls_report_urls(monkeypatch):
    from metacrafter.core import CrafterCmd

    monkeypatch.setenv("METACRAFTER_REGISTRY_URL", "http://localhost:8089")
    clear_adapter_cache()
    report = CrafterCmd(quiet=True).scan_data([{"contact_email": "a@example.com"}], parse_dates=False)
    field = report["data"][0]
    assert field["datatype_url"].startswith("http://localhost:8089/datatype/")
    assert all(m["classurl"].startswith("http://localhost:8089/") for m in field["matches"])
