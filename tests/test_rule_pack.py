"""Default rule pack from GitHub releases: verification, safe unpacking, source order."""
import hashlib
import io
import sys
import tarfile

import pytest

import metacrafter.rules.pack as pack

RULE_YAML = b"""name: t
description: t
context: common
lang: common
rules:
  emailbyname: {key: email, match: text, type: field, rule: email}
"""


def make_archive(extra=None):
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as tar:
        files = {
            "metacrafter-rules-9.9.9/rules/common/t.yaml": RULE_YAML,
            "metacrafter-rules-9.9.9/metacrafterext/__init__.py": b"",
            "metacrafter-rules-9.9.9/metacrafterext/checks.py": b"def ok(value):\n    return True\n",
        }
        files.update(extra or {})
        for name, data in files.items():
            info = tarfile.TarInfo(name)
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))
    data = buffer.getvalue()
    return data, hashlib.sha256(data).hexdigest()


@pytest.fixture
def cache(tmp_path, monkeypatch):
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))
    monkeypatch.delenv("METACRAFTER_RULES_DIR", raising=False)
    monkeypatch.delenv("METACRAFTER_RULES_OFFLINE", raising=False)
    monkeypatch.setattr(pack, "_installed_pack", lambda: None)
    return tmp_path / "cache"


def test_fetch_verifies_and_unpacks(cache, monkeypatch):
    data, digest = make_archive()
    calls = []
    monkeypatch.setattr(pack, "_download", lambda url, timeout: calls.append(url) or data)
    result = pack.fetch_pack("v9.9.9", digest)
    assert result.rules_dir.name == "rules" and (result.rules_dir / "common" / "t.yaml").exists()
    assert result.python_path and (result.python_path / "metacrafterext").is_dir()
    assert calls == [pack.archive_url("v9.9.9")]
    # cached: no second download
    again = pack.fetch_pack("v9.9.9", digest)
    assert again.rules_dir == result.rules_dir and len(calls) == 1


def test_hash_mismatch_unpacks_nothing(cache, monkeypatch):
    data, _ = make_archive()
    monkeypatch.setattr(pack, "_download", lambda url, timeout: data)
    with pytest.raises(pack.RulePackError, match="does not match its pinned SHA-256"):
        pack.fetch_pack("v9.9.9", "0" * 64)
    assert not (cache / "metacrafter" / "rules" / "v9.9.9").exists()


def test_path_traversal_rejected(cache, monkeypatch):
    data, digest = make_archive({"../evil.py": b"x = 1\n"})
    monkeypatch.setattr(pack, "_download", lambda url, timeout: data)
    with pytest.raises(pack.RulePackError, match="Unsafe path"):
        pack.fetch_pack("v9.9.9", digest)
    assert not (cache / "metacrafter" / "evil.py").exists()


def test_unpinned_release_refused(cache):
    with pytest.raises(pack.RulePackError, match="needs rules_sha256"):
        pack.resolve_pack(release="v9.9.9")


def test_offline_without_cache_returns_none(cache):
    # Only the pinned release is bundled; another release needs a cache or a download.
    assert pack.resolve_pack(release="v9.9.9", sha256="0" * 64, offline=True) is None


def _no_network(url, timeout):
    raise AssertionError(f"unexpected download of {url}")


def test_bundled_archive_matches_pin():
    bundled = pack.bundled_archive()
    assert bundled.is_file(), "run scripts/bundle_rule_pack.py after changing DEFAULT_RELEASE"
    assert hashlib.sha256(bundled.read_bytes()).hexdigest() == pack.DEFAULT_SHA256
    assert sorted(p.name for p in pack.BUNDLED_DIR.glob("*.tar.gz")) == [bundled.name]


def test_cache_miss_uses_bundled_archive_without_network(cache, monkeypatch):
    monkeypatch.setattr(pack, "_download", _no_network)
    resolved = pack.resolve_pack()
    assert resolved.source.startswith("bundled:") and resolved.release == pack.DEFAULT_RELEASE
    assert any(resolved.rules_dir.rglob("*.yaml"))
    assert (resolved.python_path / "metacrafterext").is_dir()
    again = pack.resolve_pack()
    assert again.source.startswith("cache:") and again.rules_dir == resolved.rules_dir


def test_offline_uses_bundled_archive(cache, monkeypatch):
    monkeypatch.setattr(pack, "_download", _no_network)
    monkeypatch.setenv("METACRAFTER_RULES_OFFLINE", "1")
    resolved = pack.resolve_pack()
    assert resolved is not None and resolved.source.startswith("bundled:")


def test_fetch_uses_bundled_archive_and_force_downloads(cache, monkeypatch):
    data = pack.bundled_archive().read_bytes()
    calls = []
    monkeypatch.setattr(pack, "_download", lambda url, timeout: calls.append(url) or data)
    assert pack.fetch_pack().source.startswith("bundled:") and calls == []
    (pack.fetch_pack().rules_dir / "stale.yaml").write_text("name: stale\n")
    forced = pack.fetch_pack(force=True)
    assert forced.source.startswith("download:") and calls == [pack.archive_url(pack.DEFAULT_RELEASE)]
    assert not (forced.rules_dir / "stale.yaml").exists()


def test_corrupt_bundled_archive_falls_back(cache, monkeypatch, tmp_path):
    data = pack.bundled_archive().read_bytes()
    monkeypatch.setattr(pack, "BUNDLED_DIR", tmp_path / "bundled")
    pack.BUNDLED_DIR.mkdir()
    pack.bundled_archive().write_bytes(b"not an archive")
    monkeypatch.setattr(pack, "_download", lambda url, timeout: data)
    assert pack.resolve_pack(offline=True) is None
    assert not (cache / "metacrafter" / "rules" / pack.DEFAULT_RELEASE).exists()
    assert pack.resolve_pack().source.startswith("download:")


def test_rules_dir_wins_and_activates_validators(cache, tmp_path):
    root = tmp_path / "checkout"
    (root / "rules").mkdir(parents=True)
    (root / "rules" / "t.yaml").write_bytes(RULE_YAML)
    (root / "metacrafterext").mkdir()
    resolved = pack.resolve_pack(rules_dir=str(root))
    assert resolved.rules_dir == (root / "rules").resolve()
    pack.activate(resolved)
    assert str(root.resolve()) in sys.path
    sys.path.remove(str(root.resolve()))


def test_default_pin_matches_ecosystem_lock():
    from pathlib import Path

    import yaml

    lock = yaml.safe_load((Path(__file__).parent.parent / "ecosystem.lock").read_text())
    assert lock["metacrafter-rules"]["release"] == pack.DEFAULT_RELEASE
    assert lock["metacrafter-rules"]["sha256"] == pack.DEFAULT_SHA256
