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
    assert pack.resolve_pack(offline=True) is None


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
