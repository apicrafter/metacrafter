# -*- coding: utf-8 -*-
"""Single access point to metacrafter-registry data.

Every consumer (scan output, HTTP API, exporters, LLM, CLI) resolves datatypes
through :class:`RegistryAdapter`. Metacrafter talks to the registry only through the
files its website publishes under ``/api/v1/``; it does not use the registry's Python
package (see ``openspec/changes/update-ecosystem-distribution``).

Source order (see ``openspec/changes/update-bundle-registry-snapshot``):

1. ``registry_path``: a snapshot directory with the ``/api/v1/`` files
   (``metacrafter registry fetch --to DIR`` writes one);
2. a fresh, hash-verified local cache;
3. when no verified cache exists and the base URL is the one of the bundled snapshot
   (``metacrafter/registry/bundled/``), that snapshot, copied into the cache;
4. a network fetch from the configured base URL (unless offline), gzip-compressed,
   limited by an overall deadline and verified against the manifest SHA-256 values;
5. a stale but verified cache. A failed fetch marks it as checked, so the network is
   tried again only after the cache TTL.

When nothing loads, the adapter is still usable: it resolves nothing and builds
URLs from the configured base URL, and its provenance says ``unavailable``.
"""
from __future__ import annotations

import gzip
import hashlib
import json
import logging
import os
import re
import shutil
import tempfile
import threading
import time
import urllib.request
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional

logger = logging.getLogger(__name__)

DEFAULT_REGISTRY_URL = "https://registry.apicrafter.io"
DEFAULT_CACHE_TTL = 24 * 3600
DEFAULT_TIMEOUT = 10.0
#: Overall time limit (seconds) for a registry download on the scan path.
DEFAULT_FETCH_TIMEOUT = 30.0
SNAPSHOT_FILES = ("datatypes.jsonl", "aliases.json", "categories.json")
#: Registry snapshot shipped in the package: the version recorded in ``ecosystem.lock``.
BUNDLED_DIR = Path(__file__).parent / "bundled"
_CHUNK = 64 * 1024

#: Crosswalk systems copied from registry ``mappings`` into match enrichment.
ENRICHMENT_MAPPINGS = ("presidio", "google_dlp", "dpv", "purview", "schema_org", "wikidata")


@dataclass(frozen=True)
class RegistryProvenance:
    """Which registry data resolved a report's matches."""

    source: str
    base_url: str
    version: Optional[str] = None
    git_sha: Optional[str] = None
    generated_at: Optional[str] = None

    def asdict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class Resolution:
    """Result of resolving a rule key against the registry."""

    key: str
    canonical_id: Optional[str]
    via_alias: bool = False
    status: Optional[str] = None
    replaced_by: Optional[str] = None

    @property
    def known(self) -> bool:
        return self.canonical_id is not None


@dataclass(frozen=True)
class DatatypeRef:
    """Registry metadata attached to a match."""

    id: str
    name: Optional[str]
    url: str
    status: Optional[str] = None
    is_pii: Optional[bool] = None
    classification: Optional[str] = None
    kind: Optional[str] = None
    categories: List[str] = field(default_factory=list)
    country: List[str] = field(default_factory=list)
    mappings: Dict[str, Any] = field(default_factory=dict)
    replaced_by: Optional[str] = None

    def asdict(self) -> Dict[str, Any]:
        data = asdict(self)
        return {k: v for k, v in data.items() if v not in (None, [], {})}


class SnapshotRegistry:
    """Registry records loaded from published ``/api/v1/`` files."""

    def __init__(self, datatypes, aliases, manifest, source):
        from metacrafter.registry.identify import RecordIndex

        self.datatypes = {r["id"]: r for r in datatypes}
        self.aliases = dict(aliases or {})
        self.manifest = dict(manifest or {})
        self.source = source
        self.index = RecordIndex(self.datatypes)

    @classmethod
    def from_directory(cls, directory: Path, source: str = "") -> "SnapshotRegistry":
        def read_json(name, default):
            path = directory / name
            return json.loads(path.read_text("utf-8")) if path.exists() else default

        lines = (directory / "datatypes.jsonl").read_text("utf-8").splitlines()
        return cls(
            datatypes=[json.loads(line) for line in lines if line.strip()],
            aliases=read_json("aliases.json", {}),
            manifest=read_json("index.json", {}),
            source=source or str(directory),
        )

    @property
    def version(self) -> str:
        return str(self.manifest.get("version", "unknown"))

    def __iter__(self) -> Iterator[Dict[str, Any]]:
        return iter(self.datatypes.values())

    def resolve(self, rid: str) -> Optional[str]:
        rid = str(rid).strip()
        if rid in self.datatypes:
            return rid
        return self.aliases.get(rid)

    def get(self, rid: str) -> Optional[Dict[str, Any]]:
        canonical = self.resolve(rid)
        return self.datatypes.get(canonical) if canonical else None


# Backward-compatible name.
_SnapshotRegistry = SnapshotRegistry


def _time_left(url: str, deadline: Optional[float]) -> Optional[float]:
    if deadline is None:
        return None
    left = deadline - time.monotonic()
    if left <= 0:
        raise TimeoutError(f"download of {url} did not finish before the deadline")
    return left


def _set_read_timeout(response: Any, seconds: float) -> None:
    """Best effort: shorten the socket timeout of the next read to the time left."""
    sock = getattr(getattr(getattr(response, "fp", None), "raw", None), "_sock", None)
    if sock is not None:
        try:
            sock.settimeout(seconds)
        except OSError:
            pass


def _http_get(url: str, timeout: float, deadline: Optional[float] = None) -> bytes:
    """GET ``url`` and return the (decompressed) body.

    ``timeout`` limits each socket operation; ``deadline`` (a ``time.monotonic()`` value)
    limits the whole request. gzip responses are accepted and decompressed.
    """
    request = urllib.request.Request(url, headers={"User-Agent": "metacrafter", "Accept-Encoding": "gzip"})
    left = _time_left(url, deadline)
    first_timeout = timeout if left is None else min(timeout, left)
    with urllib.request.urlopen(request, timeout=first_timeout) as response:  # noqa: S310 - configured registry URL
        chunks = []
        while True:
            left = _time_left(url, deadline)
            if left is not None:
                _set_read_timeout(response, min(timeout, left))
            chunk = response.read(_CHUNK)
            if not chunk:
                break
            chunks.append(chunk)
        encoding = (response.headers.get("Content-Encoding") or "").strip().lower()
    body = b"".join(chunks)
    if encoding == "gzip":
        body = gzip.decompress(body)
    return body


def _expected_hashes(manifest: Dict[str, Any]) -> Dict[str, str]:
    hashes = {}
    for name, info in (manifest.get("files") or {}).items():
        if not isinstance(info, dict) or "sha256" not in info:
            continue
        path = str(info.get("path") or name)
        if "api/v1/" in path or str(name).startswith("api/v1/"):
            hashes[Path(path).name] = info["sha256"]
    return hashes


def verify_snapshot(directory: Path) -> bool:
    """Return True when every snapshot file listed in ``index.json`` matches its SHA-256."""
    index = directory / "index.json"
    if not (directory / "datatypes.jsonl").exists():
        return False
    if not index.exists():
        return True
    hashes = _expected_hashes(json.loads(index.read_text("utf-8")))
    for name, expected in hashes.items():
        path = directory / name
        if path.exists() and hashlib.sha256(path.read_bytes()).hexdigest() != expected:
            logger.warning("Registry file %s does not match its manifest SHA-256 (%s)", path, expected)
            return False
    return True


def _install_snapshot(cache_dir: Path, files: Dict[str, bytes]) -> Path:
    """Write snapshot files to a staging directory, then swap them into ``cache_dir``.

    ``index.json`` is replaced last, so an interrupted swap leaves a cache that fails
    verification instead of one that looks current.
    """
    cache_dir.mkdir(parents=True, exist_ok=True)
    # One staging directory per call: concurrent first scans must not remove each other's files.
    staging = Path(tempfile.mkdtemp(prefix=cache_dir.name + ".partial-", dir=cache_dir.parent))
    try:
        for name, body in files.items():
            (staging / name).write_bytes(body)
        names = sorted(files, key=lambda n: n == "index.json")
        for name in names:
            os.replace(staging / name, cache_dir / name)
    finally:
        shutil.rmtree(staging, ignore_errors=True)
    return cache_dir


def fetch_snapshot(
    base_url: str,
    cache_dir: Path,
    timeout: float = DEFAULT_TIMEOUT,
    deadline_seconds: Optional[float] = None,
) -> Path:
    """Download the v1 snapshot into ``cache_dir`` and verify it against the manifest.

    Files are downloaded and verified first and swapped in only after every hash matches,
    so a failed download never corrupts a working cache. ``deadline_seconds`` limits the
    whole download (``None``: no overall limit).
    """
    deadline = None if deadline_seconds is None else time.monotonic() + deadline_seconds
    base = base_url.rstrip("/")
    manifest_bytes = _http_get(f"{base}/api/v1/index.json", timeout, deadline)
    manifest = json.loads(manifest_bytes)
    cache_dir.mkdir(parents=True, exist_ok=True)
    cached_index = cache_dir / "index.json"
    if cached_index.exists() and json.loads(cached_index.read_text("utf-8")) == manifest and verify_snapshot(cache_dir):
        cached_index.touch()
        return cache_dir
    hashes = _expected_hashes(manifest)
    files: Dict[str, bytes] = {}
    for name in SNAPSHOT_FILES:
        body = _http_get(f"{base}/api/v1/{name}", timeout, deadline)
        expected = hashes.get(name)
        if expected and hashlib.sha256(body).hexdigest() != expected:
            raise ValueError(f"Registry file {name}: SHA-256 does not match the manifest ({expected})")
        files[name] = body
    files["index.json"] = manifest_bytes
    return _install_snapshot(cache_dir, files)


def bundled_manifest(directory: Optional[Path] = None) -> Optional[Dict[str, Any]]:
    """Manifest (``index.json``) of the bundled registry snapshot, or None when absent."""
    index = Path(directory or BUNDLED_DIR) / "index.json"
    if not index.exists():
        return None
    return json.loads(index.read_text("utf-8"))


def read_bundled_snapshot(directory: Optional[Path] = None) -> Dict[str, bytes]:
    """Files of the bundled snapshot, decompressed and verified against its manifest."""
    directory = Path(directory or BUNDLED_DIR)
    manifest_bytes = (directory / "index.json").read_bytes()
    hashes = _expected_hashes(json.loads(manifest_bytes))
    files: Dict[str, bytes] = {}
    for name in SNAPSHOT_FILES:
        packed = directory / f"{name}.gz"
        body = gzip.decompress(packed.read_bytes()) if packed.exists() else (directory / name).read_bytes()
        expected = hashes.get(name)
        if not expected:
            raise ValueError(f"Bundled registry manifest has no SHA-256 for {name}")
        actual = hashlib.sha256(body).hexdigest()
        if actual != expected:
            raise ValueError(f"Bundled registry file {name}: SHA-256 {actual} does not match the manifest ({expected})")
        files[name] = body
    files["index.json"] = manifest_bytes
    return files


def default_cache_dir(base_url: str) -> Path:
    root = os.environ.get("XDG_CACHE_HOME") or str(Path.home() / ".cache")
    digest = hashlib.sha1(base_url.rstrip("/").encode()).hexdigest()[:12]
    return Path(root) / "metacrafter" / "registry" / digest


class RegistryAdapter:
    """Resolve, describe and link datatypes using metacrafter-registry data."""

    def __init__(self, registry: Any, provenance: RegistryProvenance) -> None:
        self.registry = registry
        self.provenance = provenance
        self._warned: set = set()

    # --- construction ------------------------------------------------------

    @classmethod
    def unavailable(cls, base_url: str = DEFAULT_REGISTRY_URL) -> "RegistryAdapter":
        return cls(None, RegistryProvenance(source="unavailable", base_url=base_url.rstrip("/")))

    @classmethod
    def from_directory(cls, directory, base_url: Optional[str] = None, source: Optional[str] = None) -> "RegistryAdapter":
        """Load a snapshot directory with the published ``/api/v1/`` files."""
        directory = Path(directory)
        if not (directory / "datatypes.jsonl").exists():
            raise FileNotFoundError(
                f"{directory} is not a registry snapshot (no datatypes.jsonl). "
                "Create one with: metacrafter registry fetch --to DIR"
            )
        if not verify_snapshot(directory):
            raise ValueError(f"Registry snapshot {directory} does not match its manifest hashes")
        registry = SnapshotRegistry.from_directory(directory)
        return cls._wrap(registry, base_url, source or f"path:{directory}")

    @classmethod
    def _wrap(cls, registry: Any, base_url: Optional[str], source: str) -> "RegistryAdapter":
        manifest = dict(getattr(registry, "manifest", {}) or {})
        base = (base_url or manifest.get("base_url") or DEFAULT_REGISTRY_URL).rstrip("/")
        provenance = RegistryProvenance(
            source=source,
            base_url=base,
            version=str(manifest.get("version")) if manifest.get("version") else None,
            git_sha=manifest.get("git_sha"),
            generated_at=manifest.get("generated_at"),
        )
        return cls(registry, provenance)

    @classmethod
    def load(
        cls,
        *,
        base_url: Optional[str] = None,
        registry_path: Optional[str] = None,
        offline: bool = False,
        cache_dir: Optional[str] = None,
        cache_ttl: float = DEFAULT_CACHE_TTL,
        timeout: float = DEFAULT_TIMEOUT,
        fetch_timeout: Optional[float] = DEFAULT_FETCH_TIMEOUT,
    ) -> "RegistryAdapter":
        """Load registry data following the documented source order.

        ``fetch_timeout`` limits the whole network download (``None``: no limit).
        """
        base = (base_url or DEFAULT_REGISTRY_URL).rstrip("/")
        if registry_path:
            try:
                return cls.from_directory(registry_path, base_url=base)
            except Exception as exc:  # noqa: BLE001 - fall through to other sources
                logger.warning("Could not load registry from %s: %s", registry_path, exc)

        cache = Path(cache_dir) if cache_dir else default_cache_dir(base)
        index = cache / "index.json"
        cache_valid = index.exists() and verify_snapshot(cache)
        if cache_valid and time.time() - index.stat().st_mtime < cache_ttl:
            return cls._from_cache(cache, base, "cache")

        if not cache_valid and cls._seed_from_bundle(cache, base):
            return cls._from_cache(cache, base, "bundled")

        if not offline:
            try:
                fetch_snapshot(base, cache, timeout=timeout, deadline_seconds=fetch_timeout)
                return cls._from_cache(cache, base, "network")
            except Exception as exc:  # noqa: BLE001 - network or hash failures fall back
                limit = f" (time limit {fetch_timeout:g} s)" if fetch_timeout is not None else ""
                logger.warning("Could not fetch registry from %s%s: %s", base, limit, exc)
                cache_valid = index.exists() and verify_snapshot(cache)
                if cache_valid:
                    # Checked now: retry after the TTL instead of on every scan.
                    index.touch()

        if cache_valid:
            return cls._from_cache(cache, base, "cache (stale)")

        logger.warning(
            "No registry data available (offline=%s); datatype ids are not resolved. "
            "Set registry_path or allow network access to %s.",
            offline,
            base,
        )
        return cls.unavailable(base)

    @staticmethod
    def _seed_from_bundle(cache: Path, base: str) -> bool:
        """Copy the bundled snapshot into an empty or invalid cache for its own base URL."""
        try:
            manifest = bundled_manifest()
            if manifest is None or str(manifest.get("base_url") or "").rstrip("/") != base:
                return False
            _install_snapshot(cache, read_bundled_snapshot())
            return True
        except Exception as exc:  # noqa: BLE001 - a broken bundle falls back to the network
            logger.warning("Could not use the bundled registry snapshot: %s", exc)
            return False

    @classmethod
    def _from_cache(cls, cache: Path, base: str, kind: str) -> "RegistryAdapter":
        registry = SnapshotRegistry.from_directory(cache, source=f"{kind}:{cache}")
        return cls._wrap(registry, base, f"{kind}:{cache}")

    # --- lookup ------------------------------------------------------------

    @property
    def available(self) -> bool:
        return self.registry is not None

    @property
    def base_url(self) -> str:
        return self.provenance.base_url

    def __len__(self) -> int:
        return len(getattr(self.registry, "datatypes", {}) or {})

    def resolve(self, key: str) -> Resolution:
        key = str(key)
        if self.registry is None:
            return Resolution(key=key, canonical_id=None)
        canonical = self.registry.resolve(key)
        if canonical is None:
            return Resolution(key=key, canonical_id=None)
        record = self.registry.get(canonical) or {}
        return Resolution(
            key=key,
            canonical_id=canonical,
            via_alias=canonical != key,
            status=record.get("status"),
            replaced_by=record.get("replaced_by"),
        )

    def record(self, datatype_id: str) -> Optional[Dict[str, Any]]:
        if self.registry is None:
            return None
        return self.registry.get(datatype_id)

    def url_for(self, datatype_id: str) -> str:
        """Datatype page URL built from the manifest template on the configured base URL."""
        canonical = datatype_id
        if self.registry is not None:
            canonical = self.registry.resolve(datatype_id) or datatype_id
        manifest = dict(getattr(self.registry, "manifest", {}) or {})
        template = manifest.get("datatype_url_template")
        manifest_base = str(manifest.get("base_url") or "").rstrip("/")
        if template and manifest_base and template.startswith(manifest_base):
            template = self.base_url + template[len(manifest_base):]
        elif not template:
            template = self.base_url + "/datatype/{id}"
        return template.replace("{id}", canonical)

    def describe(self, datatype_id: str) -> Optional[DatatypeRef]:
        record = self.record(datatype_id)
        if record is None:
            return None
        mappings = record.get("mappings") or {}
        return DatatypeRef(
            id=record["id"],
            name=record.get("name"),
            url=self.url_for(record["id"]),
            status=record.get("status"),
            is_pii=record.get("is_pii"),
            classification=record.get("classification"),
            kind=record.get("kind"),
            categories=_ids(record.get("categories")),
            country=_ids(record.get("country")),
            mappings={k: mappings[k] for k in ENRICHMENT_MAPPINGS if k in mappings},
            replaced_by=record.get("replaced_by"),
        )

    def is_pii(self, datatype_id: str) -> Optional[bool]:
        record = self.record(datatype_id)
        return None if record is None else bool(record.get("is_pii"))

    def iter_records(
        self,
        *,
        status: Optional[Iterable[str]] = None,
        categories: Optional[Iterable[str]] = None,
        country: Optional[str] = None,
    ) -> Iterator[Dict[str, Any]]:
        if self.registry is None:
            return iter(())
        statuses = set(status) if status else None
        cats = set(categories) if categories else None
        cc = country.upper() if country else None

        def keep(record):
            if statuses and record.get("status") not in statuses:
                return False
            if cats and not cats & set(_ids(record.get("categories"))):
                return False
            if cc and cc not in [c.upper() for c in _ids(record.get("country"))]:
                return False
            return True

        return (r for r in self.registry if keep(r))

    # --- detection support -------------------------------------------------

    def qddate_pattern(self, date_format: str) -> Optional[str]:
        """Registry pattern record id for a qddate format key (e.g. ``dt:date:date_9``)."""
        if self.registry is None or not date_format:
            return None
        return self.registry.index.qddate_pattern(date_format)

    def specificities(self, ids: Iterable[str]) -> Dict[str, float]:
        """``1 - broadness`` of the regexps of ``ids`` (share of other records' examples matched)."""
        if self.registry is None:
            return {}
        return {rid: round(1.0 - b, 3) for rid, b in self.registry.index.broadness(ids).items()}

    def identify(self, values: Iterable[Any], *, field_name: Optional[str] = None,
                 country: Optional[str] = None, limit: int = 10) -> List[Dict[str, Any]]:
        """Rank registry datatypes for sample values of one column (runs locally)."""
        if self.registry is None:
            return []
        return self.registry.index.identify(values, field_name=field_name, country=country, limit=limit)

    def search(self, query: str, limit: int = 20) -> List[Dict[str, Any]]:
        """Records matching query tokens (id, aliases, names, categories, description)."""
        if self.registry is None:
            return []
        return self.registry.index.search(query, limit=limit)

    # --- match enrichment --------------------------------------------------

    def warn_unknown(self, key: str) -> None:
        if key in self._warned or not self.available:
            return
        self._warned.add(key)
        logger.warning("Rule key '%s' is not a registry datatype id or alias", key)

    def enrich_match(self, match: Dict[str, Any]) -> Dict[str, Any]:
        """Add canonical id, URL, registry metadata and PII source to a match dict.

        ``match['dataclass']`` keeps the original rule key for backward compatibility.
        """
        key = match.get("rule_key") or match.get("dataclass")
        if not key:
            return match
        match["rule_key"] = key
        resolution = self.resolve(key)
        pii_source = match.get("pii_source")
        if resolution.known:
            ref = self.describe(resolution.canonical_id)
            match["datatype"] = resolution.canonical_id
            match["classurl"] = ref.url if ref else self.url_for(resolution.canonical_id)
            if ref is not None:
                match["registry"] = ref.asdict()
                if ref.is_pii and pii_source != "rule":
                    # Precedence of the reported source: rule flag, registry, rule context.
                    pii_source = "registry"
                match["is_pii"] = bool(match.get("is_pii") or ref.is_pii)
            if resolution.replaced_by:
                match["replaced_by"] = resolution.replaced_by
        else:
            match["datatype"] = None
            match["classurl"] = self.url_for(key)
            self.warn_unknown(key)
        if match.get("is_pii") and not pii_source:
            pii_source = "context"
        match["pii_source"] = pii_source if match.get("is_pii") else None
        return match


def _ids(values: Any) -> List[str]:
    out = []
    for value in values or []:
        if isinstance(value, dict):
            value = value.get("id")
        if value:
            out.append(str(value))
    return out


_CACHE: Dict[tuple, RegistryAdapter] = {}
_CACHE_LOCK = threading.Lock()


def get_adapter(
    *,
    base_url: Optional[str] = None,
    registry_path: Optional[str] = None,
    offline: bool = False,
    cache_dir: Optional[str] = None,
    cache_ttl: float = DEFAULT_CACHE_TTL,
    timeout: float = DEFAULT_TIMEOUT,
    fetch_timeout: Optional[float] = DEFAULT_FETCH_TIMEOUT,
) -> RegistryAdapter:
    """Return a process-wide adapter for the given settings (loaded once)."""
    key = (base_url, registry_path, offline, cache_dir)
    with _CACHE_LOCK:
        adapter = _CACHE.get(key)
        if adapter is None:
            adapter = RegistryAdapter.load(
                base_url=base_url,
                registry_path=registry_path,
                offline=offline,
                cache_dir=cache_dir,
                cache_ttl=cache_ttl,
                timeout=timeout,
                fetch_timeout=fetch_timeout,
            )
            _CACHE[key] = adapter
        return adapter


def clear_adapter_cache() -> None:
    with _CACHE_LOCK:
        _CACHE.clear()
