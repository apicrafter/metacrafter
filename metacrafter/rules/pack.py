"""Default rule pack from GitHub releases of ``apicrafter/metacrafter-rules``.

The rule pack is not a Python dependency. Metacrafter downloads the release archive
pinned below, verifies its SHA-256, unpacks it into the cache and loads ``rules/``. The
pack's Python validators (``metacrafterext``) become importable from the unpacked
archive.

Source order (first match wins):

1. ``rules_dir`` (config) or ``METACRAFTER_RULES_DIR``: a metacrafter-rules checkout or
   an unpacked pack (a directory with ``rules/``), or a plain directory of rule YAML;
2. an installed ``metacrafterext`` package (development installs);
3. the cached release in ``~/.cache/metacrafter/rules/<tag>/``;
4. a download of the release, unless ``rules_offline`` / ``METACRAFTER_RULES_OFFLINE``.

``rules_release`` selects another release tag and needs ``rules_sha256``; an unpinned
release is refused.
"""
from __future__ import annotations

import hashlib
import io
import json
import logging
import os
import shutil
import sys
import tarfile
import tempfile
import urllib.request
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Optional

logger = logging.getLogger(__name__)

RULES_REPO = "apicrafter/metacrafter-rules"
#: Release pinned by this version of metacrafter.
DEFAULT_RELEASE = "v0.0.2"
DEFAULT_SHA256 = "6828315bda4e36ba7afcb24ba949bcaea73faff4b08cc2b4f2d8609b26f6d946"
MARKER = ".metacrafter-pack.json"
DOWNLOAD_TIMEOUT = 60.0
MAX_ARCHIVE_BYTES = 50 * 1024 * 1024


class RulePackError(RuntimeError):
    """The rule pack could not be resolved, downloaded or verified."""


@dataclass(frozen=True)
class RulePack:
    """A resolved rule pack."""

    rules_dir: Path
    python_path: Optional[Path]
    source: str
    release: Optional[str] = None


def archive_url(tag: str, repo: str = RULES_REPO) -> str:
    return f"https://github.com/{repo}/archive/refs/tags/{tag}.tar.gz"


def cache_root() -> Path:
    base = os.environ.get("XDG_CACHE_HOME") or str(Path.home() / ".cache")
    return Path(base) / "metacrafter" / "rules"


def _env_flag(name: str) -> Optional[bool]:
    value = os.environ.get(name)
    if value is None:
        return None
    return value.strip().lower() in ("1", "true", "yes", "on")


def _pack_from_dir(directory: Path, source: str, release: Optional[str] = None) -> RulePack:
    directory = directory.expanduser().resolve()
    if (directory / "rules").is_dir():
        python_path = directory if (directory / "metacrafterext").is_dir() else None
        return RulePack(directory / "rules", python_path, source, release)
    if directory.is_dir() and any(directory.rglob("*.yaml")):
        sibling = directory.parent
        python_path = sibling if (sibling / "metacrafterext").is_dir() else None
        return RulePack(directory, python_path, source, release)
    raise RulePackError(f"{directory} contains no rules/ directory or rule YAML files")


def _installed_pack() -> Optional[RulePack]:
    try:
        from metacrafterext.rules import rules_dir  # type: ignore[import-not-found]
    except Exception:  # noqa: BLE001 - not installed
        return None
    path = Path(rules_dir())
    if path.is_dir() and any(path.rglob("*.yaml")):
        return RulePack(path, None, "installed package")
    return None


def _cached_pack(tag: str, sha256: str) -> Optional[RulePack]:
    target = cache_root() / tag
    marker = target / MARKER
    if not marker.exists():
        return None
    try:
        info = json.loads(marker.read_text("utf-8"))
    except (OSError, ValueError):
        return None
    if info.get("sha256") != sha256:
        return None
    root = target / info.get("root", "")
    try:
        return _pack_from_dir(root, f"cache:{target}", tag)
    except RulePackError:
        return None


def _safe_members(archive: tarfile.TarFile):
    """Archive members that stay inside the extraction directory (files and directories only)."""
    for member in archive.getmembers():
        name = PurePosixPath(member.name)
        if name.is_absolute() or ".." in name.parts:
            raise RulePackError(f"Unsafe path in rule pack archive: {member.name}")
        if member.issym() or member.islnk() or member.isdev() or member.isfifo():
            continue
        if not (member.isfile() or member.isdir()):
            continue
        yield member


def _download(url: str, timeout: float) -> bytes:
    request = urllib.request.Request(url, headers={"User-Agent": "metacrafter"})
    with urllib.request.urlopen(request, timeout=timeout) as response:  # noqa: S310 - pinned GitHub URL
        data = response.read(MAX_ARCHIVE_BYTES + 1)
    if len(data) > MAX_ARCHIVE_BYTES:
        raise RulePackError("Rule pack archive is larger than expected")
    return data


def fetch_pack(tag: str = DEFAULT_RELEASE, sha256: str = DEFAULT_SHA256, *, repo: str = RULES_REPO,
               force: bool = False, timeout: float = DOWNLOAD_TIMEOUT) -> RulePack:
    """Download, verify and unpack a release into the cache; return the pack."""
    if not force:
        cached = _cached_pack(tag, sha256)
        if cached is not None:
            return cached
    url = archive_url(tag, repo)
    logger.info("Downloading rule pack %s from %s", tag, url)
    try:
        data = _download(url, timeout)
    except RulePackError:
        raise
    except Exception as exc:  # noqa: BLE001
        raise RulePackError(f"Could not download the rule pack {tag} from {url}: {exc}") from exc
    actual = hashlib.sha256(data).hexdigest()
    if actual != sha256:
        raise RulePackError(
            f"Rule pack {tag} does not match its pinned SHA-256 (expected {sha256}, got {actual}); "
            "nothing was unpacked"
        )
    root_dir = cache_root()
    root_dir.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{tag}-", dir=root_dir))
    try:
        with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as archive:
            members = list(_safe_members(archive))
            extra = {"filter": "data"} if hasattr(tarfile, "data_filter") else {}
            for member in members:
                archive.extract(member, staging, set_attrs=False, **extra)
        tops = {PurePosixPath(m.name).parts[0] for m in members if PurePosixPath(m.name).parts}
        top = tops.pop() if len(tops) == 1 else ""
        (staging / MARKER).write_text(
            json.dumps({"repo": repo, "tag": tag, "sha256": sha256, "root": top}), "utf-8"
        )
        target = root_dir / tag
        if target.exists():
            shutil.rmtree(target)
        os.replace(staging, target)
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return _pack_from_dir(target / top, f"download:{url}", tag)


def resolve_pack(*, rules_dir: Optional[str] = None, release: Optional[str] = None,
                 sha256: Optional[str] = None, offline: bool = False,
                 allow_installed: bool = True) -> Optional[RulePack]:
    """Resolve the default rule pack following the documented source order."""
    rules_dir = os.environ.get("METACRAFTER_RULES_DIR") or rules_dir
    release = os.environ.get("METACRAFTER_RULES_RELEASE") or release
    sha256 = os.environ.get("METACRAFTER_RULES_SHA256") or sha256
    env_offline = _env_flag("METACRAFTER_RULES_OFFLINE")
    offline = env_offline if env_offline is not None else offline

    if rules_dir:
        return _pack_from_dir(Path(rules_dir), f"path:{rules_dir}")
    if release and release != DEFAULT_RELEASE and not sha256:
        raise RulePackError(
            f"rules_release {release!r} needs rules_sha256: the SHA-256 of "
            f"{archive_url(release)} (unpinned rule packs are not downloaded)"
        )
    tag = release or DEFAULT_RELEASE
    digest = sha256 or DEFAULT_SHA256
    if allow_installed and not release:
        installed = _installed_pack()
        if installed is not None:
            return installed
    cached = _cached_pack(tag, digest)
    if cached is not None:
        return cached
    if offline:
        return None
    return fetch_pack(tag, digest)


def activate(pack: RulePack) -> None:
    """Make the pack's ``metacrafterext`` validators importable."""
    if pack.python_path is None:
        return
    path = str(pack.python_path)
    if path not in sys.path:
        sys.path.insert(0, path)
