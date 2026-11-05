# Change: Rules from GitHub releases, registry through its website

## Why

Accepted on 2026-11-05: neither `metacrafter-rules` nor `metacrafter-registry` will be
published on PyPI.
- The registry is a website and data service. Metacrafter talks to it only through the
  published `/api/v1/` files.
- The rule pack is distributed as a GitHub release of `apicrafter/metacrafter-rules`.

1.0.0.dev0 made `metacrafter-rules` a required Python dependency. It also used the
`metacrafter_registry` package for checksums, value identification and crosswalks. Both
block a PyPI release of metacrafter and are replaced here.

## What Changes

- **BREAKING (packaging)** Remove `metacrafter-rules` from the dependencies and drop the
  `registry` extra. The `rules` extra stays as an empty alias.
- **Rule pack from a GitHub release.** On first use, metacrafter downloads the release
  archive pinned in the package (tag and SHA-256) from GitHub. It verifies the hash, unpacks
  the archive safely into `~/.cache/metacrafter/rules/<tag>/` and loads `rules/`. The
  pack's Python validators (`metacrafterext`) become importable only from a verified archive.
- **Rule pack sources**, in order:
  1. `rules_dir` (a local checkout or unpacked pack);
  2. an installed `metacrafterext` package;
  3. the cache;
  4. a download (unless `rules_offline`).

  `rules_release` with `rules_sha256` selects another release; a release without a hash is
  refused.
- **Registry through its website only.**
  - The adapter loads `/api/v1/` snapshots (download with SHA-256 checks, cache, or a local
    snapshot directory) and no longer imports `metacrafter_registry`.
  - Checksum algorithms are vendored from the registry (`metacrafter/engine/checksum_algorithms.py`)
    so record `checksum` names keep working.
  - Value identification, search and the qddate crosswalk are implemented in the adapter.
- **New commands.** `metacrafter rules fetch` downloads or shows the rule pack.
  `metacrafter registry fetch --to DIR` writes a registry snapshot for offline use.
- **CI.**
  - Jobs fetch the rule release and the registry snapshot with these commands instead of
    installing packages from GitHub.
  - The ecosystem gate is blocking while the live registry matches the version recorded in
    `ecosystem.lock`. When the registry has moved on, the gate only reports, and the weekly
    workflow opens the drift issue.

## Relationship to other changes

- **Supersedes** in `refactor-metacrafter-foundation`: `Required Rules Package` and the
  package-based part of `Package-Resolved Default Rule Pack`.
- **Supersedes** step 5 (the registry package snapshot) of `Registry Source Resolution` in
  `add-registry-integration`, and checksum validators backed by the registry package in
  `update-detection-engine`.
- **Supersedes** the PyPI publication follow-ups (Beads `metacrafter-2u6`).

## Impact

- Affected specs: `ecosystem-distribution`
- Affected code:
  - `metacrafter/config.py`;
  - new `metacrafter/rules/pack.py`;
  - `metacrafter/registry/adapter.py`;
  - `metacrafter/engine/checksums.py` and new `checksum_algorithms.py`;
  - `metacrafter/classify/llm/registry_classifier.py`;
  - CLI `rules fetch` and `registry fetch`;
  - `pyproject.toml`, CI workflows, `ecosystem.lock`, docs.
- **Network:** the first scan downloads the rule pack (about 280 KB) and registry data
  unless local paths or the offline settings are used.
