# Change: Ship a registry snapshot in the package and bound registry downloads

## Why

The first scan on a machine (or in a fresh CI job, Docker image or virtualenv) blocks while
`RegistryAdapter.load()` downloads the registry snapshot from `registry.apicrafter.io`.
`datatypes.jsonl` is 3.1 MB and is sent uncompressed: `_http_get` sends no
`Accept-Encoding`, although the server (GitHub Pages behind Fastly) serves gzip, 5.6 times
smaller. The 10 s timeout applies to each socket read, so a slow transfer has no overall
limit. Measured on 2026-11-06:

| Path | Time |
|---|---|
| Load and verify a local snapshot (1867 records) | 0.03 s |
| Warm cache (`RegistryAdapter.load`) | 0.02–0.03 s |
| Cold cache, `RegistryAdapter.load` (3 runs, 50–150 KB/s) | 32 s, 45 s, 61 s |
| Cold cache, first `metacrafter scan file` (about 15 KB/s) | 555 s, then `<urlopen error timed out>` and no registry data |
| Cold cache, first `metacrafter scan file` (2 runs, rules cached) | 97 s, 123 s |
| Warm cache, `metacrafter scan file` | 1.6–1.7 s |
| `datatypes.jsonl` plain / gzip | 3,142,847 / 561,169 bytes |

Each of the four requests opens a new TLS connection: the three small files take 2–4 s each.

After `registry_cache_ttl` (24 hours) a scan checks `index.json` again. When the registry
has published a new version, the scan downloads the full snapshot again without an overall
limit. When that download fails, every later scan retries it.

## What Changes

- **Bundled snapshot.** The package includes the registry snapshot that `ecosystem.lock`
  records (`version`, `git_sha`): `metacrafter/registry/bundled/` with `index.json`,
  `aliases.json`, `categories.json` and `datatypes.jsonl.gz` (about 550 KB). It is verified
  against the SHA-256 values in its `index.json`.
- **Cold cache.** When no verified cache exists and the base URL is the default
  `https://registry.apicrafter.io`, the bundled snapshot is copied into the cache and used.
  Nothing is downloaded. The next network check is after `registry_cache_ttl`. A
  self-hosted registry (another base URL) never uses the bundled snapshot.
- **Offline.** `registry_offline` with the default base URL works without
  `metacrafter registry fetch`.
- **Compressed downloads.** Registry requests send `Accept-Encoding: gzip` and decompress
  gzip responses before hash verification.
- **Download deadline.** A refresh during a scan stops after `registry_fetch_timeout`
  seconds in total (default 30). The scan then uses the older cache. A failed refresh marks
  the cache as checked, so the next attempt is after `registry_cache_ttl`, not on every scan.
  `metacrafter registry fetch` has no overall deadline.
- **Release bump.** `scripts/bundle_registry_snapshot.py` downloads the live snapshot,
  verifies it and writes the bundled files. A test fails when the bundled snapshot does not
  match `ecosystem.lock`.

## Impact

- Affected specs: `registry-integration`
- Affected code:
  - `metacrafter/registry/adapter.py`, `metacrafter/config.py`, `metacrafter/scan_service.py`;
  - new `metacrafter/registry/bundled/` and `scripts/bundle_registry_snapshot.py`;
  - `pyproject.toml` (package data);
  - `tests/test_registry_adapter.py`;
  - docs (registry, installation, ecosystem), CHANGELOG, AGENTS.md.
- **Size:** the wheel and sdist grow by about 550 KB.
- **Data age:** a new installation starts with the registry version of its metacrafter
  release and moves to the live registry after `registry_cache_ttl`. Reports already state
  the registry version in `registry.version`.
- **Network:** the first scan with the default registry needs no network access.
