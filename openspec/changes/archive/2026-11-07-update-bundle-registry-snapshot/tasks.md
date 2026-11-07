## 1. Bundled snapshot

- [x] 1.1 Add `metacrafter/registry/bundled/` (`index.json`, `aliases.json`, `categories.json`, `datatypes.jsonl.gz`) for the registry version in `ecosystem.lock`
- [x] 1.2 `adapter.py`: on a missing or unverified cache with the default base URL, verify the bundled snapshot and copy it into the cache; source `bundled`
- [x] 1.3 Include the bundled files as package data in the wheel and sdist
- [x] 1.4 Add `scripts/bundle_registry_snapshot.py` for registry baseline bumps

## 2. Downloads

- [x] 2.1 `_http_get`: send `Accept-Encoding: gzip`, decompress gzip bodies
- [x] 2.2 `fetch_snapshot`: optional overall deadline; remove the staging directory on failure
- [x] 2.3 `load`: use `registry_fetch_timeout` (default 30 s) as the deadline; on a failed refresh mark the older cache as checked
- [x] 2.4 `registry_fetch_timeout` in `.metacrafter` and `ConfigLoader.get_registry_settings`; `metacrafter registry fetch` without a deadline

## 3. Tests and docs

- [x] 3.1 Tests: bundled snapshot matches `ecosystem.lock` and its hashes; cold cache uses it without network; offline uses it; custom base URL ignores it; gzip body; deadline falls back and is not retried within the TTL
- [x] 3.2 Measure cold-start scan before and after
- [x] 3.3 Update registry, installation and ecosystem docs, CHANGELOG, AGENTS.md
