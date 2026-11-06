## 1. Bundled rule pack

- [x] 1.1 Add `metacrafter/rules/bundled/metacrafter-rules-v0.0.2.tar.gz` (the pinned archive bytes)
- [x] 1.2 `pack.py`: unpack the bundled archive on a cache miss, verified with the pinned SHA-256; share the install path with downloads
- [x] 1.3 Source order: `rules_dir`, installed `metacrafterext`, cache, bundled archive, download; `rules_offline` uses the bundled archive
- [x] 1.4 `rules fetch`: bundled archive on a cache miss; `--force` and `--release` download
- [x] 1.5 Include the archive as package data in the wheel and sdist
- [x] 1.6 Add `scripts/bundle_rule_pack.py` for release bumps

## 2. Tests, CI and docs

- [x] 2.1 Tests: bundled archive matches `DEFAULT_SHA256` and `ecosystem.lock`; cache miss resolves without network; offline resolves; `--force` downloads
- [x] 2.2 Drop `metacrafter rules fetch` from CI test jobs
- [x] 2.3 Update installation and ecosystem docs, CHANGELOG, AGENTS.md
