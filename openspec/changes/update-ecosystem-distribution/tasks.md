## 1. Rule pack from GitHub releases

- [x] 1.1 Add `metacrafter/rules/pack.py`: pinned release (tag, URL, SHA-256), cache layout, verified download, safe extraction
- [x] 1.2 Resolve the default pack: `rules_dir`, installed `metacrafterext`, cache, download; `rules_offline`; refuse unpinned releases
- [x] 1.3 Make `metacrafterext` validators importable from a verified pack
- [x] 1.4 Add `metacrafter rules fetch [--release TAG --sha256 HEX]`
- [x] 1.5 Remove `metacrafter-rules` from dependencies; keep `rules` as an empty extra

## 2. Registry through its website

- [x] 2.1 Vendor the registry checksum algorithms; `checksum:` validators and registry rules use them
- [x] 2.2 Remove `metacrafter_registry` imports from the adapter; snapshot directories only
- [x] 2.3 Implement identify, search and the qddate crosswalk in the adapter; use them in the LLM shortlist
- [x] 2.4 Add `metacrafter registry fetch --to DIR`; drop the `registry` extra

## 3. CI, tests and docs

- [x] 3.1 Tests resolve the rule pack once per session (no package install, no network in tests)
- [x] 3.2 CI fetches the rule release and the registry snapshot with metacrafter commands
- [x] 3.3 Ecosystem gate: blocking when the live registry matches `ecosystem.lock`, report-only otherwise
- [x] 3.4 Update installation, rules, registry, ecosystem and migration docs, CHANGELOG, ROADMAP, AGENTS.md
