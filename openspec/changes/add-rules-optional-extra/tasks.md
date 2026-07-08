## 1. metacrafter-rules (prerequisite)

- [x] 1.1 Add `rules_dir() -> Path` to `metacrafterext/rules/__init__.py`
- [x] 1.2 Configure wheel/sdist to include top-level `rules/` YAML tree (`package-data` or `MANIFEST.in`)
- [x] 1.3 Add unit test: `rules_dir()` exists and contains at least one `.yaml` after install
- [ ] 1.4 Release or tag metacrafter-rules with the new API (coordinate version floor)

## 2. metacrafter packaging

- [x] 2.1 Add `[project.optional-dependencies] rules = ["metacrafter-rules>=0.0.2"]`
- [x] 2.2 Add `metacrafter-rules` to the `[all]` extra list
- [x] 2.3 Document extras in README: `pip install 'metacrafter[rules]'`

## 3. Auto rulepath resolution

- [x] 3.1 Add `discover_extended_rules_path() -> Optional[str]` in `metacrafter/config.py`
- [x] 3.2 Extend `MetacrafterConfig` with `auto_rules: bool = True`
- [x] 3.3 Update `ConfigLoader.get_rulepath()` to append discovered path when `auto_rules` is true
- [x] 3.4 Log at INFO when extended rules path is auto-appended (include resolved path)
- [x] 3.5 Ensure `--rulepath` CLI override composes correctly (user paths after auto-discovered)

## 4. Tests

- [x] 4.1 Test: without metacrafter-rules installed, rulepath is `["rules"]` only
- [x] 4.2 Test: with metacrafter-rules installed (mock or optional dep), extended path appended
- [x] 4.3 Test: `auto_rules: false` in config skips auto-append
- [x] 4.4 Test: duplicate rule keys between built-in and extended do not crash (warning only)

## 5. Documentation

- [x] 5.1 README: install matrix (minimal / rules / all)
- [x] 5.2 README: update "Custom rules and validator plugins" with auto-discovery behavior
- [x] 5.3 Example `.metacrafter` snippet showing `auto_rules: false` for custom-only setups
- [x] 5.4 Add roadmap entry under Phase 3 in `openspec/ROADMAP.md`
