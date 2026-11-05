## 1. Runtime baseline

- [x] 1.1 Set `requires-python = ">=3.10"` and update classifiers in `pyproject.toml`
- [x] 1.2 Update CI matrix to 3.10, 3.11, 3.12, 3.13
- [x] 1.3 Replace `@validator` with `@field_validator` and class `Config` with `model_config` in `metacrafter/config.py`
- [x] 1.4 Remove `from __future__` and typing shims that only served Python <3.10

## 2. Dependencies and extras

- [x] 2.1 Move `pymongo` to extra `mongodb`; `duckdb`, `duckdb-engine` to extra `duckdb`; `flask` to extra `server`
- [x] 2.2 Add `MissingExtraError` and lazy imports in source and server entry points
- [x] 2.3 Declare every directly imported runtime package (`requests`, `chardet`) in core dependencies
- [x] 2.4 Delete `requirements.txt` or generate it from `pyproject.toml`; make CI install from `pyproject.toml` extras
- [x] 2.5 Update `all` extra to include the new extras

## 3. Rule source

- [x] 3.1 Add `metacrafter-rules>=0.0.2` to core dependencies; keep `rules` extra as an empty alias
- [x] 3.2 Resolve the default pack via `metacrafterext.rules.rules_dir()`; drop `DEFAULT_RULEPATH = ["rules"]`
- [x] 3.3 Apply the rulepath order from design.md; keep `auto_rules: false` behaviour
- [x] 3.4 Fail with a clear error when no rule directory resolves and no user path is configured
- [x] 3.5 Delete `rules/` (the bundled-only rule `entitleknown` is listed as a follow-up for `metacrafter-rules`)
- [x] 3.6 Test: wheel installed into a clean venv scans an email/UUID CSV from an unrelated directory

## 4. Layout and documentation

- [ ] 4.1 Document the target package layout in `docs/docs/development/architecture.md`
- [x] 4.2 Update installation docs and README for extras and Python ≥3.10
- [ ] 4.3 Add a migration note to `CHANGELOG.md` (extras, removed `rules/`, Python floor)
- [ ] 4.4 Rewrite `openspec/ROADMAP.md` to list current changes and their status

## 5. Ecosystem

- [ ] 5.1 Archive the `metacrafter-llm` repository with a README pointer to `metacrafter[llm]` (owner action, blocked)
- [x] 5.2 Comment on Beads `metacrafter-3` that the packaging approach changed to a required dependency
