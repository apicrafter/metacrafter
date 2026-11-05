# Change: Modernize the Metacrafter runtime foundation

## Why

Metacrafter 0.0.6 still declares Python 3.8 support, uses the pydantic v1 validator API
on top of pydantic v2, installs database drivers and Flask for every user, and keeps a
stale in-repository `rules/` pack. That pack is not shipped in wheels. When Metacrafter
runs from a checkout, the pack shadows newer rules from `metacrafter-rules`: 95 of 138
shared rules diverge, and loading logs 139 duplicate-key warnings.
`metacrafter-registry` already requires Python 3.10.

The registry-driven modernization (see `devdocs/MODERNIZATION_PLAN.md`) needs a clean
baseline first. This change is phase 0 of that plan.

Decisions accepted on 2026-11-05 (plan section 6):
- `metacrafter-rules` becomes a required dependency, and the in-repository `rules/`
  directory is removed (decision 2).
- The minimum Python version becomes 3.10. 1.0.0 will be a breaking release (decision 4).
- The `metacrafter-llm` repository is archived (decision 6).

## What Changes

- **BREAKING** Require Python `>=3.10`; CI matrix 3.10–3.13.
- Migrate configuration models to the pydantic v2 API (`field_validator`, `model_config`).
- **BREAKING** Move source drivers and the server out of core dependencies into extras:
  `mongodb` (pymongo), `duckdb` (duckdb, duckdb-engine), `server` (HTTP stack). Missing
  extras produce an actionable error that names the extra.
- Remove `requirements.txt` as a second dependency list. Generate it from `pyproject.toml`
  if any tooling still needs it.
- **BREAKING** Make `metacrafter-rules` a required dependency. Delete the in-repository
  `rules/` directory. Resolve the default rule pack from the installed package, never from
  the current working directory. Keep the `rules` extra as an empty compatibility alias.
- Establish the target package layout (`registry/`, `rules/`, `engine/`, `sources/`,
  `report/`, `exporters/`, `llm/`, `interfaces/`) behind a stable `CrafterCmd` facade.
  Module extraction is carried out under `refactor-scan-service-and-api`.
- Archive the `metacrafter-llm` repository with a pointer to `metacrafter[llm]`.
- Reconcile `openspec/ROADMAP.md` with the current change set.

## Relationship to other changes

- **Supersedes** the `Packaged defaults` requirement of `update-rule-loading-and-packaging`
  (Codex review, 2026-10-01). The default pack now comes from the `metacrafter-rules`
  dependency, not from YAML vendored into this package. That change's other requirements
  (deterministic validated rules, restricted pattern expressions) still apply.
- **Supersedes** the built-in-first ordering in `add-rules-optional-extra`. The `rules_dir()`
  API and the `auto_rules` flag are kept.
- **Depends on** `update-hermetic-quality-gates` for the deterministic test baseline.
- **Coordinates with** `refactor-scan-service-and-api`, which performs the `core.py`
  extraction into the layout defined here.
- Absorbs the open items of the July `modernize-metacrafter-packaging` change: extras and
  the Python matrix.

## Impact

- Affected specs: `packaging`, `rule-loading`
- Affected code: `pyproject.toml`, `requirements.txt`, `metacrafter/config.py`,
  `metacrafter/classify/processor.py` (rule source resolution), `rules/` (deleted),
  `.github/workflows/ci.yml`, docs (installation, rules, architecture)
- Cross-repo: `metacrafter-rules` must release a version with `rules_dir()` (≥0.0.2,
  already merged) on PyPI. `metacrafter-llm` is archived.
- Beads: `metacrafter-3` (approach changed), `metacrafter-10`, `metacrafter-11`
