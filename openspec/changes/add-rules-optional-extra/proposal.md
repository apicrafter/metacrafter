# Change: Optional `metacrafter-rules` Extra and Auto Rulepath

## Why

`metacrafter-rules` is the de facto full ruleset (~214 YAML files, 40+ locales) but
today requires a separate install and manual `rulepath` configuration. Users who run
`pip install metacrafter` get only the small built-in rule pack (~35 files) with no
obvious path to the extended rules. A repo merge was considered and rejected; this
change delivers most of the onboarding benefit while preserving the extension-pack
architecture and independent release cadence of `metacrafter-rules`.

## What Changes

- Add `metacrafter[rules]` optional extra that depends on the `metacrafter-rules`
  PyPI package (separate installable, not vendored into `metacrafter`)
- When `metacrafter-rules` is importable, automatically append its bundled `rules/`
  directory to the effective rulepath (after built-in `rules/`, before user overrides)
- Add `metacrafterext.rules.rules_dir()` (or equivalent) in **metacrafter-rules** so
  the engine can resolve the installed rules location portably (wheel, editable, sdist)
- Ensure `metacrafter-rules` ships YAML rule files in the wheel (`package-data`)
- Include `rules` in the `metacrafter[all]` extra
- Document install paths: `pip install metacrafter` (minimal) vs
  `pip install 'metacrafter[rules]'` (full ruleset)
- Add tests for rulepath resolution with and without the extra installed

## Non-Goals

- Merging `metacrafter-rules` into the `metacrafter` git repository or Python package
- Changing the `rulepath` override mechanism or custom-rule plugin model
- Pinning metacrafter and metacrafter-rules to identical version numbers (use a
  minimum compatible version range instead)
- Bundling `metacrafter-registry` data

## Impact

- Affected specs: `packaging`, `rule-loading`
- Affected code (metacrafter): `pyproject.toml`, `metacrafter/config.py`, README
- Affected code (metacrafter-rules): `setup.py` or `pyproject.toml`, `metacrafterext/rules/__init__.py`
- Roadmap: complements Phase 3 `modernize-metacrafter-packaging`; does not replace
  `add-ecosystem-alignment-workflow`
- Cross-repo: coordinated release when `rules_dir()` API lands in metacrafter-rules
