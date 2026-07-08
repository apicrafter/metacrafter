## Context

Metacrafter deliberately separates the scan engine (`metacrafter`) from extended
rule content (`metacrafter-rules`) and datatype metadata (`metacrafter-registry`).
Users extend classification via `rulepath` directories and importable `func` validators
(`metacrafterext.*`). Cross-repo alignment is handled by `scripts/ecosystem_check.py`.

The pain point is discoverability: after `pip install metacrafter`, users do not
automatically get the extended rules unless they also install `metacrafter-rules` and
configure `rulepath` manually.

## Goals / Non-Goals

**Goals:**

- One documented install command for the full ruleset: `pip install 'metacrafter[rules]'`
- Zero manual `rulepath` editing when the extra is installed
- Preserve ability to run with built-in rules only (no extra, no auto-append)
- Portable rules directory resolution across install modes

**Non-Goals:**

- Monorepo merge or vendoring YAML into `metacrafter`
- Breaking existing `.metacrafter` configs that set explicit `rulepath`
- Auto-installing rules when the extra is not requested

## Decisions

### Decision: Keep `metacrafter-rules` as a separate PyPI package

`metacrafter[rules]` declares `metacrafter-rules>=X.Y` as an optional dependency.
The extended rules remain versioned and released independently.

**Alternatives considered:**

| Option | Pros | Cons | Verdict |
|--------|------|------|---------|
| Full repo merge | Atomic commits | Blurs engine/content; large repo; weakens extension model | Rejected |
| Git monorepo, separate packages | Atomic git, separate PyPI | Migration cost; still 3 packages for registry | Deferred |
| **`[rules]` extra (chosen)** | Simple UX; no merge | Two packages to release | **Adopt** |

### Decision: Opt-in auto-append via import detection

`ConfigLoader.get_rulepath()` appends the extended rules directory when
`import metacrafterext.rules` succeeds **and** `rules_dir()` returns an existing path.

Precedence for the effective rulepath:

1. Built-in `rules/` (package-relative, always first)
2. Auto-discovered `metacrafter-rules` `rules/` (when installed)
3. Paths from `.metacrafter` / `--rulepath` (appended; user paths win on duplicate `key`)

If the user sets an explicit `rulepath` in config, auto-append still applies unless
`auto_rules: false` is set (new optional config flag, default `true`).

**Rationale:** Users who install `[rules]` expect it to "just work". Users with
custom-only rulepaths can disable auto-append.

### Decision: `rules_dir()` API in metacrafter-rules

```python
# metacrafterext/rules/__init__.py
def rules_dir() -> Path:
    """Return the bundled rules/ directory shipped with this package."""
```

Resolved relative to `metacrafterext/rules/__file__` (sibling `../../rules` from
package root). Uses `importlib.resources` or `Path(__file__).resolve().parent` pattern
consistent with existing packaging.

**metacrafter-rules packaging fix required:** `include_package_data=True` alone does
not ship the top-level `rules/` tree. Add explicit `package-data` / `MANIFEST.in` so
wheels contain YAML files.

### Decision: Version constraint

`metacrafter[rules]` depends on `metacrafter-rules>=0.0.2` (first release with
`rules_dir()` and wheel package-data). Bump floor when the API changes.

No upper bound; ecosystem-check CI catches ID drift.

## Risks / Trade-offs

| Risk | Mitigation |
|------|------------|
| Rules missing from wheel | CI test: after `pip install metacrafter-rules`, `rules_dir()` exists and contains YAML |
| Duplicate keys between built-in and extended rules | Existing dedup-by-key behavior; extended rules load second |
| User confusion: installed extra but `auto_rules: false` | Log at INFO when extended rules are discovered but skipped |
| Version skew between engine and rules | Document minimum version; ecosystem-check on schedule |

## Migration Plan

1. Release **metacrafter-rules** with `rules_dir()` + wheel package-data (no breaking changes)
2. Release **metacrafter** with `[rules]` extra and auto-append logic
3. Update README install section and `.metacrafter` example
4. Add `auto_rules` to config schema (optional, default `true`)

**Rollback:** Remove auto-append block in `ConfigLoader`; `[rules]` extra is inert if
unused. No data migration.

## Open Questions

- Should `metacrafter[all]` include `rules`? **Proposed: yes.**
- Publish `metacrafter-rules` to PyPI if not already? **Verify before implementation.**
- Log level when auto-append activates: DEBUG or INFO?
