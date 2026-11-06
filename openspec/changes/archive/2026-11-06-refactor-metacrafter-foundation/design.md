## Context

Three generations of plans touch the same foundation:
- the July 2026 ecosystem roadmap (`openspec/ROADMAP.md`);
- the October 2026 Codex repository review (`REPOSITORY_REVIEW_AND_IMPROVEMENT_PLAN.md`
  on branch `codex/repository-review-2026-10-01`);
- the November 2026 registry-driven plan (`devdocs/MODERNIZATION_PLAN.md`).

This design fixes the decisions that the later phases rely on.

## Goals / Non-Goals

**Goals:**
- One supported runtime baseline shared with `metacrafter-registry` (Python ≥3.10).
- One source of rule content (`metacrafter-rules`), resolved independently of the CWD.
- A minimal core install; heavy or service-specific dependencies live in extras.
- A target module layout that later phases can fill without another rename.

**Non-Goals:**
- Extracting `CrafterCmd` internals (done in `refactor-scan-service-and-api`).
- Changing rule semantics (done in `update-rule-loading-and-packaging` and
  `update-detection-engine`).
- Adding the registry dependency (done in `add-registry-integration`).

## Decisions

### Decision: `metacrafter-rules` is a required dependency

The in-repository `rules/` pack is an outdated subset (140 rules) of `metacrafter-rules`
(1267 rules). Every key in it exists in `metacrafter-rules`, and its validators already
import `metacrafterext`. Keeping it causes shadowing and wheel-content bugs.

Effective rulepath order:
1. the `metacrafter-rules` pack from `metacrafterext.rules.rules_dir()`, unless
   `auto_rules: false`;
2. user paths from `.metacrafter` `rulepath`;
3. CLI `--rulepath` values.

Later entries override earlier ones on duplicate rule ids. The override policy itself is
defined in `update-rule-loading-and-packaging`.

**Alternatives considered:**

| Option | Verdict |
|---|---|
| Vendor YAML as package data (`metacrafter-3` original approach) | Rejected: two copies of the same rules drift again |
| Minimal core pack inside metacrafter, loaded after the extended pack | Rejected: still two sources of truth |
| **Required dependency (chosen)** | Adopted: one source, independent release cadence kept |

### Decision: Extras for sources and server

| Extra | Packages | Used by |
|---|---|---|
| `mongodb` | pymongo | `scan mongodb` |
| `duckdb` | duckdb, duckdb-engine | native DuckDB path, `duckdb://` URLs |
| `server` | HTTP framework and ASGI server (see `add-agent-interfaces`) | `server run` |
| `mcp` | MCP SDK (see `add-agent-interfaces`) | `mcp` |
| `llm` | unchanged | LLM modes |

SQLAlchemy stays in core because `scan sql` is a primary use case. Command entry points
import optional modules lazily and raise `MissingExtraError` naming the extra.

### Decision: Target package layout

```
metacrafter/
  registry/   rules/   engine/   sources/   report/   exporters/   llm/   interfaces/
```

`metacrafter.CrafterCmd`, `metacrafter.core.CrafterCmd` and the exception classes stay
importable. New modules are created empty with `__init__.py` docstrings only when a later
change populates them. This change documents the layout; it does not move code.

## Risks / Trade-offs

- Users who installed `metacrafter` and scan MongoDB or DuckDB must add an extra. Mitigation:
  explicit error message, CHANGELOG entry, migration guide.
- Removing `rules/` changes results for users running from a checkout. In practice they get
  the newer rules. Mitigation: release note listing the rule-pack version used.
- Python 3.8/3.9 users must stay on 0.0.x. Mitigation: keep 0.0.x installable; no yanking.

## Migration Plan

1. Land `update-hermetic-quality-gates` so the test baseline is trustworthy.
2. Apply this change in one release (0.1.0) with a migration note.
3. Archive `metacrafter-llm` after 0.1.0 ships.
