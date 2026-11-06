# Change: SQL correctness and bounded sampling

## Why

The repository review identified actionable reliability gaps and product opportunities (W03, W07; Q04, Q11). The user requested creation and implementation of the plan.

## What Changes

- Use SQLAlchemy reflected select/limit and schema-aware DuckDB queries with resource cleanup.
- Implement bounded head/reservoir sampling and provenance across file, SDK and database reports.
- Add SQLite identifier, sampling determinism and failure tests; document statistics scope.

## Impact

- Affected spec: `source-scanning`.
- Source: `REPOSITORY_REVIEW_AND_IMPROVEMENT_PLAN.md` (W03, W07; Q04, Q11).
- Existing SDK and CLI entry points remain supported; privacy and validation failures become explicit.
