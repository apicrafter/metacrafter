# Change: Unified registry resolution

> **Status (2026-11-05): superseded** by `add-registry-integration`. Do not implement this change; see `openspec/ROADMAP.md`.

## Why

The repository review identified actionable reliability gaps and product opportunities (W11; Q17). The user requested creation and implementation of the plan.

## What Changes

- Centralize registry URL resolution, cached response validation and offline-safe links.
- Add registry override/status/cache tests and refresh SDK/CLI configuration documentation.

## Impact

- Affected spec: `registry-resolution`.
- Source: `REPOSITORY_REVIEW_AND_IMPROVEMENT_PLAN.md` (W11; Q17).
- Existing SDK and CLI entry points remain supported; privacy and validation failures become explicit.
