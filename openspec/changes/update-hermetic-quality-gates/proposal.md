# Change: Hermetic tests and enforced quality gates

## Why

The repository review identified actionable reliability gaps and product opportunities (W09; Q15). The user requested creation and implementation of the plan.

## What Changes

- Isolate configuration and optional integration tests; repair swallowed assertions and registry discovery.
- Enforce lint, scoped format/types, measured coverage and wheel smoke checks.
- Run the suite and document measured coverage and supported verification.

## Impact

- Affected spec: `quality-gates`.
- Source: `REPOSITORY_REVIEW_AND_IMPROVEMENT_PLAN.md` (W09; Q15).
- Existing SDK and CLI entry points remain supported; privacy and validation failures become explicit.
