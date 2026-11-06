# Change: Report, policy and rule tooling

## Why

The repository review identified actionable reliability gaps and product opportunities (F01–F07). The user requested creation and implementation of the plan.

## What Changes

- Add report normalization, diff and PII inventory CLI commands with deterministic JSON.
- Add policy check with violations and stable exit codes.
- Add rules lint/test/draft and scan explain commands with safe examples and evidence.
- Add labeled evaluation and offline dbt manifest dry-run adapter.
- Test commands, compatibility and adversarial inputs; document formats and examples.

## Impact

- Affected spec: `product-tools`.
- Source: `REPOSITORY_REVIEW_AND_IMPROVEMENT_PLAN.md` (F01–F07).
- Existing SDK and CLI entry points remain supported; privacy and validation failures become explicit.
