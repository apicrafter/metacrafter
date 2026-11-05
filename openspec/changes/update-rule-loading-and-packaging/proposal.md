# Change: Rules and standalone installation

> **Status (2026-11-05):** the `Packaged defaults` requirement is superseded by `refactor-metacrafter-foundation`: the default rules come from the required `metacrafter-rules` dependency, not from YAML vendored into this package. The other requirements stay in force.

## Why

The repository review identified actionable reliability gaps and product opportunities (W02, W06; Q03, Q09, Q10). The user requested creation and implementation of the plan.

## What Changes

- Bundle rule resources and declare directly imported runtime dependencies.
- Validate YAML and AST expressions; implement deterministic precedence, PII and inclusive thresholds.
- Add isolated installation and negative rule tests; document trusted plugins and precedence.

## Impact

- Affected spec: `rule-loading`.
- Source: `REPOSITORY_REVIEW_AND_IMPROVEMENT_PLAN.md` (W02, W06; Q03, Q09, Q10).
- Existing SDK and CLI entry points remain supported; privacy and validation failures become explicit.
