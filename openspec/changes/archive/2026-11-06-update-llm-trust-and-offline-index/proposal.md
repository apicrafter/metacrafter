# Change: LLM reliability, privacy and offline retrieval

## Why

The repository review identified actionable reliability gaps and product opportunities (W08; Q12, Q13, Q14). The user requested creation and implementation of the plan.

## What Changes

- Validate provider results, field selection, thresholds, modes and fallback; preserve PII and evidence.
- Add sample policies, secret redaction, request budgets and timeouts.
- Implement local embeddings, manifest validation and atomic replacement with metadata filtering.
- Add fake-provider/index tests and document offline configuration and privacy defaults.

## Impact

- Affected spec: `llm-classification`.
- Source: `REPOSITORY_REVIEW_AND_IMPROVEMENT_PLAN.md` (W08; Q12, Q13, Q14).
- Existing SDK and CLI entry points remain supported; privacy and validation failures become explicit.
