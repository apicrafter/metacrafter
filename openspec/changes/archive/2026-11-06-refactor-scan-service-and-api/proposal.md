# Change: Shared scan service and API parity

## Why

The repository review identified actionable reliability gaps and product opportunities (W05, W10; Q08, Q16). The user requested creation and implementation of the plan.

## What Changes

- Replace core.py/package import collision with a normal command module and compatibility exports.
- Route Flask scans through the shared SDK service with validated options, limits, auth and health.
- Test SDK/API parity, initialization count and invalid requests; update service documentation.

## Impact

- Affected spec: `scan-service`.
- Source: `REPOSITORY_REVIEW_AND_IMPROVEMENT_PLAN.md` (W05, W10; Q08, Q16).
- Existing SDK and CLI entry points remain supported; privacy and validation failures become explicit.
