## Context

Analyzer returns 19 positional statistics values; SDK scan_data maps only 14 headers while the API separately maps 19. Saved reports rename data to fields, but exporters expect data. Rule, LLM and exporter paths independently infer PII and confidence. Existing public behavior must remain readable during a shared-contract migration.

## Goals / Non-Goals

- Goals: consistent statistics, complete documented field coverage, explicit confidence/PII metadata, usable report interchange and truthful export outcomes.
- Non-goals: full core extraction, new database connectors, streaming/reservoir sampling, a dashboard, new country packs or a new catalog platform.

## Decisions

- Introduce one named mapping/model at the Analyzer boundary. Retain the existing table output through an adapter so consumers do not need an immediate migration.
- Version the report boundary and retain readers for legacy data/fields forms. Select one canonical authoring form when representative consumers/fixtures have been reviewed; do not change public SDK return types incidentally.
- Use a shared field walker with documented array and collision semantics. Missing requested fields produce a validation outcome; they must not cause an internal KeyError.
- Separate evidence from display. Normalize booleans, use inclusive finite minimum thresholds and propagate authoritative PII metadata. Where metadata is unavailable, retain explicit unknown/degraded evidence instead of inventing safety claims.
- Export helpers return operation outcomes or raise a domain error; the aggregator records partial failures. Counts reflect confirmed remote outcomes, and preserved existing metadata remains part of adapter-specific contract tests.

## Risks / Trade-offs

- Additional field coverage and inclusive thresholds change reports → capture before/after fixtures and document the correction.
- Legacy consumers depend on report keys → use versioning and compatibility readers rather than silent renaming.
- Catalog APIs have different success/idempotency semantics → test each supported adapter contract separately; mocked transport alone does not certify live compatibility.
- Required-validator failure changes permissive loading → expose the failure and provide an explicitly configured permissive/degraded policy only if needed.

## Migration Plan

1. Establish fixtures and canonical/legacy read contracts.
2. Correct named statistics and field coverage with narrow regressions.
3. Normalize classification metadata and correct threshold boundaries.
4. Repair writer/exporter interchange and partial-operation outcomes.
5. Release with compatibility documentation, then continue structural extraction separately.

## Open Questions

- Which report form should be canonical for new serialized output, given current SDK and file consumers?
- Should scalar arrays be classified as element samples, array fields with element metadata, or both?
- What escaping/versioning scheme avoids collisions between literal dotted keys and nested paths?
- Which partial-result CLI exit code should be stable across scan/export commands?
