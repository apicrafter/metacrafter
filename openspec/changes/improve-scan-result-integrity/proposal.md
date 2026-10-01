# Change: Improve scan result integrity

## Why

The 2026-10-01 repository review reproduced mislabeled SDK statistics, discarded valid field shapes, threshold-boundary errors, PII metadata loss, incompatible scan-file/exporter formats, and export success counts after failed writes. These affect the correctness of existing user workflows and are prerequisites for report diffing and PII inventories.

See [the repository review](../../../REPOSITORY_REVIEW_AND_IMPROVEMENT_PLAN.md), findings Q01, Q02, Q05–Q10 and work packages W01, W04 and W06. Beads issues: `metacrafter-2`, `metacrafter-5`, `metacrafter-7`.

## What Changes

- Define a shared named statistics mapping and report interchange contract while retaining legacy dictionary readers/public imports.
- Preserve valid field coverage, document scalar-array/path behavior and validate missing-field requests.
- Apply inclusive finite confidence thresholds and propagate normalized PII metadata.
- Make serialized reports directly consumable by exporters and aggregate bulk output into valid documents.
- Report confirmed catalog write outcomes and incomplete scan/export results accurately.
- Add synthetic negative and round-trip regression tests across SDK, API and writers/exporters.

## Impact

- Affected specs: `scan-results`, `catalog-export`.
- Affected code: statistics/flattening, processor, core scan/result serialization, server/API, catalog exporters and tests.
- Coordinates with `modernize-metacrafter-packaging` typed results, `refactor-metacrafter-core` shared services and `expand-metacrafter-llm-platform` PII enrichment. Their existing untracked proposals are not rewritten by this change.
- Changes confidence boundary behavior and expands field coverage; document these corrections. Report aliases/readers provide compatibility for current consumers.
- Status: draft for review. Production implementation has not started.
