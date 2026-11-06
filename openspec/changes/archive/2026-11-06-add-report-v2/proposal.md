# Change: Registry-enriched scan report (version 3) and crosswalk exports

> Note (2026-11-05): the Codex review already uses `report_version: 2` for the
> interchange format, so the typed, registry-enriched report defined here is
> `report_version: 3`, a superset of version 2. The change id keeps its original name.

## Why

Scan output is an untyped dictionary. It carries a rule key, a share-based confidence and
a URL; it has no datatype name, category, PII source, evidence, or registry and rule
provenance. Catalog exporters (DataHub, OpenMetadata, Atlas) decide PII from a hard-coded
list of five datatypes (`metacrafter/integrations/datahub.py:242`), while the registry
marks 325 datatypes as PII.

The registry also publishes crosswalks to other classification systems:
- Presidio (60 records), Google DLP (66), DPV (258), Purview (223);
- Schema.org (238), Wikidata (371).

These crosswalks let Metacrafter results feed tools that already use those vocabularies.

This change is phase 3 of `devdocs/MODERNIZATION_PLAN.md`. Accepted decision 4: 1.0.0
switches the default output to report v2. A legacy format stays available for a
transition period.

## What Changes

- **BREAKING (default output in 1.0.0)** Typed report v2 built with pydantic models:
  - `ScanReport` with tool, rules, registry and scoring provenance plus sources;
  - `FieldReport` with statistics, matches and a PII summary;
  - `Match` with datatype, rule key, confidence, `evidence` and a registry reference.
- Publish the report v2 JSON Schema with the documentation. Tag every report with
  `report_format: 2`.
- `--format legacy` produces the pre-1.0 structure for at least two minor releases.
- Catalog exporters take PII, glossary terms and datatype links from the report's registry
  data. They no longer contain lists of datatype ids.
- Crosswalk export: `metacrafter export labels --system presidio|google_dlp|dpv|purview`
  maps report matches to labels of the target system.
- Frictionless Table Schema export with semantic annotations: `rdfType` from Schema.org or
  Wikidata mappings, plus the registry URL.
- Markdown report (`--format md`) for human review and agent consumption.

## Relationship to other changes

- **Depends on** `improve-scan-result-integrity` (Codex W01/W04). That change provides the
  versioned report container, round-trip exporters and confirmed export outcomes. This
  change defines version 2 of the report content.
- **Depends on** `add-registry-integration` (registry references, PII determination) and
  `update-detection-engine` (`evidence`).
- Report diff, PII inventory and policy checks stay in `add-report-and-rule-product-tools`
  (Codex F01, F02, F04). They consume report v2.
- Absorbs the deferred typed-model tasks (3.x) of the July `modernize-metacrafter-packaging`.

## Impact

- Affected specs: `scan-report`, `catalog-export`
- Affected code: new `metacrafter/report/` (models, schema, legacy adapter, markdown);
  `metacrafter/integrations/*` (moved to `metacrafter/exporters/`); CLI `export` and
  `--format`; HTTP API responses; docs
- Breaking for consumers that parse JSON output. Mitigation: `--format legacy`, the
  published schema and a migration guide.
