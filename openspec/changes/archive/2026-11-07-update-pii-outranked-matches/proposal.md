# Change: Value-only PII matches outranked by a named datatype do not mark a field PII

## Why

Values of one datatype can be valid values of another: OKPD2 codes such as `21.20.10.226` are
IPv4 strings (apicrafter/metacrafter#62). Since #58 a field `okpd2` reports `okpd` first, but the
value-only `ipv4` and `ipaddr` matches still carry `is_pii: true`, so the field PII summary, the
Markdown report, `report pii`, `check` and catalog exports mark the field PII. Dropping value-only
matches whenever the field names another datatype is too broad: it would drop `uuid` from a field
`aoguid` and could hide real PII (email values in a field `login`).

## What Changes

- After registry enrichment and before registry filters, a PII value match (`data` or `registry`)
  without field-name evidence is demoted when another value match has field-name evidence, is not
  PII, is a different datatype that is not its registry parent or child, and has the same or a
  higher confidence.
- A demoted match stays in `matches` with `is_pii: false`, `pii_source: null` and
  `pii_outranked_by` (the outranking datatype). `registry.is_pii` keeps the datatype's flag.
  `--pii-only` drops it.
- `RegistryAdapter.ancestors()` returns the registry `parent` chain.
- Report model and published schema gain `Match.pii_outranked_by`.

## Impact

- Affected specs: `scan-report`
- Affected code: `metacrafter/engine/pii.py`, `metacrafter/scan_service.py`,
  `metacrafter/registry/adapter.py`, `metacrafter/report/models.py`,
  `docs/static/schema/report-v3.schema.json`
