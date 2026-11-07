# Change: Precise scan defaults

## Why

A default scan reports many wrong matches. Scans report far fewer of
them with `--registry-detectors off --confidence 50`, and that should be the default:
broader, less precise detection should be something a user turns on.

Measured on 10 sample files of Russian open data (`datacrafter-examples`: 209 fields,
up to 1000 records each, metacrafter-rules v0.0.6, registry 3e99a78):

| Options | Matches | Fields with a match |
|---|---|---|
| `--registry-detectors stable --confidence 5` (1.0.0 default) | 238 | 116 |
| `--registry-detectors off --confidence 50` | 133 | 106 |
| new defaults (`checksum`, 50, date patterns after name-only matches) | 135 | 106 |

Of the 105 matches the stricter options drop:

- 82 are value matches with confidence under 30. Most are chance checksum matches: a column
  of 10-digit INNs also passes the ISBN-10, NHS number, NPI, Turkish tax number and Polish
  NIP checks for 6–12% of its values, so every `inn` field had 8–11 extra datatypes.
- 3 are registry-derived matches: `cficode` at 98.4 on a `status` column (wrong, and not
  removed by a confidence threshold: its regexp has no checksum), `it_aic` at 12.1 on `uid`
  (wrong) and `date` on `dateLicDoc` (right; a rule also reports `datetime`).
- 4 value matches between 39 and 41 are right: `orgname` on organisation name columns. Their
  fields keep a field-name match (`name`, 60).

Field-name-only matches score 60 × priority weight: 60 (priority 1), 48 (priority 2) and
36 (priority 3). With a default of 50, field-name-only matches of priority 2 and 3 rules are
no longer reported by default.

Turning registry detectors off entirely loses detectors that are precise because a checksum
validates every value. The rule pack has only field-name rules for `bankcard`, so with `off`
a column of Luhn-valid card numbers gets no match. Of the 124 stable detectors, 11 have a
checksum (`bankcard`, `be_iban`, `be_vat`, `benatcardid`, `cz_ico`, `dedriverlic`, `it_aic`,
`it_codicefiscale`, `it_iva`, `kz_bin`, `nl_onderwijsnummer`); on the sample files they
report nothing at confidence 50.

The registry `date` detector also gave value evidence for ISO dates in fields with a
date-like name: qddate date patterns ran only when a field had no match at all, and a
field-name match such as `created` → `datetime` skipped them.

## What Changes

- **BREAKING** `--confidence` defaults to 50 instead of 5 in the CLI, SDK (`CrafterCmd`
  scans), HTTP API and MCP server. `--confidence 5` restores the 1.0.0 behaviour.
- **BREAKING** New registry detector mode `checksum`: stable records with a checksum that
  metacrafter validates. It is the default of `--registry-detectors` and of
  `registry_detectors` in `.metacrafter` instead of `stable`. `--registry-detectors stable`
  restores the 1.0.0 behaviour. `match: registry` rules are not affected.
- Date patterns (qddate) run on a field whose only matches are field-name matches.
- `metacrafter evaluate` and its benchmark baseline use the new defaults: `userid` (a false
  positive) is no longer reported, and `date` on the ISO date column `created` is no longer
  found (it reported `date` through the registry detector only). `metacrafter rules test
  --registry-examples` keeps `--registry-detectors stable`: it tests registry coverage.
- Docs, skills and the CHANGELOG describe the defaults and the options for broader scans.

## Impact

- Affected specs: `detection-engine` (registry-derived detectors, date patterns),
  `scan-results` (default confidence threshold).
- Affected code: `metacrafter/engine/registry_detectors.py`, `metacrafter/classify/processor.py`,
  `metacrafter/scan_service.py`, `metacrafter/config.py`, `metacrafter/cli/commands.py`,
  tests, `tests/benchmark/baseline.json`, docs.
- Users who relied on detectors without a checksum or on low-confidence matches must pass
  `--registry-detectors stable` or `--confidence 5` (or set `registry_detectors: stable` in
  `.metacrafter`).
- Follow-up in metacrafter-rules: a value rule for ISO 8601 dates (`date`), so default scans
  report `date` without registry detectors.
