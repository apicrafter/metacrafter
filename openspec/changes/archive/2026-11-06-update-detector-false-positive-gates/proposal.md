# Change: Gate generic and country-specific detections behind field-name evidence

## Why

`metacrafter scan file` on GeoNode layer metadata from the Armenian spatial data portal
(`armsis`, 29 records, 105 fields) reported 89 false matches next to 44 correct ones
(precision 0.33 on 59 hand-labelled fields). 76 of the false matches came from
registry-derived detectors, 11 from country-specific field-name rules and 2 from curated
value rules:

| Cause | Example |
|---|---|
| Registry regexp accepts any short integer | `flightnumber` (`...\|\d{1,4}`), `udkindex` (`\d{1,3}(\.\d{1,3})*`), `fips52`/`okfs` (`\d{2}`), `am_postal_code` (`\d{4}`) on `id`, `layers_count`, `rating`, years |
| Registry regexp accepts any plain word | `tld` (`[A-Za-z]{2,63}`) on `workspace`, `language`, `csw_type`, `storeType` |
| Registry regexp accepts any short acronym | `eunuts`, `vatin`, `miccode`, `molformula` on `JPEG`, `PDF` |
| Country-specific variants of a universal name match | `owner.city`: `city` plus `egcity`, `idcity`, `krcity`, `sacity`, `trcity` |
| Country-specific pattern that repeats a universal one | `uuid` plus `rufias` on every GUID column |

The `stable` detector mode admits a regexp when its specificity (1 − share of other
registry records' examples it matches) is at least 0.8. Registry examples are mostly
long, distinctive identifiers, so `\d{1,4}` still scores 0.889 and `[A-Za-z]{2,63}`
0.923. A detector needs no field-name evidence, and a country-specific detector is only
penalised (−10) when `--country-codes` is set. Metacrafter 0.0.4 had no derived
detectors and reported 2 false matches on the same file.

Other options were measured on the same labels:

| Option | False | Correct | Precision |
|---|---|---|---|
| Default | 89 | 44 | 0.33 |
| `--registry-detectors off --confidence 50` | 1 | 44 | 0.98 |
| `--use-llm` (hybrid; calls the LLM only for fields under 50) | 90 | 47 | 0.34 |
| `--llm-only` (gpt-4o-mini) | 20–30 | 26–28 | 0.48–0.57 |
| LLM verifies rule candidates (one request per table) | 0 | 38–41 | 1.00 |
| This change | 1 | 42 | 0.98 |

The two correct matches this change removes are duplicates (`firstname`, `lastname` from
an India-specific rule next to `person_firstname`, `person_surname`); no labelled field
loses its match.

## What Changes

- **Background broadness.** New `metacrafter/engine/background.py` tests a regexp against
  classes of ordinary tokens: integers of 1 to 6 digits (one class per length), plain
  words, title-case words and short acronyms. The largest share of one class is the
  background broadness. 84 of 346 stable registry regexps reach 0.3.
- **Generic detectors need field-name evidence.** A registry-derived detector without a
  checksum whose background broadness is at least 0.3 reports a match only with
  field-name evidence.
- **Field-name evidence from the registry.** Besides a field rule for the same datatype,
  the field name counts as evidence when it contains the tokens of the record id or an
  alias, without the record's country codes and common tokens (`code`, `number`, `id`,
  ...). It also gives the field-name bonus.
- **Country-specific detectors.** Without field-name evidence, a detector for a
  country-specific record is skipped when `--country-codes` names other countries, or no
  country is set and the record has no checksum.
- **Curated rules, no scan country.** A field-name match from a country-specific rule
  file is dropped when a universal rule matched with at least its priority weight, or any
  rule matched with a higher weight. A value match from a country-specific rule without a
  validator and without field-name evidence is dropped when a universal rule matched the
  same field with at least the same confidence.
- `--explain` reports each skipped detector and dropped match with its reason.
- `tests/benchmark/baseline.json` drops nine datatypes that had only false positives.

Not changed: `--country-codes` still loads only rule files with those countries (files
with `country_code: xx` are dropped, as `--langs` drops `lang: common`). Registry regexps
are not edited here.

## Impact

- Affected specs: `detection-engine`
- Affected code: `metacrafter/engine/background.py` (new),
  `metacrafter/engine/registry_detectors.py`, `metacrafter/classify/processor.py`
- Results: fewer matches; a column whose name does not name a generic or country-specific
  registry datatype no longer reports it. Use `--explain` to see what was skipped.
