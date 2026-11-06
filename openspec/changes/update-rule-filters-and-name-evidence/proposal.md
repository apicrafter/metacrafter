# Change: Universal rules under filters, parent and suffix name evidence, LLM request budget

## Why

Follow-ups from `update-detector-false-positive-gates` (Beads `metacrafter-qrv`,
`metacrafter-ciw`, `metacrafter-4uu`), measured on the same hand-labelled GeoNode metadata
(`armsis`, 59 labelled fields):

- `--country-codes am` loaded only rule files with `am` in `country_code`, so universal rules
  (`country_code: xx`: email, URL, UUID, description, …) were dropped: 44 correct matches
  fell to 23. `--langs en` dropped `lang: common` files and every registry-derived detector
  (all `lang: common`) the same way.
- `abstract_en` and `category.description_en` got no `description` match: field rules do not
  know language suffixes.
- Code types such as ISO 639 language codes have generic regexps; their only name hint in
  the registry is `parent: language`, which field-name evidence ignored.
- Field-name evidence lifted the country check of a country-specific detector even when the
  field name named only the broader type (`driver_license` for `am_driver_license`), because
  name keys drop the record's country codes.
- `llm_max_requests` (100) had no CLI or configuration setting; a file with 105 fields made
  `--llm-only` fail after 100 requests with `LLM classification failed`.

## What Changes

- **Rule filters** (`rule-loading`): `--langs` and `--country-codes` select language- and
  country-specific rule files; files with `lang: common` and files with `country_code: xx`
  or no country always load. The match-time language filter keeps `lang: common` rules.
- **Language suffix** (`detection-engine`): when a field name matches no field rule, the name
  without a trailing language code (`_en`, `-ru`, `.hy`; an allowlist without codes that are
  also common words such as `id`, `no`, `ms`) is tried.
- **Parent evidence** (`detection-engine`): the tokens of the record's `parent` id count as
  field-name evidence at weight 0.5. It satisfies the generic-pattern gate and gives half the
  name bonus.
- **Country check** (`detection-engine`): a country-specific detector passes the country
  check through its name only when the field name contains the record id or alias tokens
  including the country codes.
- **LLM budget** (`llm-classification`): `llm_max_requests` in `.metacrafter` and
  `--llm-max-requests`; a warning when there are more fields than requests left; the error
  names the budget and the settings. Documented that hybrid mode does not review rule matches.

Measured: `armsis` default 44 correct / 1 false (field recall 0.82 → 0.86);
`--country-codes am` 23 → 45 correct; seven other catalogues: 371 → 375 matches, the four
new ones are `description` on `*_en` fields.

## Impact

- Affected specs: `rule-loading`, `detection-engine`, `llm-classification`
- Affected code: `metacrafter/classify/processor.py`, `metacrafter/classify/utils.py`,
  `metacrafter/engine/registry_detectors.py`, `metacrafter/scan_service.py`,
  `metacrafter/config.py`, `metacrafter/cli/commands.py`, `metacrafter/exceptions.py`
- Behaviour: `rules list --country-codes` and scans with `--langs`/`--country-codes` include
  universal rules.
