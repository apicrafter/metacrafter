## ADDED Requirements

### Requirement: Universal Rules Under Filters

Language filters (`--langs`) and country filters (`--country-codes`) SHALL select
language- and country-specific rule files. Rule files with `lang: common` SHALL load and
match under any language filter. Rule files with `country_code: xx` or without a country
SHALL load under any country filter. Rule files of other languages or countries SHALL NOT
load.

#### Scenario: Country filter

- **WHEN** a scan runs with `--country-codes am`
- **THEN** rules from files with `country_code: xx` or no country are loaded
- **AND** rules from files with `country_code: ru` are not loaded

#### Scenario: Language filter

- **WHEN** a scan runs with `--langs en`
- **THEN** rules with `lang: common`, including registry-derived detectors, are applied
