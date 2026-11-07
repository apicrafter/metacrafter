## 1. Rule filters

- [x] 1.1 `import_rules`: keep `lang: common` files under `--langs` and `country_code: xx`/no-country files under `--country-codes`
- [x] 1.2 `get_filtered_rules`: keep `lang: common` rules under a match-time language filter

## 2. Names

- [x] 2.1 `strip_language_suffix` with an allowlist of language codes; field rules retry the base name when the full name matches nothing
- [x] 2.2 Parent id tokens as field-name evidence at weight 0.5 for derived detectors
- [x] 2.3 Country check uses id/alias tokens with country codes kept

## 3. LLM budget

- [x] 3.1 `llm_max_requests` in `.metacrafter`, `--llm-max-requests`, default 100
- [x] 3.2 `LLMBudgetExceeded`; error message names the budget and settings; warning when fields exceed the remaining budget
- [x] 3.3 Document that hybrid mode does not review rule matches

## 4. Tests and docs

- [x] 4.1 Tests for filters, suffixes, parent evidence, the country check and the budget
- [x] 4.2 Measure on `armsis` and seven catalogues
- [x] 4.3 Docs (`rules.md`, `llm.md`, options), CHANGELOG; archive `update-detector-false-positive-gates`
