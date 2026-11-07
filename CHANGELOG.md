# Changelog

All notable changes to Metacrafter will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- **Default rule pack is metacrafter-rules v0.0.6; bundled registry snapshot is 3e99a78.**
  Fields of museum and cultural heritage data get labels: on an export of the
  ar.culture.ru "Артефакт" API (16077 objects, 193 fields) a full scan labels 125 fields
  instead of 100.
  - Goskatalog registration numbers (`goskatalogNumber`) are `ru_goskatalog_regnum`, by
    field name and by value in such fields.
  - Surnames with initials (`Иванов И.И.`, `И.И. Иванов`) are `rus_initials_surname`;
    fields named `author` or `автор` are `dc_creator`.
  - Historical dating (`XIX век`, `конец XIX века`, `19th century`, `1950s`) is
    `ru_historical_dating` and `historical_dating`.
  - URL slugs are `slug`, artwork technique and material fields are `art_medium`, and
    dimensions as text (`30х40 см`, `68 x 82 cm`) are `object_dimensions`.
  - `fqdn` no longer matches Japanese and Chinese text with the ideographic full stop `。`.
  - `oid` no longer matches decimal fractions such as `0.1`.
  - The `languagetag` value rule runs on fields such as `lang`, `language_code` and
    `locale`; its field guard matched no field name.

  The registry snapshot adds `slug`, `art_medium`, `object_dimensions` and
  `historical_dating`, which v0.0.6 rule keys use, and makes `ogrn` cover OGRNIP (15
  digits), with `ogrn_ogrnip` as an alias. All rule keys resolve. In
  `tests/ecosystem/rule_thresholds.yaml` the new value rules start at recall 1.0, and
  `ruogrn` goes down to 0.6667: it rejects the new registry example `385768585948949`,
  whose year digits `85` are not a year in which OGRNIP numbers were issued (the check
  digit is valid). The benchmark baseline is unchanged.
- **Default rule pack is metacrafter-rules v0.0.5; bundled registry snapshot is 34a946c**
  (#61). Fields of the Russian ESKLP drug catalogue get labels (18 of 29 fields of an
  export instead of 10):
  - OKPD2 codes are `okpd`, by field name (`okpd2`, `код окпд2`) and by value in such
    fields. A field `okpd2` used to report `ipv4` first. `okpd` is stable in the registry,
    with the alias `okpd2`.
  - ESKLP codes (`21.20.10.226-000001-1-00008-0000000000000`) are `ru_esklp_code`.
  - OKEI unit symbols (`шт.`, `мл`, `мг`, `ЕД`) are `unit_of_measure`.
  - `ath`, `std_mnn_name`, `norm_mnn`, `klp_all_list` and `ftg` fields get `atccode`,
    `rumedmnn`, `rumedicinetradename` and `medication_class` by name.
  - ATC codes match by value again: the four value rules described 6 characters with a
    length of 7 and matched nothing.
  - The OKVED value rule runs on fields such as `okved2` and `main_okved`; its field guard
    matched no ordinary name.

  The registry snapshot adds `medication_dosage_form`, `medication_strength`,
  `atc_code_name`, ESKLP SMNN and KLP codes, Italian datatypes and `filepath`, `height`
  and `width`. The rule keys `path_file`, `height_measurement` and `width_measurement`
  resolve now, so `tests/ecosystem/known_unresolved_rule_keys.yaml` is empty.
  `metacrafter/engine/checksum_algorithms.py` is re-copied from the registry at the same
  commit and adds `issn`, `ismn`, `iso7064_mod11_2` (ISNI, ORCID), `it_aic`, `ror` and
  `ru_inn_padded`. In `tests/ecosystem/rule_thresholds.yaml`, `issnvalue`,
  `issnvaluewithoutdash` and `rusfeddistrict` have lower minimums: the registry added
  examples these rules miss (`0960-894X`, `Приволжский`, `ЦФО`). `wikidataidvalue` accepts
  the new negative example `Q01` and is listed as an exception.
- **Default rule pack is metacrafter-rules v0.0.4** (#42). Full-name rules of every country
  except China report `person_fullname`, and first-name and last-name rules report
  `person_firstname` and `person_surname`. Before, `full_name` in the Australian, Indian and
  Singapore rules was labelled `person_name`, which in the registry is a Chinese datatype, and
  Indian first and last names used the Indian `firstname` and `lastname`. The universal rules
  also match `full_name`, `person_name`, `given_name`, `family_name` and `Last Name` (a
  surname, no longer a middle name). New label `file_extension`: fields named
  `file_extension`, `file_ext` and the like, and `extension` or `ext` fields whose values are
  extensions (`pdf`, `.docx`), such as GeoNode `links.extension`.
- **Default rule pack is metacrafter-rules v0.0.3** (#41). Rule keys resolve to registry
  datatypes: field-name rules report `boolean`, `latitude`, `datetime`, `address`, `orgname`,
  `userid` and other registry ids instead of draft keys such as `active_boolean` or
  `lat_geo`, so their matches get registry metadata and links. Value rules no longer accept
  the registry's negative examples: IBAN, EU and French VAT, USt-IdNr., Handelsregister,
  German plates, ASN and ULID check their check digits or format, and the IBAN print format
  with spaces matches. ISBN rules match hyphenated ISBNs, `esmonthbydatafiltered` loads
  again (key `month`), and the pack imports without `SyntaxWarning`. In
  `tests/ecosystem/`, the allow-list of unresolved rule keys shrinks from 32 to 3
  (`height_measurement`, `width_measurement`, `path_file`) and `negative_exceptions` is
  empty. OGRN and OGRNIP are detected by value in default scans (#51): `ruogrn` is no longer
  imprecise and accepts 15-digit OGRNIP, `ruogrnknown` reports `ogrn` and matches more
  field names, and `rutitleknown` no longer labels `ИНН`, `КПП`, `ОКПО` and `ОГРН` columns
  `name`. In `tests/fixtures/ru_utf8_comma.csv` the `ogrn` column gets `ogrn 100` instead
  of only `ogrn_ogrnip 36` from its name.
- **Float columns of whole numbers are matched by data rules.** A column whose values are
  all whole numbers stored as floats (`.xls` files, pandas columns with missing values, CSV
  text such as `1027700132195.0`) is matched as integer text (`1027700132195`). Before,
  every float column skipped data rules, so identifiers such as OGRN or INN read from Excel
  were found only by field name. Columns with a fractional value, or a value of 2^53 or
  more, are still skipped.

- **Bundled registry snapshot updated** to registry 1.3.0.dev0 at `2ec9d12`, which includes
  apicrafter/metacrafter-registry#115: `flightnumber` no longer accepts a bare 1–4 digit
  number, `eunuts` and `vatin` need a country prefix (no more `JPEG`, `PDF`, `BE10`), and
  `wkt` is stable, so WKT geometry columns are detected (`armsis` GeoNode metadata: 45
  correct matches, field recall 0.86 → 0.88). Existing caches pick up the change after
  `registry_cache_ttl`, because a refresh compares the whole manifest, not only the version.
- **The pinned rule pack ships with the package.** `metacrafter/rules/bundled/` holds the
  archive of the pinned metacrafter-rules release (`v0.0.2`, 284 KB), verified with the same
  SHA-256. The first scan unpacks it into the cache instead of downloading it from GitHub:
  0.07 s instead of 20–70 s. `rules_offline` works without `metacrafter rules fetch`.
  `metacrafter rules fetch --force` and `rules_release` still download from GitHub.
  After a release bump, run `scripts/bundle_rule_pack.py`.
- Package metadata uses the SPDX license expression `Apache-2.0` (PEP 639); building needs
  setuptools 77 or newer.
- **`--langs` and `--country-codes` keep universal rules.** Both options select language-
  and country-specific rule files; rules with `lang: common` and rules with
  `country_code: xx` (or no country) now always load. Before, `--country-codes am` dropped
  the universal rules (email, URL, UUID, description, …) and cut correct matches on
  `armsis` GeoNode metadata from 44 to 23; now it finds 45. `--langs en` also dropped every
  registry-derived detector (`lang: common`). `rules list --country-codes` lists the
  universal rules too.

### Added

- `--strip` (scan file, sql, mongodb, bulk) and `CrafterCmd(strip=True)` /
  `scan_data(strip=True)` remove leading and trailing whitespace from string values,
  nested ones included, before statistics and matching. Off by default
  (apicrafter/metacrafter#13).
- `scan sql` reports primary and foreign keys from the table schema: tags `primary_key`,
  `autoincrement`, `foreign_key`, a `database` object per key column, and a schema match
  `id` (confidence 100) for single-column integer keys the database generates
  (apicrafter/metacrafter#14).
- Registry-derived detectors are also built from registry pattern records (`kind:
  pattern`). A match reports the pattern's `semantic_type` as the datatype and the pattern
  id in `pattern`: a column `language` with `eng` reports `language` / `iso639_2` once the
  registry marks those patterns stable (apicrafter/metacrafter#39). The background corpus
  gains two-letter codes and letter-and-digit codes (`WGS84`, `EA1`), so `^[A-Z]{2}$`-style
  and flight-number-style regexps need field-name evidence.
- `--llm-verify` / `llm_verify: true`: after the rules, one LLM request per table keeps or
  rejects value matches (rules and registry detectors) given the column paths, value samples
  under `--sample-policy` and candidate descriptions; the answer is limited to the candidate
  ids. Field-name matches are not touched. The report gets `llm_verifier`; rejected matches
  appear in `--explain` (apicrafter/metacrafter#40).
- Reports have `field_groups`: sibling fields that share a name prefix (`place`,
  `placeURL`, `placeType`), a suffix (`reviewScore`, `avgScore`) or are language variants
  (`title`, `title_en`). Markdown output lists them (apicrafter/metacrafter#12).
- `llm_max_requests` in `.metacrafter` and `--llm-max-requests` set the LLM request budget
  per scan (default 100). A scan that needs more stops with `LLM request budget of N
  requests exceeded; raise llm_max_requests (--llm-max-requests) or use --llm-fallback
  rules` instead of `LLM classification failed`, and logs a warning first when there are
  more fields than requests left. Before, a file with more than 100 fields could not be
  scanned with `--llm-only`.
- Field rules try a field name without a language suffix when the full name matches
  nothing: `abstract_en` → `abstract`, `title_ru` → `title`. Codes that are also common
  words or abbreviations (`id`, `no`, `ms`, …) are not treated as suffixes.
- Registry-derived detectors accept the id of the record's `parent` as field-name evidence
  at half weight, so a generic code type with `parent: language` matches in a column
  `language`.

### Fixed

- **Value-only PII matches no longer make a field PII when the field names a non-PII
  datatype that outranks them** (#62). OKPD2 codes are valid IPv4 strings, so a field
  `okpd2` listed `ipv4` and `ipaddr` after `okpd`, and its PII summary, Markdown report,
  `report pii`, `check` and catalog exports marked it PII. A PII match without field-name
  evidence is now demoted when another value match with field-name evidence is not PII, is
  a different datatype that is not its registry parent or child, and has the same or a
  higher confidence. The match stays in `matches` with `is_pii: false`,
  `pii_source: null` and `pii_outranked_by: okpd`; `registry.is_pii` keeps the datatype's
  flag. `--pii-only` drops it. Field-name-only matches never outrank a value match, so a
  field `login` with email values stays PII.
- **The primary match follows confidence and field-name evidence** (#58). A field's
  `matches` list and its `datatype_url` kept rule order, so the first match was the
  matching rule with the highest priority, not the best-scored one. Confidence is clamped
  at 100, so a datatype that the field name names could tie with a value-only match and
  lose: OKPD2 codes such as `21.20.10.226` are valid IPv4 strings, and a field `okpd2`
  reported `ipv4` first even when the `okpd` detector matched with field-name evidence.
  Matches are now ordered by confidence, then field-name evidence, then rule order; the
  summary string uses the same order. Value-only matches are kept, so `ipv4` is still
  listed after `okpd` (since #62 without marking the field as PII).
- **Value rules on columns with missing nested values.** When one record lacked a nested
  object or had an empty array (`{"details": null}`, `addresses: []`), its column held a
  null, the length prefilter fell back to a length of 4 and dropped every rule that cannot
  match 4 characters. In a JSON Lines file of Russian suppliers, `details.ogrn` lost `ogrn`
  and `addresses.region` lost `rusregion` once the sample held such a record. The
  prefilter now uses the lengths of the non-empty values.
- **Registry detectors with digit-only patterns need field-name evidence.** Background
  broadness measured integers of up to six digits only, so a regexp such as
  `^[1-9]\d{9,11}$` (`scopusauthoirid`) or the `uiccode` pattern counted as distinctive
  and labelled phone numbers, INN, OKATO and account ids at 77-96 confidence. Integers of
  7 to 20 digits are now background classes: a stable detector without a checksum whose
  pattern accepts most numbers of some length reports a match only when the field name
  names its datatype.
- **`de_iban` on columns of IBANs from several countries** (#52). Since rules v0.0.3,
  `de_pii_ibanvalue` validates check digits with the same `validate_iban` as the universal
  `iban` rule, so it was kept as country evidence and reported `de_iban` on one value in
  three. A country-specific value rule whose validator a matched universal rule also uses
  is now dropped when its confidence is lower than that match: it missed values the
  universal rule accepted. On a column of German IBANs it is still reported. Country rules
  with their own checksum, such as `de_steuerid`, keep their matches. The benchmark
  baseline no longer lists `de_iban`.
- **Country-specific detectors and broader field names.** Field-name evidence lifted the
  country check for country-specific registry detectors even when the field name only
  named the broader type: a column `driver_license` supported every
  `<country>_driver_license`. The country check now needs the type's own name including
  its country (`am_postal_code`), unless `--country-codes` includes that country.

- **Fewer false positives from registry-derived detectors.** Registry specificity is
  measured against other records' examples, which are mostly long identifiers, so
  patterns such as `\d{1,4}` (`flightnumber`), `\d{4}` (`am_postal_code`) or
  `[A-Za-z]{2,63}` (`tld`) passed the `stable` threshold and matched every counter, year
  and plain word. A detector whose regexp accepts at least 30% of a class of ordinary
  tokens (integers of one length, words, short acronyms) and has no checksum now needs
  field-name evidence: a field rule for the datatype, or the registry id or an alias in
  the field name. Country-specific detectors without field-name evidence are skipped when
  the scan country differs, or is unknown and the record has no checksum. `--explain`
  lists the reasons. On `armsis` GeoNode metadata (105 fields, 59 labelled) false
  positives fell from 89 to 1 with no labelled field lost; across seven other Armenian
  open-data catalogues matches fell from 961 to 371, and none of those reviewed was a
  true positive.
- **Country-specific duplicates of universal matches.** Without `--country-codes`, a
  field-name match from a country-specific rule file is dropped when a universal rule
  matched the field with at least its priority, or any rule matched with a higher
  priority (`city` no longer also reports `egcity`, `idcity`, `trcity`); a value match
  from a country-specific rule without a validator or field-name evidence is dropped when
  a universal rule matched the same values (`uuid` no longer also reports `rufias`).
  `tests/benchmark/baseline.json` loses nine datatypes that only had false positives.

### Performance

- **Bundled registry snapshot.** The package ships the registry snapshot recorded in
  `ecosystem.lock` (`metacrafter/registry/bundled/`, about 550 KB). With the default
  registry URL, a cold cache is seeded from it, so the first scan downloads nothing. A
  first `metacrafter scan file` with an empty registry cache took 97–123 s and now takes
  1.4–2.0 s (a warm scan takes 1.4–1.7 s). `registry_offline` works on a new machine
  without `metacrafter registry fetch`. Update it with `scripts/bundle_registry_snapshot.py`.
- **Smaller, bounded registry downloads.** Registry requests accept gzip (3.1 MB to 561 KB
  for `datatypes.jsonl`). A refresh during a scan stops after `registry_fetch_timeout`
  seconds in total (new setting, default 30) and falls back to the older cache; a failed
  refresh is retried after `registry_cache_ttl`, not on every scan. `metacrafter registry
  fetch` has no overall time limit.

## [1.0.0] - 2026-11-06

Registry-driven modernization (see `devdocs/MODERNIZATION_PLAN.md`, `openspec/ROADMAP.md`)
together with the fixes from the 2026-10-01 repository review. Migration guide:
`docs/docs/getting-started/migration.md`.

### Distribution

Neither metacrafter-rules nor metacrafter-registry is a Python dependency:

- **Rule pack from GitHub releases.** The default rule pack is downloaded on first use from
  the pinned metacrafter-rules release (`v0.0.2`), verified by SHA-256 and cached.
  `metacrafter-rules` is no longer a Python dependency. New command: `metacrafter rules fetch`.
  New settings: `rules_dir`, `rules_release` + `rules_sha256`, `rules_offline`.
- **Registry through its website.** Metacrafter no longer uses the `metacrafter-registry`
  package:
  - the registry's checksum algorithms are vendored;
  - value identification, search and the qddate crosswalk run on the downloaded `/api/v1/` data;
  - `registry_path` must be a snapshot directory, which `metacrafter registry fetch --to DIR` writes;
  - the `registry` extra is removed.
- **CI.** The ecosystem gate uses the live registry. It blocks while the registry version
  matches `ecosystem.lock` and reports otherwise.

### Breaking

- Python 3.10 or newer.
- The in-repository `rules/` directory is removed; rules no longer load from `./rules`.
  The default rule pack comes from metacrafter-rules (see Distribution above).
- Optional dependencies moved to extras: `mongodb`, `duckdb`, `server` (now FastAPI +
  uvicorn instead of Flask), `formats` (Parquet, ORC, Excel, XML, compression), `mcp`,
  `llm` (Anthropic SDK), `llm-chroma` (deprecated vector index). Missing extras raise
  `MissingExtraError` naming the extra.
- Reports are version 3. Use `--output-format legacy` for the pre-1.0 JSON shape.
- JSON, YAML and Markdown output omit raw values unless `--include-samples` is set.
- Field-name-only matches score at most 60 (`--legacy-scoring` restores 100).
- A later rulepath entry now overrides an earlier rule with the same id.
- `priority: 1` is the strongest rule priority (rules sort ascending).
- LLM providers share one OpenAI-compatible HTTP client; the OpenAI SDK is no longer used
  for chat. `llm_registry_path` is deprecated.

### Added

- Registry adapter: offline-capable loading with SHA-256 verification, canonical ids for
  alias keys, registry metadata on every match (`registry`, `datatype`, `rule_key`,
  `pii_source`, `replaced_by`), registry provenance in reports, configurable registry URL,
  filters `--categories`, `--pii-only`, `--status`.
- Rule format version 2 with a JSON Schema; `match: regex`, `match: registry`,
  `validator: checksum:<algorithm>`; `deprecated` rules; `metacrafter rules validate`.
- Registry-derived detectors for datatypes without rules (`--registry-detectors`).
- Scoring model v1 with an `evidence` object; qddate formats mapped to registry patterns.
- `metacrafter rules check --registry`, `rules test --registry-examples`, `evaluate --baseline`.
- `--output-format md`; `metacrafter export labels --system presidio|google_dlp|dpv|purview|...`;
  `metacrafter export frictionless`.
- FastAPI HTTP API: `POST /v2/scan`, `POST /v2/classify`, `/health`, `/ready`, OpenAPI.
- MCP server (`metacrafter mcp`) with scoped, read-only tools; agent skills in `skills/`;
  `llms.txt` for the documentation site.
- Registry-constrained LLM classification with PII-aware disclosure and an Anthropic provider.
- From the repository review: shared scan service, report interchange and bulk output,
  bounded head/reservoir sampling, schema-aware SQL quoting, export outcome accounting,
  report diff, PII inventory, policy checks, rule lint/test/draft, `scan explain`,
  dbt dry-run export, secret redaction, hermetic tests.
- Ecosystem contract tests in CI (pinned registry and rules) and a weekly compatibility check.

### Fixed

- Boolean `is_pii` in rules was ignored.
- Catalog exporters used a hard-coded list of five PII datatypes.
- `metacrafter scan file` crashed when `--llm-min-confidence` was not given.
- Valid PyParsing rules with tuple or indexed repetition were rejected.
- Loading the default rule pack took 4.6 s (now about 0.4 s).

## [0.0.5] - 2026-07-08

### Added
- **LLM-Based Classification**: New LLM-powered classification using Retrieval-Augmented Generation (RAG)
  - Support for multiple LLM providers: OpenAI, OpenRouter, Ollama, LM Studio, and Perplexity
  - Three classification modes: rules-only (default), LLM-only, and hybrid (rules + LLM fallback)
  - Vector-based similarity search using ChromaDB for retrieving relevant registry entries
  - Automatic index building from registry on first use
  - Configurable confidence thresholds for LLM results
  - CLI options: `--classification-mode`, `--llm-provider`, `--llm-model`, `--llm-api-key`, `--llm-base-url`, etc.
  - Configuration support via `.metacrafter` config file
  - Optional dependencies: `openai`, `chromadb`, `requests`
  - See [README](README.md#llm-based-classification) for usage examples
- **Apache Atlas Integration**: New export command to push Metacrafter scan results to Apache Atlas metadata catalog
  - Export PII labels, datatypes, and confidence scores as classifications and custom attributes
  - CLI command: `metacrafter export atlas`
  - Configuration support via `.metacrafter` config file or environment variables
  - Optional dependency: `requests`
  - See [Implementation Guide](devdocs/ISSUE_ATLAS_IMPLEMENTATION.md)
- **DataHub Integration**: New export command to push Metacrafter scan results to DataHub metadata catalog
  - Export PII labels, datatypes, and confidence scores as tags, glossary terms, and custom properties
  - CLI command: `metacrafter export datahub`
  - Configuration support via `.metacrafter` config file or environment variables
  - Optional dependency: `acryl-datahub[datahub-rest]`
  - See [Issue #24](https://github.com/apicrafter/metacrafter/issues/24) and [Implementation Guide](devdocs/ISSUE_24_IMPLEMENTATION.md)
- **OpenMetadata Integration**: New export command to push Metacrafter scan results to OpenMetadata metadata catalog
  - Export PII labels, datatypes, and confidence scores as tags, glossary terms, and custom properties
  - CLI command: `metacrafter export openmetadata`
  - Configuration support via `.metacrafter` config file or environment variables
  - Optional dependency: `openmetadata-ingestion`
  - See [Implementation Guide](devdocs/ISSUE_OPENMETADATA_IMPLEMENTATION.md)
- **Rules Inspection Commands**: New commands for inspecting and managing classification rules
  - `metacrafter rules list`: List all loaded rules with metadata (ID, name, type, match method, language, country, contexts)
    - Supports multiple output formats: table (default), JSON, YAML, CSV
    - Filterable by rule path and country codes
  - `metacrafter rules stats`: Display aggregate statistics about loaded rules
    - Shows counts of field rules, data rules, languages, contexts, country codes, and date/time patterns

### Changed
- Updated configuration system to support LLM, DataHub, Apache Atlas, and OpenMetadata settings
- Enhanced CLI with new `export` command group, `rules` command group, and LLM classification options
- Extended `MetacrafterConfig` with LLM-related fields and validation
- Improved error handling for missing optional dependencies (graceful degradation)

## [0.0.4] - Previous Release

### Added
- Support for multiple file formats (CSV, JSON, JSONL, XML, Parquet, Avro, ORC, Excel, BSON, Pickle)
- Support for compressed files (gzip, bzip2, xz, lz4, zstandard, Brotli, Snappy, ZIP)
- Database scanning for SQL databases (via SQLAlchemy) and MongoDB
- Rule-based classification system with 111+ rules
- Date detection with 312+ patterns
- Context and language filtering
- Built-in API server
- Statistics and field analysis

### Changed
- Improved error handling and logging
- Enhanced output formats (table, JSON, YAML, CSV)

## [0.0.3] - Earlier Release

Initial public release with core functionality.

---

## Notes

- **LLM Classification**: The LLM classification feature requires optional dependencies: `openai`, `chromadb`, and `requests`. Install them with `pip install openai chromadb requests` if you plan to use LLM-based classification. The feature gracefully degrades if dependencies are missing.
- **Apache Atlas Integration**: The Apache Atlas integration requires the optional `requests` package. Install it separately if you plan to use this feature.
- **DataHub Integration**: The DataHub integration requires the optional `acryl-datahub[datahub-rest]` package. Install it separately if you plan to use this feature.
- **OpenMetadata Integration**: The OpenMetadata integration requires the optional `openmetadata-ingestion` package. Install it separately if you plan to use this feature.
- **Configuration**: LLM, DataHub, Apache Atlas, and OpenMetadata settings can be configured in `.metacrafter` config file or via environment variables.

