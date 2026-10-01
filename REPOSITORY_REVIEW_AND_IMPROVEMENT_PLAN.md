# Metacrafter repository review and improvement plan

**Review date:** 2026-10-01

**Source baseline:** `b319103` (`main`, package version `0.0.6`)

**Status:** planning document; implementation work remains open in Beads.

**Recommended order:** result correctness and installed-package reliability → reproducible tests → bounded scanning and shared contracts → feature expansion.

## 1. Assessment

Metacrafter has a useful foundation: configurable semantic rules, several source adapters, field statistics, a CLI and Python API, optional LLM classification, and three catalog exporters. The main constraint is consistency across these interfaces. A scan can compute a statistic correctly but serialize it under the wrong name; a saved report can be accepted by an exporter yet export no fields; a failed catalog write can appear successful.

Prioritize trustworthy results and installation behavior before adding more connectors or a user interface. Preserve the rule-based engine and current public imports while extracting shared scanning, configuration, and reporting services incrementally. Make sampling, degraded operation, and classification evidence visible in the report.

This review covers production Python modules, built-in YAML, packaging, tests, CI, documentation, and the existing OpenSpec roadmap. It includes source inspection, synthetic reproductions, a local test/coverage run, lint checks, and wheel inspection. It does not establish precision/recall on real datasets or validate live remote databases, LLM providers, or catalog deployments.

The original checkout had an edited CI workflow and untracked proposals, ecosystem scripts, and configuration files. Those were read as pending work and left untouched. Historical counts concerning the sibling rules and registry repositories were not revalidated and must not be treated as current measurements.

## 2. Current features and maturity

| Capability | Current implementation | Main improvement needed |
|---|---|---|
| Field/value rules | Text, PyParsing, Python functions; language/context/country filters; date patterns via qddate | Typed rule validation, deterministic precedence, mandatory-validator handling, evidence and threshold semantics |
| Built-in and extended packs | 35 built-in YAML files with 140 declared rules: 100 field rules and 40 data rules; `metacrafter[rules]` auto-discovery exists | Bundle built-in YAML in distributions, resolve package resources outside the checkout, diagnose skipped rules |
| File scanning | iterabledata readers for text, binary, Excel, XML, and compressed files | Bound memory and explicitly verify the advertised format/codec matrix against supported dependency versions |
| SQL and DuckDB | SQLAlchemy scanning and a native DuckDB path; batched fetching; SQLite decoding recovery | Dialect-aware queries, schema-qualified identities, connection cleanup, tested portability |
| MongoDB | Database/collection enumeration and a limited cursor sample | Close clients explicitly, retain source identity, add container-backed integration tests |
| Bulk scans | Recursive directory traversal with per-file scanning | Parseable aggregate output, input filtering, exclusions, completeness/error accounting |
| Statistics | Types, uniqueness, string lengths, character composition, numeric ranges, dictionary values | Repair the SDK statistics mapping; support currently discarded field shapes; record the statistics scope |
| Python SDK | Lazy top-level `CrafterCmd` export; `scan_data()` returns dictionaries | Stable typed result contract, separation of scan results from output writing, explicit errors |
| CLI and output | Scan/rules/server/export groups; Rich/table/JSON/YAML/CSV; verbosity, progress, remote retry controls | Shared option validation, consistent machine output, partial-failure exit codes, round-trip reports |
| HTTP API | Flask application factory and `/api/v1/scan_data`; injectable processors/parsers | SDK option parity, input bounds, persistent worker dependencies, documented production deployment |
| LLM/RAG | Rules/LLM/hybrid modes; OpenAI, OpenRouter, Ollama, LM Studio, Perplexity provider implementations | Local embeddings, accepted-ID validation, explicit fallback, PII propagation, nested sampling, index lifecycle |
| Registry | URL generation, HTTP client, environment configuration and a config resolver | One resolver used end to end; optional validated, cached enrichment; HTTP error handling |
| Catalog exports | DataHub, OpenMetadata, Atlas tagging/properties/glossary/classification adapters | Scan-file interchange, explicit PII metadata, truthful write outcomes, merge/idempotency contracts |
| Extensibility | Custom rule paths and importable Python validators already work | Formal diagnostics and trust boundaries; add entry-point discovery only when packaging needs justify it |
| Documentation/automation | Docusaurus site, CI, CodeQL and docs deployment; roadmap and draft ecosystem checks | Executable examples, PR docs builds, honest proposal/release status and installation matrices |

The optional extras, top-level SDK export, comma-separated API filters, default-disabled server debug mode, Rich output, CLI exit-code guard, and catalog exporters are already implemented. They should be improved or verified rather than reintroduced as new features. The built-in pack has a duplicate `passportbyknown` ID in `rules/pii/pii.yaml` and `rules/pii/ru/ru_pii.yaml`; 140 declarations does not imply 140 successfully loaded rules.

## 3. Verification baseline

| Check | Observed result | Interpretation |
|---|---|---|
| `python -m pytest tests --cov=metacrafter --cov-report=term-missing --tb=short -q` | **219 passed, 22 failed, 50 skipped**, 345.67 seconds | Local baseline, not a clean-environment or remote-CI result |
| Line coverage from that run | **43.4%** overall; processor 82.2%, statistics 87.3%, core 37.9%, config 75.9%, API 68.5% | CLI and registry client show 0% in this run; subprocess coverage and undiscovered tests must be addressed before interpreting that as absence of behavior tests |
| Explicit registry tests: `python -m pytest tests/tests_registry.py -q` | **11 passed** | This filename is not collected by the default pytest filename pattern |
| Critical lint: `python -m flake8 metacrafter --select=E9,F63,F7,F82 --statistics --count` | **0 violations** | Useful narrow gate; it does not establish style, typing, or correctness |
| `ruff check metacrafter` with default rules | **36 findings**: 19 F401, 9 F841, 6 E741, 2 E402 | Mostly unused imports/variables and naming/import-layout debt |
| `python -m black --check metacrafter` | **35 files would be reformatted**, 5 unchanged | Adopt formatting in a dedicated change and then enforce it |
| `python -m build --wheel --no-isolation` | Wheel built successfully; **0 YAML files** in 49 archive entries | Build success currently misses a functional installation defect |
| Synthetic SDK/API/serialization/export tests | Reproduced the findings identified as **R** below | Mocked HTTP, injected processors and temporary data; no catalog writes or paid LLM calls |

Runtime: macOS, Python 3.13.7, pytest 9.0.2, pytest-cov 7.1.0, Pydantic 2.12.5, PyParsing 3.2.5, iterabledata 1.0.13, Ruff 0.14.9, Black 25.12.0. The installed iterabledata resolves to an editable sibling checkout. OpenAI, ChromaDB, DataHub, OpenMetadata and SQLAlchemy were absent from the initial test environment. SQLAlchemy 2.1.1 was subsequently installed only in a temporary diagnostic venv to reproduce identifier handling.

Failure classification: two XML tests fail inside the installed iterabledata reader; several LLM tests attempt unavailable optional imports; configuration tests observe the home configuration; the country-filter expectation conflicts with composed rule paths; an Ollama test expects an exception that the provider deliberately converts to a fallback result; a duplicate-warning assertion is affected by logging state. These require dependency-matrix and test-isolation work. Do not describe all 22 failures as proven production defects.

Committed CI makes lint/type/format checks advisory and has no coverage floor. The pre-existing local CI edit adds blocking critical lint and a 50% floor, but is uncommitted; the measured local coverage is below that proposed floor. No supported Python-version matrix or documentation-site build was run in this review.

## 4. Prioritized findings

**P1:** fix before relying on the affected workflow in automation. **P2:** planned maintainability, scaling or capability work. **R:** reproduced with synthetic data or artifact inspection. **S:** source-supported finding, with the stated integration limits.

| ID | Priority / evidence | Finding and consequence | Source at the reviewed baseline |
|---|---|---|---|
| Q01 | P1 / R | SDK stats use 14 headers for Analyzer's 19 values: numeric `minval` becomes `dictvalues`, and range/character flags disappear. API uses a different mapping. | `metacrafter/core.py:1289`, `metacrafter/classify/stats.py:460`, `metacrafter/server/api.py:131` |
| Q02 | P1 / R | One-character and numeric top-level keys are discarded by Analyzer; scalar arrays are dropped by both flatteners. A report can omit valid data. Selecting a missing field raises `KeyError`. | `metacrafter/classify/stats.py:149`, `metacrafter/classify/stats.py:341`, `metacrafter/classify/utils.py:9`, `metacrafter/classify/processor.py:791` |
| Q03 | P1 / R+S | The built wheel contains no YAML. Built-in paths default to CWD-relative `rules`, and absent directories yield no rules without an error. `requests` and `chardet` are directly imported but not declared as core dependencies. | `pyproject.toml`, `metacrafter/config.py:13`, `metacrafter/classify/processor.py:570`, `metacrafter/core.py:12`, `metacrafter/classify/utils.py:5` |
| Q04 | P1 / R+S | `str(quoted_name(...))` is interpolated into SQL and does not emit quoted SQL identifiers. A SQLite table named `order detail` fails. SQL and DuckDB SELECTs omit schema qualification; result keys use bare table names, permitting cross-schema collisions. Raw `LIMIT` also assumes dialect support. | `metacrafter/core.py:1797`, `metacrafter/core.py:1983`, `metacrafter/core.py:2003`, `metacrafter/core.py:2016`, `metacrafter/core.py:2240` |
| Q05 | P1 / R | Saved scan reports use `fields`; every exporter reads `data`. Feeding serialized output to Atlas processes zero fields with no error. Bulk JSON concatenates documents and cannot be loaded as one JSON value; CSV repeats headers and lacks source identity. | `metacrafter/core.py:854`, `metacrafter/core.py:1641`, `metacrafter/cli/commands.py:714`, `metacrafter/integrations/atlas.py:125` |
| Q06 | P1 / R+S | Export helpers swallow write errors, while callers increment success counts. Mocked Atlas HTTP 500 yields one classification added and no errors. Similar control flow exists in DataHub/OpenMetadata; live catalog semantics remain unverified. | `metacrafter/integrations/atlas.py:179`, `metacrafter/integrations/atlas.py:345`, `metacrafter/integrations/datahub.py:172`, `metacrafter/integrations/openmetadata.py:189` |
| Q07 | P1 / R+S | Exporters infer PII from tags and a short hard-coded datatype list instead of `match.is_pii`. An explicitly PII-marked `national_id` receives no Atlas PII classification. LLM bridges construct `RuleResult` without registry PII metadata. | `metacrafter/integrations/atlas.py:263`, `metacrafter/integrations/datahub.py:218`, `metacrafter/integrations/openmetadata.py:235`, `metacrafter/core.py:1106`, `metacrafter/core.py:1198` |
| Q08 | P1 / R+S | API ignores client-supplied confidence/date/field/stat options and always uses 5% confidence with date parsing. JSON scalar `3` produces 500. Factory routes recreate dependencies unless explicitly initialized; `run_server()` does initialize them. Request size/shape/limit bounds are absent. | `metacrafter/server/api.py:68`, `metacrafter/server/api.py:112`, `metacrafter/server/api.py:160`, `metacrafter/server/manager.py:39`, `metacrafter/core.py:984` |
| Q09 | P1 / R | Value/date matching uses `>` while consumers describe a minimum threshold; 100% rejects perfect value matches. Zero limit with included empty values can divide by zero. Missing/custom-empty handling differs between Analyzer and matcher. | `metacrafter/classify/processor.py:792`, `metacrafter/classify/processor.py:878`, `metacrafter/classify/processor.py:920`, `metacrafter/classify/processor.py:965` |
| Q10 | P1 / R+S | YAML boolean `is_pii: true` is compared to string `"True"`; a non-PII context then loses the flag. Missing validators permit unchecked pattern matching; failed field guards can broaden a rule. Duplicate IDs are first-wins, traversal is unsorted, and IDs are reserved before successful compilation. | `metacrafter/classify/processor.py:433`, `metacrafter/classify/processor.py:496`, `metacrafter/classify/processor.py:507`, `metacrafter/classify/processor.py:550`, `metacrafter/classify/processor.py:589` |
| Q11 | P1 scaling / R+S | `scan_file()` materializes the entire reader before applying a limit. An injected 20-row reader forwards all 20 rows with `limit=1`. Analyzer and column conversion also retain data proportional to input/cardinality; statistics and matching inspect different scopes. | `metacrafter/core.py:1567`, `metacrafter/core.py:1280`, `metacrafter/classify/stats.py:346`, `metacrafter/classify/utils.py:74`, `metacrafter/classify/utils.py:32` |
| Q12 | P1 / R+S | LLM results are not checked against registry/candidate IDs. A fake provider's invented ID is accepted. LLM-only failures fall back to rules; nested field samples and false/zero values are lost. Caller `fields` and LLM thresholds are not consistently applied in LLM-only mode. | `metacrafter/classify/llm/providers/base.py:90`, `metacrafter/classify/llm/classifier.py:205`, `metacrafter/core.py:308`, `metacrafter/core.py:1059`, `metacrafter/core.py:1327` |
| Q13 | P2 / S | Local providers still require OpenAI embeddings. Existing indexes are reused based only on nonzero count; rebuild clears before embedding succeeds; failed embeddings can become hard-coded 1536-dimensional zero vectors. Retrieval uses only the first requested filter value and filters after a limited nearest-neighbor query. | `metacrafter/classify/llm/classifier.py:58`, `metacrafter/classify/llm/classifier.py:115`, `metacrafter/classify/llm/classifier.py:131`, `metacrafter/classify/llm/embedder.py`, `metacrafter/classify/llm/retriever.py:73`, `metacrafter/classify/llm/vector_store.py:156` |
| Q14 | P1 / S | Debug logging interpolates the full LLM config, including `llm_api_key`, and error handling logs raw config. SQL scanning prints the complete connection string. Field samples can be sent to cloud embeddings/LLMs without a per-scan disclosure/minimization policy. | `metacrafter/config.py:321`, `metacrafter/config.py:327`, `metacrafter/core.py:1962`, `metacrafter/classify/llm/retriever.py:62`, `metacrafter/classify/llm/prompt_builder.py` |
| Q15 | P1 / R+S | Tests depend on home config/optional services and some accept errors as success. Registry tests are undiscovered; default-registry-path test ends with `pass`. CI installs `requirements.txt`, bypassing the minimal package/extras contract. | `tests/test_core.py:44`, `tests/test_core.py:178`, `tests/test_llm_classifier.py:95`, `tests/tests_registry.py`, `.github/workflows/ci.yml` |
| Q16 | P2 / S | `core.py` has 2,367 lines, `commands.py` 1,040, and `match_dict()` 310. `core.py` and `core/` coexist through an importlib shim. CLI app objects still live in core; constructor can force global logging setup. Statistics/output/source logic is duplicated. XML helper's mutable default retains candidates across calls, also reproduced. | `metacrafter/core/__init__.py`, `metacrafter/core.py:97`, `metacrafter/core.py:157`, `metacrafter/classify/utils.py:180` |
| Q17 | P2 / S | SDK/API/`RuleResult` URLs use a hard-coded base. Config getter, registry client and LLM URL resolver use different paths; the config getter is not wired throughout. Registry HTTP reads do not validate status/schema before JSON use. | `metacrafter/classify/processor.py:221`, `metacrafter/core.py:1422`, `metacrafter/server/api.py:200`, `metacrafter/config.py:257`, `metacrafter/registry/client.py:29` |

Additional rule-authoring concern: the expression blacklist rejects harmless `Literal('open')`. Replace substring checks with an allowlisted expression grammar/AST, validate the resulting parser type, and bound expression complexity. This review does not demonstrate an arbitrary-code-execution exploit. Python function validators are executable extensions and must be documented as trusted code.

## 5. Improvements to existing capabilities

Estimates below are **rough engineering days per work package**, including tests/docs, for an engineer familiar with the project. They are not delivery commitments or additive calendar deadlines. Priority overrides the order within a phase when a dependency blocks work.

| Work package / Beads | Changes and outcome | Dependencies | Estimate |
|---|---|---|---|
| W01 / `metacrafter-2` | One named statistics mapping; shared flattening with explicit paths, scalar-array behavior and missing-field policy; preserve field coverage | None; capture current valid outputs first | 2–4 days |
| W02 / `metacrafter-3` | Package YAML under resources; load via `importlib.resources`; constrain package discovery; declare direct dependencies; test wheel and sdist outside source tree | None | 2–3 days |
| W03 / `metacrafter-4` | Use SQLAlchemy reflected tables/select/limit with schema-qualified report identities; fix native DuckDB schema selection; context-managed connections/results and Mongo client cleanup | Source identity contract from W04 | 3–6 days |
| W04 / `metacrafter-5` | Versioned canonical report and legacy readers; round-trip export; aggregate JSON/YAML and one-header CSV with source IDs; confirmed write outcomes and idempotent merging | W01; reuse typed-model work already proposed | 4–7 days |
| W05 / `metacrafter-6` | Shared SDK/API request options and validation; initialize once per worker; body/record/depth bounds; readiness, sanitized errors, deployable WSGI factory | W01/W04 contracts | 3–5 days |
| W06 / `metacrafter-7` | Typed rule/group schema, boolean PII, inclusive thresholds, finite ranges, consistent empties, strict load report, stable ordering and explicit override policy | Regression fixtures from W01/W09 | 3–5 days |
| W07 / `metacrafter-8` | Bounded head/reservoir sampling; incremental statistics with capped/approximate cardinality where requested; sample provenance and cancellation | W01/W04/W06; separate behavior proposal | 5–8 days |
| W08 / `metacrafter-9` | Immediately redact secrets; then explicit LLM failure policies, schema/ID validation, PII enrichment, local embedding backend, atomic fingerprinted indexes, budgets/timeouts | W04/W06; offline backend needs its own acceptance tests | 6–10 days in separate slices |
| W09 / `metacrafter-10` | Hermetic fixtures, optional-dependency jobs, default registry collection, meaningful core assertions, installed-package smoke tests, staged lint/type/coverage gates | Begins immediately; new gates depend on measured clean baseline | 3–5 days |
| W10 / `metacrafter-11` | Shared scan service, file/SQL/Mongo adapters, configuration resolver, LLM bridge and result writers; remove importlib collision while keeping compatible imports | W01/W04/W09; extract one responsibility per change | 5–8 days |
| W11 / `metacrafter-13` | Unify registry resolver and cached optional enrichment; correct architecture/examples/format matrix; reconcile roadmap and proposal statuses | W02/W04/W05; registry service validation is separate | 2–4 days |

### Phase A — make results and releases trustworthy

Start W01, W02, W04 interchange/failure accounting, W06 threshold/PII fixes, W09 isolation and W08 secret redaction. These directly address incorrect results, broken installation/export workflows, and observable failures. Repair the reproduced SQL quoting/schema problems as soon as their fixtures are available.

**Exit criteria:** wheel installs retain a known built-in email/UUID detection outside the checkout; canonical statistics/PII agree across interfaces; scan → saved JSON → exporter works; failed catalog operations are visible; full-suite failures are classified and deterministic; the targeted regressions fail on the old implementation and pass on the fixes.

### Phase B — establish shared contracts and bounded execution

Complete W03–W07 and incrementally start W10. Use a single scan service from both SDK and HTTP, with source adapters returning records and source metadata. Writers consume reports instead of deciding scan semantics. Implement explicit partial-result/error/cancellation behavior before introducing concurrent bulk execution.

**Exit criteria:** API/SDK parity fixtures pass; identical table names in different schemas remain separate; head sampling retains at most the configured sample; reservoir sampling reads the stream but retains bounded state; limits and statistics scope are explicit; bulk output parses and records failed/skipped sources.

### Phase C — improve LLM and operational capabilities

Finish W08 and W11: fully local embedding/classification, consistent registry metadata, index manifests, per-stage/rule timing, and cost/request budgets. The API can expose hybrid mode only after its behavior matches SDK semantics and reports the effective mode.

**Exit criteria:** offline integration test succeeds with all cloud HTTP blocked; changed registry/model/dimension invalidates the index; embedding failure preserves the previous usable index; response IDs are validated; PII and field filters survive both LLM paths; requested/effective mode and degradation appear in the report.

### Phase D — ship selected product additions

Implement F01–F04 first. Treat later adapters, dashboards and authoring tools as separately approved feature slices with concrete users. Use `metacrafter-12` as the umbrella feature backlog, not as a claim that these features are implemented.

## 6. Proposed new features

All command names below are **proposed interfaces**. None should be documented as available until implementation and release verification.

| ID / feature | User outcome and initial scope | Dependencies | Acceptance criteria / priority |
|---|---|---|---|
| F01 — scan report diff | Compare two saved reports; detect added/removed fields, semantic/PII changes and configurable statistical deltas. Proposed `metacrafter report diff old.json new.json`. | W04/W07 source IDs, schema and sampling provenance | Same reports produce an empty deterministic diff; incompatible sampling/rules versions are flagged; JSON and concise CLI output; configurable pipeline exit policy. **High** |
| F02 — consolidated PII inventory | One file/SQL/Mongo/bulk report listing dataset, field, datatype, confidence/evidence and suggested severity; include source failures and scan completeness. | W04/W06/W07, accurate PII | Custom registry PII types are included; a failed source cannot look like a clean scan; counts separate sampled observations from whole-dataset estimates; source IDs remain stable. **High** |
| F03 — rule lint/test/explain | `rules lint` validates schema/imports/collisions; `rules test` executes positive/negative examples; `scan explain` shows matched rule IDs, counts and rejected candidates. | W06 and report evidence | Broken mandatory validator fails strict lint; a checksum-invalid negative case is rejected; output identifies rule file/version and reasons without exposing raw PII by default. **High** |
| F04 — pipeline policy checks | Evaluate saved reports against a small YAML policy: prohibited PII, required labels, thresholds, completeness. Proposed `metacrafter check report.json --policy policy.yaml`. | F01/F02, W04 | Explicit machine-readable violations and exit codes; unknown/incomplete evidence is handled by policy; input scanning is not repeated unnecessarily. **High** |
| F05 — rule draft generation | Generate a YAML draft from a sample column or registry examples; suggest field aliases, length/charset constraints and validator references. Productize the pending skeleton script. | F03, sanitized samples, reviewed registry snapshot | Draft compiles, carries provenance plus positive/negative examples, preserves leading zeros, and requires author review before activation; generated regex must not be assumed equivalent to a checksum validator. **Medium** |
| F06 — evaluation and calibration | Dataset-based per-datatype precision/recall, false positives/negatives, coverage, abstention and timing. Start with CLI/JSON reports before a dashboard. | Labeled fixtures, F03/W07 | Frozen evaluation set and train/tuning separation; field-name evidence and value match share are reported separately; confidence percentages are not presented as calibrated probabilities until validated. **Medium** |
| F07 — optional ecosystem adapters | Export policy/PII results into existing workflows, initially one validated dbt metadata or data-quality integration; package supported validators through optional entry points when needed. | W04, F04, demonstrated adopter | Contract fixture and versioned adapter; optional install does not burden core; dry-run previews and reruns preserve existing metadata. **Later** |

New country-specific rules belong primarily in `metacrafter-rules`; canonical datatype metadata belongs in `metacrafter-registry`. Select additions using measured coverage gaps and user datasets, then verify rule-key/registry-ID alignment. Do not carry the July roadmap's sibling-repository gap counts forward without a fresh check.

## 7. Target implementation contracts

### Results and confidence

Use named `ScanOptions`, `ScanReport`, `DatasetResult`, `FieldResult`, `FieldStats`, `RuleMatch`, and `ScanError` models at boundaries. Preserve existing dictionary serialization through a compatibility adapter and schema versioning. Do not replace `scan_data()`'s public return type abruptly.

Each report should record source identity, engine/rule-pack/registry versions or hashes, effective options/mode, sample method/seed/counts, timing, warnings, completeness and errors. Match evidence should distinguish field-name inference, value validation, date parsing and LLM prediction. Keep matched/eligible/empty/missing counts so denominators are reviewable. Choose one confidence scale at the public boundary, use finite values and inclusive minimum thresholds, and document that field-name 100% is evidence strength rather than proven value accuracy.

### Rules and configuration

Resolve configuration once with explicit argument → environment → project config → home config → packaged default precedence, distinguishing an omitted CLI value from an explicit value equal to the default. Report effective paths and settings with secrets removed. Keep `auto_rules: false` usable for exact custom packs. Sorted discovery plus an explicit collision policy must replace accidental filesystem precedence; preserve current first-wins behavior initially unless a reviewed migration changes it.

Validate YAML before compilation, normalize actual booleans/context memberships, and emit a structured load report with successful/skipped/error counts. A required validator or field guard that cannot load must disable that rule or fail strict startup. Add an explicit permissive mode only with visible degradation. Treat Python validators as trusted extensions; constrain PyParsing expressions structurally instead of rejecting substrings within literals.

### Sources, errors and deployment

Separate rows read, sample rows retained, values tested and statistics population. Decide whether statistics describe the sample or full stream; name that scope in output. Preserve numeric precision/type tags and distinguish missing from null/empty values. Define scalar arrays, arrays of objects, literal dots in field names and flattened-path collisions before implementing the shared walker.

Use dialect-aware SELECT and schema identifiers, retain `(database, schema, table)` identity, and close every source handle on success, failure or cancellation. SQLAlchemy provides structured SELECT and limit constructs for this purpose. [SQLAlchemy SELECT reference](https://docs.sqlalchemy.org/en/20/core/selectable.html)

Expose partial failures as structured outcomes; provide documented CLI exit codes for invalid input, scan failure, partial batch failure and policy violations. The existing nonexistent-file CLI check already exits nonzero, so preserve it. Route logs/progress to stderr when stdout carries structured output; emit one JSON value or explicitly selected JSONL, and write result files atomically where feasible.

Ship YAML as package data and resolve installed resources independently of CWD; `include-package-data = true` alone has not included these rules in the inspected wheel. [Setuptools package-data guidance](https://setuptools.pypa.io/en/latest/userguide/datafiles.html)

Keep local server convenience mode, and publish a WSGI application factory plus deployment instructions for a production WSGI server. Add request bounds, health/readiness and opt-in authentication when remote deployment is supported. Default debug disabled is already delivered and does not by itself provide production hosting. [Flask deployment guidance](https://flask.palletsprojects.com/en/stable/deploying/)

### LLM and catalog operations

Local embedding/classification must be a complete offline path. Add explicit sample policies (field names only, redacted examples, approved raw examples), per-provider budgets/timeouts, accepted-ID/schema checks and a configurable failure policy. Record fallback rather than silently reporting the requested mode as if it succeeded. Do not persist sample values in report/index caches by default.

Fingerprint registry content, embedding backend/model/dimension and normalization version in an index manifest. Build a new index, validate it, and atomically switch only after success; never insert zero vectors as a silent substitute for failed embeddings. Apply all requested filters with documented AND/OR semantics and verify retrieval recall against exact filtering fixtures.

Exporters consume the same report contract and filtered matches. They honor explicit PII metadata, merge existing tags/terms/properties, and return attempted/succeeded/skipped/failed outcomes per operation. Add dry-run output and contract tests against declared supported catalog versions before claiming live compatibility.

## 8. Testing and quality gates

1. Create fixtures that isolate project/home configuration and extended-pack discovery without modifying the user's real configuration. Restore logging state and use injected services instead of reconstructing the full stack for every unit test.
2. Split base, LLM, SQL/format and catalog dependency jobs. Missing extras get explicit skip behavior in the base job; corresponding optional jobs must execute their contract tests with pinned compatible versions and mocked HTTP.
3. Rename `tests/tests_registry.py` or configure collection explicitly. Replace `try/except: pass` and assertions allowing empty/error outcomes with assertions about actual classifications, counts and serialized values.
4. Add golden cross-interface tests for W01/W04/W05 and negative fixtures for confidence boundaries, missing keys, empty samples, validators, nested arrays and malformed input. Include scan → serialize → exporter round trips and injected write failures.
5. Build/install both wheel and sdist in clean venvs, run outside the repo, and verify built-in resources and each advertised extra. Constrain dependency ranges around tested APIs and test minimum/current supported versions.
6. Enforce critical lint immediately; normalize format in a separate change; add scoped type checking on new contracts and expand coverage incrementally. Establish a clean branch/line coverage baseline first, then ratchet it; do not arbitrarily substitute a high global percentage for missing behavior tests.
7. Add format/codec compatibility tests, container-backed PostgreSQL/Mongo tests and schema/identifier fixtures; compile other advertised dialect queries without requiring live services. Build Docusaurus and validate command/example links on PRs.
8. Benchmark wide tables, high cardinality, late-appearing fields, nested records, compressed inputs and failing validators. Track peak RSS, throughput, p50/p95 scan latency, rule timings and LLM request/token counts. Set regression budgets after collecting a reproducible baseline.

Release gates should include a deterministic passing supported matrix, no unexpected optional-job skips, verified package-resource detection, SDK/API parity, parseable outputs, truthful failure accounting, and maintained classification precision/recall on a frozen evaluation set. No accuracy percentage or throughput target is claimed by this review.

## 9. OpenSpec reconciliation

`openspec list` reports eight active changes; `openspec list --specs` reports no current capability specs. Seven proposals are present only as pre-existing untracked work in the original checkout. Use actual code/release evidence to reconcile checklist status before implementation.

| Existing change | Delivered or present | Remaining work to refine |
|---|---|---|
| `stabilize-metacrafter-reliability` | Flask dependency, top-level exit-code guard, comma filters, debug default and Rich output | Broader partial-failure contracts and standalone install checks; avoid repeating delivered items |
| `modernize-metacrafter-packaging` | pyproject metadata, extras, thin setup shim, public lazy SDK export | YAML package resources, direct deps, install matrices, typed compatible results |
| `add-rules-optional-extra` | Extra and discovery logic; tracked proposal | Verify published extension-pack compatibility and exact composition/override semantics |
| `improve-metacrafter-ci` | Local uncommitted blocking lint/50% edit; registry HTTP tests are mocked | Collection, isolation, meaningful assertions, optional matrix and measured gates |
| `refactor-metacrafter-core` | Helpers and output module exist; command functions moved | Orchestration/source extraction, removal of dynamic import collision; CLI app objects still remain in core |
| `expand-metacrafter-llm-platform` | Provider implementations and partial environment registry configuration | Complete resolver wiring, offline embeddings, server parity, PII/index/privacy/error contracts |
| `add-ecosystem-alignment-workflow` | Untracked script and workflow present locally | Validate/land that work independently, add reproducible snapshots and built-in collision checks |
| `add-ecosystem-product-features` | Custom function-rule mechanism exists; skeleton script is untracked | F01–F07 as small independently testable slices; generated stubs are not finished validators |

A focused draft, [improve-scan-result-integrity](openspec/changes/improve-scan-result-integrity/proposal.md), accompanies this review for the most urgent result/statistics/PII/interchange corrections. Its tasks remain unchecked. It should coordinate with existing packaging/API proposals rather than duplicate their implementation.

Create or refine separate feature/architecture proposals for bounded sampling, offline LLM behavior and F01–F07 before implementation. Bug fixes that restore an already specified contract can remain narrow. Follow the repository's proposal approval gate for significant behavior changes. This review requests no implementation approval and does not change production behavior.

## 10. Tracking and handoff

The review task is `metacrafter-1`; W01–W11 are tracked by the Beads IDs above and feature development by `metacrafter-12`. Follow-up issues contain scope and acceptance criteria; leave them open until their implementation and validation complete.

Start the next implementation session with **W01 statistics/field coverage and W02 installed rules**, using the draft result-integrity proposal and package smoke fixtures. Add W04 report interchange immediately afterward, since it is the prerequisite for useful exports, diffing and PII inventory. Resolve W09 isolation early so future green tests represent the requested pack/dependencies rather than the developer's home environment.

Preserve existing imports and report readers during migration, split formatting from behavior fixes, and update documentation with verified examples for each delivered slice. Archive OpenSpec changes only after the corresponding functionality is verified and released; a checked task or script present on disk is insufficient evidence of delivery.
