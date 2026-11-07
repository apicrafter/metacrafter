# llm-classification Specification

## Purpose
Optional LLM classification constrained to registry candidates, with validation, privacy controls and providers.
## Requirements
### Requirement: Validated transparent classification

LLM results SHALL use registered datatype IDs, finite confidence in range, configured confidence and selected nested fields; reports SHALL record requested and effective modes and explicit fallback policy.

#### Scenario: Unknown model label

- **WHEN** a provider returns an unknown datatype or non-finite confidence
- **THEN** the result is rejected with a diagnostic rather than emitted as a classification

### Requirement: Sample privacy and secrets

LLM classification SHALL offer names-only, redacted and explicitly selected raw sample policies, bounded sample counts and provider timeouts; logs SHALL redact credentials and API keys.

#### Scenario: Names-only policy

- **WHEN** a cloud provider classifies a field under the default names-only policy
- **THEN** no raw values are sent to embeddings or generation

### Requirement: Local embeddings and verified indexes

Retrieval SHALL support a local embedding provider, fingerprint registry and embedding configuration, reject incompatible indexes, and rebuild through a replacement collection without zero-vector fallbacks.

#### Scenario: Embedding failure during rebuild

- **WHEN** an embedding request fails while replacing an existing index
- **THEN** the usable index remains intact and the failure is reported

### Requirement: Complete filter metadata

Retrieval SHALL retain datatype PII metadata and match all supplied language, category and country filter values.

#### Scenario: Multiple language filters

- **WHEN** a candidate matches the second requested language
- **THEN** the candidate is eligible and keeps its PII metadata

### Requirement: Registry Candidate Shortlist

Before calling a language model for a field, Metacrafter SHALL build a shortlist of
candidate datatypes from the registry using value identification and field-name search.
Vector retrieval SHALL be optional. LLM classification SHALL work with no embedding
backend configured.

#### Scenario: Local model without embeddings

- **WHEN** the LLM mode runs with the `ollama` preset and no embedding backend
- **THEN** each unresolved field gets a shortlist from the registry
- **AND** no request is sent to any remote embedding service

### Requirement: Shortlist-Constrained Model Output

The model answer SHALL be requested as structured output with `datatype_id` restricted to
the shortlist ids or null. Metacrafter SHALL validate the answer against the registry,
resolve aliases, and reject any id outside the shortlist.

#### Scenario: Invented id rejected

- **WHEN** a provider returns a `datatype_id` that is not in the shortlist
- **THEN** the field gets no LLM match
- **AND** the report records the rejection reason in `evidence`

#### Scenario: Alias answer resolved

- **WHEN** a provider returns `guid` and `uuid` is in the shortlist
- **THEN** the match reports `datatype` as `uuid`

### Requirement: Negative Examples as Counter-Evidence

Prompts SHALL present registry examples marked `valid: false` as counter-examples, separate
from positive examples. Negative examples SHALL NOT be embedded or presented as positive
evidence.

#### Scenario: Prompt content

- **WHEN** a shortlisted record has positive and negative examples
- **THEN** the prompt lists them under separate labels

### Requirement: Corpus Fingerprint

Any LLM index SHALL be fingerprinted by the registry manifest hash of its corpus and the
embedding model id. An index whose fingerprint differs from the current registry SHALL be
rebuilt before use.

#### Scenario: Registry update

- **WHEN** the registry corpus hash changes after an update
- **THEN** the next LLM scan rebuilds the index before retrieval

### Requirement: Rules-First Hybrid Classification

In hybrid mode, Metacrafter SHALL call the language model only for fields that have no
match at or above the confidence threshold from rules and registry detectors. LLM results
SHALL be cached by field name, values hash and registry version.

#### Scenario: Field already labelled by rules

- **WHEN** a field has an `email` match at 95 confidence and the threshold is 50
- **THEN** no LLM request is made for that field

### Requirement: Unified Provider Client

LLM providers SHALL be configured through presets over one OpenAI-compatible client, plus
an Anthropic client. Existing `llm_provider` values (`openai`, `openrouter`, `perplexity`,
`ollama`, `lmstudio`) SHALL remain valid.

#### Scenario: Existing configuration

- **WHEN** `.metacrafter` sets `llm_provider: openrouter` and `llm_model`
- **THEN** classification uses the OpenAI-compatible client with the OpenRouter base URL

### Requirement: PII-Aware Sample Disclosure

When a field's shortlist contains a PII datatype and the provider is not local, Metacrafter
SHALL send masked value shapes instead of raw values, unless `llm_send_pii_values: true` is
set. The report SHALL record the disclosure mode for each field.

#### Scenario: Remote provider and PII candidate

- **WHEN** a field's shortlist contains `email`
- **AND** the provider is `openai`
- **AND** `llm_send_pii_values` is not set
- **THEN** the prompt contains masked shapes and no raw values
- **AND** `evidence.disclosure` is `masked`

### Requirement: LLM Verification of Value Matches

When LLM verification is enabled (`--llm-verify`, `llm_verify: true`), Metacrafter SHALL send one LLM
request per table after rule matching. The request SHALL list all column paths and, for each column with
value matches from rules or registry detectors, its type, distinct-value count, value samples under the
sample policy and the candidate datatypes with their registry descriptions. The answer schema SHALL allow
only each column's candidate ids. Value matches whose datatype the answer does not keep SHALL be removed;
kept matches SHALL carry `evidence.llm_verified: true`; a column missing from the answer SHALL keep its
matches. Field-name matches SHALL NOT be sent or removed. Answers SHALL be cached, requests SHALL count
against `llm_max_requests`, and failures SHALL follow `llm_fallback`.

#### Scenario: Rejected value match

- **WHEN** a column `package_id` of GUIDs has a `uuid` value match and the verifier does not keep `uuid`
- **THEN** the report has no `uuid` match for the column
- **AND** with `--explain` the rejected candidates give the reason "Rejected by the LLM verifier"

#### Scenario: Names-only policy

- **WHEN** verification runs with `--sample-policy names-only`
- **THEN** the request contains column paths and candidates but no values

#### Scenario: Failure with rules fallback

- **WHEN** the verification request fails and `llm_fallback` is `rules`
- **THEN** the rule matches are kept and the report is marked incomplete

### Requirement: LLM Request Budget

A scan SHALL make at most `llm_max_requests` LLM requests (default 100), set in
`.metacrafter` or with `--llm-max-requests`. Cached answers and fields without registry
candidates SHALL NOT count. When the fields to classify exceed the remaining budget, the
scan SHALL log a warning. When the budget is exhausted, a scan with `llm_fallback: error`
SHALL fail with a message that names the budget, `llm_max_requests` and
`--llm-fallback rules`; a scan with `llm_fallback: rules` SHALL classify the remaining
fields with rules.

#### Scenario: More fields than the budget

- **WHEN** a scan with `--llm-only` and `--llm-max-requests 1` needs two LLM requests
- **THEN** it fails with "LLM request budget of 1 requests exceeded"

#### Scenario: Budget from configuration

- **WHEN** `.metacrafter` sets `llm_max_requests: 250`
- **THEN** the scan allows 250 LLM requests unless `--llm-max-requests` is given

