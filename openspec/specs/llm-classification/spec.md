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

