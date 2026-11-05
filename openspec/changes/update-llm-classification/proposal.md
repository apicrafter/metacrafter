# Change: Registry-driven LLM classification

## Why

The LLM mode embeds the legacy `datatypes_latest.jsonl`, found through a relative path. It
treats negative examples (`valid: false`) as positive ones. Retrieval uses vector search
only, and the index is never invalidated when the registry changes; the separate
`metacrafter-llm` index holds 407 of 1812 records. Model answers are free text parsed as
JSON, and an invented datatype id is accepted (Codex review Q12). Five provider classes
duplicate the same OpenAI-compatible HTTP call.

The registry now provides better inputs:
- a deterministic shortlist through `Registry.identify()` and `search()`;
- a versioned RAG corpus (`/api/v1/rag/corpus.jsonl`) with a manifest hash;
- explicit negative examples;
- an authoritative `is_pii` flag.

This change is phase 4b of `devdocs/MODERNIZATION_PLAN.md`.

## What Changes

- **Candidates first.** Build a shortlist per field with `Registry.identify()` (values and
  field name) and `Registry.search()` (field name and description tokens). Vector
  retrieval becomes an optional addition, not a requirement.
- **Corpus.** Use the registry RAG corpus or package records. Fingerprint the index with
  the manifest SHA-256 and rebuild it when the fingerprint changes. ChromaDB becomes an
  optional backend; the default is an in-memory index (1812 records).
- **Negative examples.** Present them to the model as counter-examples, never as positive
  evidence.
- **Constrained output.** Use structured output or tool calling with a schema whose
  `datatype_id` is restricted to the shortlist plus `null`. Validate the answer against the
  registry and resolve aliases. Reject ids outside the shortlist.
- **Rules first in hybrid mode.** Call the LLM only for fields without a match at or above
  the confidence threshold. Cache results by field name, a values hash and the registry
  version.
- **Providers.**
  - One OpenAI-compatible client covers OpenAI, OpenRouter, Ollama, LM Studio and Perplexity.
  - A native Anthropic provider is added.
  - The duplicated provider classes are removed.
  - Existing `llm_provider` values keep working as presets.
- **PII-aware disclosure.** For a field whose shortlist contains a PII datatype, raw values
  are sent to a non-local provider only when the user enables it explicitly. Otherwise the
  prompt carries the field name, statistics and masked value shapes.
- LLM matches carry `ruletype: llm`, registry enrichment, `pii_source` and an `evidence`
  object with the shortlist and model id.

## Relationship to other changes

- **Depends on** `update-llm-trust-and-offline-index` (Codex W08, Beads `metacrafter-9`).
  That change provides result validation, secret redaction, sample policies, local
  embeddings, verified atomic indexes and filter metadata. This change builds on those
  requirements and changes where candidates and corpus come from.
- **Depends on** `add-registry-integration` and `update-detection-engine` (shared
  confidence scale and evidence).
- **Supersedes** the LLM items of the July `expand-metacrafter-llm-platform` (sections 2–3).
- Replaces the `metacrafter-llm` repository, which is archived in
  `refactor-metacrafter-foundation`.

## Impact

- Affected specs: `llm-classification`
- Affected code: `metacrafter/classify/llm/` (moved to `metacrafter/llm/`), `core.py` LLM
  bridge, config keys `llm_*`, docs `integrations/llm.md`
- Config: `llm_registry_path` is deprecated in favour of the registry adapter sources. The
  `chromadb` dependency moves from the `llm` extra to an `llm-chroma` extra.
