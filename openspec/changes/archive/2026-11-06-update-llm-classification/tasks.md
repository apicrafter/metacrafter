## 1. Candidates and corpus

- [x] 1.1 Implement the shortlist from `Registry.identify()` and `Registry.search()` with configurable k
- [x] 1.2 Make vector retrieval optional (the default pipeline needs no index); move ChromaDB to extra `llm-chroma`
- [x] 1.3 Load the corpus through the registry adapter; cache keys include the registry version and git sha (the deprecated vector index keeps its own fingerprint)
- [x] 1.4 Deprecate `llm_registry_path` with a warning that points to `registry_path`

## 2. Prompt and output

- [x] 2.1 Include positive and negative examples separately in the prompt
- [x] 2.2 Use structured output or tool calling with `datatype_id` constrained to the shortlist and `null`
- [x] 2.3 Validate the answer, resolve aliases, enrich from the registry and set `ruletype: llm`
- [x] 2.4 Map LLM confidence onto the engine scale (cap 80) and fill `evidence`

## 3. Hybrid flow and cache

- [x] 3.1 Call the LLM only for fields below the confidence threshold after rules and registry detectors
- [x] 3.2 Cache results by field name, values hash and registry version

## 4. Providers and disclosure

- [x] 4.1 Replace provider classes with one OpenAI-compatible client and presets
- [x] 4.2 Add an Anthropic provider
- [x] 4.3 Implement the PII-aware disclosure table and `llm_send_pii_values`
- [x] 4.4 Record the disclosure mode in `evidence`

## 5. Tests and docs

- [x] 5.1 Fake-provider tests: invented id rejected, alias resolved, null answer kept
- [x] 5.2 Test: PII shortlist plus remote provider sends no raw values by default
- [x] 5.3 Test: a changed registry version changes the cache key (no index in the default pipeline)
- [x] 5.4 Update `docs/docs/integrations/llm.md`
