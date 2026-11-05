## 1. Candidates and corpus

- [ ] 1.1 Implement the shortlist from `Registry.identify()` and `Registry.search()` with configurable k
- [ ] 1.2 Make vector retrieval optional; add an in-memory default index; move ChromaDB to extra `llm-chroma`
- [ ] 1.3 Load the corpus through the registry adapter; fingerprint by manifest SHA-256 and embedding model id
- [ ] 1.4 Deprecate `llm_registry_path` with a warning that points to `registry_path`

## 2. Prompt and output

- [ ] 2.1 Include positive and negative examples separately in the prompt
- [ ] 2.2 Use structured output or tool calling with `datatype_id` constrained to the shortlist and `null`
- [ ] 2.3 Validate the answer, resolve aliases, enrich from the registry and set `ruletype: llm`
- [ ] 2.4 Map LLM confidence onto the engine scale (cap 80) and fill `evidence`

## 3. Hybrid flow and cache

- [ ] 3.1 Call the LLM only for fields below the confidence threshold after rules and registry detectors
- [ ] 3.2 Cache results by field name, values hash and registry version

## 4. Providers and disclosure

- [ ] 4.1 Replace provider classes with one OpenAI-compatible client and presets
- [ ] 4.2 Add an Anthropic provider
- [ ] 4.3 Implement the PII-aware disclosure table and `llm_send_pii_values`
- [ ] 4.4 Record the disclosure mode in `evidence`

## 5. Tests and docs

- [ ] 5.1 Fake-provider tests: invented id rejected, alias resolved, null answer kept
- [ ] 5.2 Test: PII shortlist plus remote provider sends no raw values by default
- [ ] 5.3 Test: corpus change triggers an index rebuild
- [ ] 5.4 Update `docs/docs/integrations/llm.md`
