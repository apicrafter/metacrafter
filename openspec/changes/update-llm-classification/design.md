## Context

Current pipeline:
1. Embed every registry record (OpenAI embeddings) into ChromaDB.
2. For each field, embed field name and samples, then retrieve the top 10.
3. Prompt the model with the entries and parse JSON from free text.

This pipeline has four problems:
- it needs a cloud embedding provider even with local LLMs;
- it ignores registry status and negative examples;
- it trusts the model's id;
- it sends raw samples by default.

## Goals / Non-Goals

**Goals:**
- Deterministic, cheap candidate generation; the LLM only disambiguates.
- Outputs constrained to real registry ids.
- No raw PII to external providers without explicit consent.
- Works fully offline with a local model and no embeddings.

**Non-Goals:**
- Fine-tuning or training models.
- Using LLMs to write rules (`metacrafter-rule-author` skill covers that).

## Decisions

### Decision: Pipeline

```
field (name, sample values, stats)
  └─ shortlist = identify(values, name, country) ∪ search(name tokens)   top-k (k=15)
       ├─ optional: + vector neighbours (if an embedding backend is configured)
       └─ prompt(shortlist records: id, name, doc, positive examples, negative examples)
            └─ structured output {datatype_id ∈ shortlist ∪ {null}, confidence, reason}
                 └─ validate + resolve alias + enrich from registry → Match(ruletype=llm)
```

LLM confidence is mapped onto the engine scale: `score = 100 × model_confidence × 0.8`,
capped at 80. A rule match with strong value evidence then outranks an unverified LLM
answer.

### Decision: Corpus and index fingerprint

- The corpus source is the registry adapter (`add-registry-integration`).
- The fingerprint is the manifest SHA-256 of `rag/corpus.jsonl` (or of `datatypes.jsonl`)
  plus the embedding model id.
- A rebuild writes to a temporary directory and swaps it in atomically, as required by
  `update-llm-trust-and-offline-index`.

### Decision: Providers

| Preset | Client | Default base URL |
|---|---|---|
| `openai` | OpenAI-compatible | https://api.openai.com/v1 |
| `openrouter` | OpenAI-compatible | https://openrouter.ai/api/v1 |
| `perplexity` | OpenAI-compatible | https://api.perplexity.ai |
| `ollama` | OpenAI-compatible | http://localhost:11434/v1 |
| `lmstudio` | OpenAI-compatible | http://localhost:1234/v1 |
| `anthropic` | Anthropic SDK | https://api.anthropic.com |

Presets only set defaults. `llm_base_url` and `llm_model` override them. Local presets
(`ollama`, `lmstudio`, or any base URL on localhost) count as local for the disclosure
policy.

### Decision: PII-aware disclosure

| Shortlist contains PII datatype | Provider local | Values sent |
|---|---|---|
| no | any | up to 10 raw values |
| yes | yes | up to 10 raw values |
| yes | no | masked shapes only (e.g. `Aaaa.Aaaa@aaaa.aaa`), unless `llm_send_pii_values: true` |

The report records the disclosure mode per field in `evidence`.

## Risks / Trade-offs

- **The shortlist may miss the true type, so the LLM cannot recover it.** Mitigation: add
  `search()` hits on name tokens; optional vector neighbours; report `null` rather than a
  forced pick.
- **Structured output support differs by provider.** Mitigation: fall back to JSON mode
  plus schema validation; reject non-conforming answers.
