---
title: "Registry"
description: "Link scan results to metacrafter-registry datatype metadata"
---
# Registry

[metacrafter-registry](https://github.com/apicrafter/metacrafter-registry) is the
canonical catalog of semantic datatypes. Scan output includes
`datatype_url` values such as
`https://registry.apicrafter.io/datatype/email`.

Rule `key` values should match registry `id` values so those URLs resolve.

## URL resolution

1. `METACRAFTER_REGISTRY_URL`
2. `registry_url` in `.metacrafter`
3. default `https://registry.apicrafter.io`

Self-hosted / air-gapped:

```yaml
registry_url: http://localhost:8089
```

```bash
export METACRAFTER_REGISTRY_URL=http://localhost:8089
```

## LLM index

Hybrid and LLM modes embed registry JSONL (`datatypes_latest.jsonl`) into
ChromaDB. Point `--llm-registry` or `llm_registry_path` at a local copy when
you cannot fetch the public registry.

## Rule skeletons

`scripts/generate_rule_skeletons.py` emits starter YAML for registry datatypes
that have a regexp but no rule yet:

```bash
METACRAFTER_REGISTRY_DIR=../metacrafter-registry \
METACRAFTER_RULES_DIR=../metacrafter-rules \
python scripts/generate_rule_skeletons.py --category geo --limit 20 -o skeletons.yaml
```

Review every skeleton before using it. See [Custom rules](/integrations/rules).
