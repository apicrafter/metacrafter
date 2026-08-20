# Metacrafter

Python command-line tool and library to label table fields and data files. Use it to find meaningful columns in tables — or to find personal identifiable information (PII).

## Documentation

The full documentation site (Docusaurus) lives in [`docs/`](docs/) and is published at **[apicrafter.github.io/metacrafter](https://apicrafter.github.io/metacrafter/)**.

| Section | What it covers |
|---------|----------------|
| [Getting started](https://apicrafter.github.io/metacrafter/getting-started/installation) | Install, quick start, positioning |
| [Cookbook](https://apicrafter.github.io/metacrafter/getting-started/cookbook) | Task index by role |
| [CLI reference](https://apicrafter.github.io/metacrafter/commands/) | Every command |
| [Formats](https://apicrafter.github.io/metacrafter/formats/) | Files and compression codecs |
| [Python SDK](https://apicrafter.github.io/metacrafter/integrations/sdk) | `CrafterCmd` API |
| [LLM classification](https://apicrafter.github.io/metacrafter/integrations/llm) | Hybrid rules + RAG |
| [Troubleshooting](https://apicrafter.github.io/metacrafter/getting-started/troubleshooting) | Common errors |

Source pages: [`docs/docs/`](docs/docs/). Changelog: [`CHANGELOG.md`](CHANGELOG.md).

## Installation

```bash
pip install metacrafter
```

Optional extras:

```bash
pip install 'metacrafter[rules]'         # extended YAML rules pack
pip install 'metacrafter[llm]'           # LLM/RAG hybrid classification
pip install 'metacrafter[datahub]'       # DataHub export
pip install 'metacrafter[openmetadata]'  # OpenMetadata export
pip install 'metacrafter[all]'           # rules + integrations
pip install 'metacrafter[dev]'           # tests and linters
```

When `metacrafter[rules]` is installed, the extended pack loads automatically.
Set `auto_rules: false` in `.metacrafter` to use only paths you list.

```python
from metacrafter import CrafterCmd
```

## Features

- YAML rules for field names and values (text, PyParsing, Python functions)
- PII, person names, identifiers (UUID/GUID, email, phone), dates (312+ patterns via [qddate](https://github.com/ivbeg/qddate)), and country-specific types
- Files via [iterabledata](https://github.com/datenoio/iterabledata): CSV, TSV, JSON/JSONL, XML, Parquet, Avro, ORC, Excel, BSON, Pickle, plus gzip/bzip2/xz/lz4/zstd/brotli/snappy/zip
- SQL databases (SQLAlchemy) and MongoDB
- Context and language filters so only relevant rules run
- Built-in HTTP API (`metacrafter server run`)
- Optional LLM/RAG classification (OpenAI, OpenRouter, Ollama, LM Studio, Perplexity)
- Export to DataHub, OpenMetadata, and Apache Atlas
- Datatype URLs from [metacrafter-registry](https://github.com/apicrafter/metacrafter-registry)

## Quick start

```bash
metacrafter scan file somefile.csv --format short

metacrafter scan file users.csv \
  --contexts pii \
  --langs en \
  --confidence 20.0 \
  --format full \
  -o pii_scan.json

metacrafter scan sql "sqlite:///users.db" --format full -o db.json

metacrafter rules list
metacrafter server run --host 127.0.0.1 --port 10399
```

More examples: [Quick start](https://apicrafter.github.io/metacrafter/getting-started/quick-start) and [Cookbook](https://apicrafter.github.io/metacrafter/getting-started/cookbook).

## Ecosystem

- [metacrafter-rules](https://github.com/apicrafter/metacrafter-rules) — extra validators and YAML rules
- [metacrafter-registry](https://github.com/apicrafter/metacrafter-registry) — canonical semantic types at [registry.apicrafter.io](https://registry.apicrafter.io)

## Commercial support

Contact ibegtin@apicrafter.io or ivan@begtin.tech for a commercial API with additional rules and dedicated support.

## License

Apache-2.0. See [LICENSE](LICENSE).
