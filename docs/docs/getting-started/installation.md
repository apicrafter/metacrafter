---
title: "Installation"
description: "Install Metacrafter with pip and optional extras"
---
# Installation

### Requirements

- Python 3.10 or newer (CI tests 3.10–3.13)
- [metacrafter-rules](https://github.com/apicrafter/metacrafter-rules), the default rule pack. It is a
  required dependency.

### Using pip

`metacrafter-rules` is not published on PyPI yet. Install it from GitHub first, then
install Metacrafter:

```bash
pip install "metacrafter-rules @ git+https://github.com/apicrafter/metacrafter-rules"
pip install metacrafter
```

Optional features are available as install extras:

```bash
pip install 'metacrafter[formats]'       # Parquet, ORC, Excel, XML, lz4/zstd/brotli
pip install 'metacrafter[mongodb]'       # metacrafter scan mongodb, .bson files
pip install 'metacrafter[duckdb]'        # native DuckDB scanning, duckdb:// URLs
pip install 'metacrafter[server]'        # metacrafter server run
pip install 'metacrafter[llm]'           # LLM/RAG hybrid classification
pip install 'metacrafter[datahub]'       # DataHub export
pip install 'metacrafter[openmetadata]'  # OpenMetadata export
pip install 'metacrafter[all]'           # every optional feature
pip install 'metacrafter[dev]'           # tests and linters
```

A command that needs a missing extra stops with an error naming the extra, for example
`pip install 'metacrafter[mongodb]'`.

The default rule pack comes from the installed `metacrafter-rules` package, not from the
current directory. Set `auto_rules: false` in `.metacrafter` to load only the paths you
list in `rulepath`.

The stable public Python API is importable from the package:

```python
from metacrafter import CrafterCmd
```

### Optional extras

| Extra | Enables |
|-------|---------|
| `formats` | Parquet, Avro, ORC, Excel, XML and lz4/zstd/brotli files (via iterabledata) |
| `mongodb` | MongoDB scanning and BSON files (`pymongo`) |
| `duckdb` | Native DuckDB scanning (`duckdb`, `duckdb-engine`) |
| `server` | HTTP API server (FastAPI, uvicorn) |
| `mcp` | MCP server for AI agents |
| `llm` | LLM classification; adds the Anthropic SDK (OpenAI-compatible providers need nothing extra) |
| `llm-chroma` | Deprecated vector-index pipeline (`llm_registry_path`): OpenAI SDK, ChromaDB |
| `datahub` | Export scan results to DataHub (`acryl-datahub`) |
| `openmetadata` | Export scan results to OpenMetadata |
| `rules` | No-op, kept for compatibility: `metacrafter-rules` is a core dependency |
| `all` | Every optional feature above |
| `dev` | pytest, coverage, ruff, mypy, build |

### From source

```bash
git clone https://github.com/apicrafter/metacrafter.git
cd metacrafter
pip install "$(python scripts/ecosystem_ref.py metacrafter-rules)"
pip install -e '.[dev,formats]'
```

`ecosystem.lock` pins the `metacrafter-rules` commit that CI is tested against.

### Next steps

- [Quick start](/getting-started/quick-start)
- [Cookbook](/getting-started/cookbook)
- [CLI reference](/commands/)
