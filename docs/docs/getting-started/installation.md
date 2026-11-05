---
title: "Installation"
description: "Install Metacrafter with pip and optional extras"
---
# Installation

### Requirements

- Python 3.10 or newer (CI tests 3.10–3.13)

### Using pip

```bash
pip install metacrafter
```

That is all you need. On first use, Metacrafter downloads its default rule pack from the
pinned [metacrafter-rules](https://github.com/apicrafter/metacrafter-rules) GitHub release
(about 280 KB). It checks the SHA-256 of the download and caches the pack in
`~/.cache/metacrafter/rules/`. Registry data is read from
[registry.apicrafter.io](https://registry.apicrafter.io) and cached the same way. Neither
project is a Python dependency.

To prepare an offline machine, run these on a machine with network access and copy the
cache or the snapshot directory:

```bash
metacrafter rules fetch                         # download and verify the rule pack
metacrafter registry fetch --to ./registry      # registry snapshot for registry_path
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

The default rule pack never comes from the current directory. Its sources, in order:

1. `rules_dir` in `.metacrafter` (or `METACRAFTER_RULES_DIR`): a metacrafter-rules
   checkout or an unpacked pack;
2. an installed `metacrafterext` package;
3. the cache;
4. a download of the pinned release.

Set `rules_offline: true` to forbid downloads. `rules_release` with `rules_sha256` selects
another release. Set `auto_rules: false` to load only the paths you list in `rulepath`.

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
| `rules` | No-op, kept for compatibility: the rule pack is downloaded from GitHub releases |
| `all` | Every optional feature above |
| `dev` | pytest, coverage, ruff, mypy, build |

### From source

```bash
git clone https://github.com/apicrafter/metacrafter.git
cd metacrafter
pip install -e '.[dev,formats]'
metacrafter rules fetch
```

To work on rules at the same time, point `rules_dir` (or `METACRAFTER_RULES_DIR`) at a
metacrafter-rules checkout. `ecosystem.lock` records the rule release and the registry
version that CI is tested against.

### Next steps

- [Quick start](/getting-started/quick-start)
- [Cookbook](/getting-started/cookbook)
- [CLI reference](/commands/)
