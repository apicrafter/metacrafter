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

That is all you need. The default rule pack is the pinned
[metacrafter-rules](https://github.com/apicrafter/metacrafter-rules) GitHub release. The
package includes a copy of its archive (about 280 KB), so the rule pack needs no network
access. On first use, Metacrafter checks the archive's SHA-256 and unpacks it into
`~/.cache/metacrafter/rules/`. Registry data is read from
[registry.apicrafter.io](https://registry.apicrafter.io) and cached. The package ships a
registry snapshot (about 550 KB), so the first scan does not wait for the registry download;
the cache is refreshed after 24 hours. Neither project is a Python dependency.

To prepare an offline machine, write a registry snapshot on a machine with network access
and copy it:

```bash
metacrafter registry fetch --to ./registry      # registry snapshot for registry_path
```

Optional features are available as install extras:

```bash
pip install 'metacrafter[formats]'       # Parquet, ORC, Excel, ODS, XML, Shapefile, lz4/zstd/brotli
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
4. the archive bundled with the package (pinned release only);
5. a download of the configured release.

Set `rules_offline: true` to forbid downloads. `rules_release` with `rules_sha256` selects
another release; it is downloaded from GitHub (`metacrafter rules fetch --release TAG
--sha256 HEX`). Set `auto_rules: false` to load only the paths you list in `rulepath`.

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
| `rules` | No-op, kept for compatibility: the rule pack comes from GitHub releases |
| `all` | Every optional feature above |
| `dev` | pytest, coverage, ruff, mypy, build |

### From source

```bash
git clone https://github.com/apicrafter/metacrafter.git
cd metacrafter
pip install -e '.[dev,formats]'
```

To work on rules at the same time, point `rules_dir` (or `METACRAFTER_RULES_DIR`) at a
metacrafter-rules checkout. `ecosystem.lock` records the rule release and the registry
version that CI is tested against.

### Next steps

- [Quick start](/getting-started/quick-start)
- [Cookbook](/getting-started/cookbook)
- [CLI reference](/commands/)
