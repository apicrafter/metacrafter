---
title: "Installation"
description: "Install Metacrafter with pip and optional extras"
---
# Installation

### Using pip

```bash
pip install metacrafter
```

Optional features are available as install extras:

```bash
pip install metacrafter               # core (built-in rules, CLI, server)
pip install 'metacrafter[rules]'      # + extended rules pack (metacrafter-rules)
pip install 'metacrafter[llm]'        # + LLM/RAG hybrid classification
pip install 'metacrafter[datahub]'    # + DataHub export integration
pip install 'metacrafter[openmetadata]'  # + OpenMetadata integration
pip install 'metacrafter[all]'        # extended rules + all optional integrations
pip install 'metacrafter[dev]'        # development + test tooling
```

When `metacrafter[rules]` is installed, Metacrafter automatically loads the
extended `metacrafter-rules` YAML pack. Set `auto_rules: false` in `.metacrafter`
to use only paths you list explicitly.

The stable public Python API is importable from the package:

```python
from metacrafter import CrafterCmd
```

### Optional extras

| Extra | Enables |
|-------|---------|
| `rules` | Extended YAML rules and validators from [metacrafter-rules](https://github.com/apicrafter/metacrafter-rules) |
| `llm` | LLM/RAG hybrid classification (OpenAI, ChromaDB, requests) |
| `datahub` | Export scan results to DataHub (`acryl-datahub`) |
| `openmetadata` | Export scan results to OpenMetadata |
| `all` | Extended rules plus LLM and catalog integrations |
| `dev` | pytest, coverage, flake8, black, mypy |

### From source

```bash
git clone https://github.com/apicrafter/metacrafter.git
cd metacrafter
pip install -e '.[dev]'
```

### Requirements

- Python 3.8 or newer
- CI currently tests Python 3.8–3.12

### Next steps

- [Quick start](/getting-started/quick-start)
- [Cookbook](/getting-started/cookbook)
- [CLI reference](/commands/)
