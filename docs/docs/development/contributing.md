---
title: "Contributing"
description: "Development setup, tests, and documentation"
---
# Contributing

Thank you for contributing to Metacrafter.

## Development setup

Prerequisites: Python 3.10+, Git, pip, Node.js 18+ (for the docs site).

```bash
git clone https://github.com/apicrafter/metacrafter.git
cd metacrafter
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -e '.[dev,llm,mongodb,duckdb,server,mcp,formats]'
metacrafter rules fetch        # download the pinned rule pack once
```

## Tests

```bash
pytest tests/
pytest --cov=metacrafter --cov-report=term-missing   # CI requires 60%
```

Tests are hermetic:
- `tests/conftest.py` resolves the rule pack once, from `METACRAFTER_RULES_DIR` or the cache
  filled by `metacrafter rules fetch`;
- each test then gets an empty home directory and no network access;
- the registry comes from the offline snapshot in `tests/fixtures/registry_snapshot`. Tests for optional integrations skip when their extra is
not installed. Measured on Python 3.11 with all extras: 400 passed, 10 skipped, about 71% line
coverage, about 20 seconds.

CI jobs (`.github/workflows/ci.yml`):

| Job | Checks |
|---|---|
| `test` | pytest with coverage on Python 3.10–3.13 |
| `quality` | ruff (syntax errors, undefined names), mypy on the report contracts |
| `installation` | builds the wheel and scans data from a clean environment outside the checkout (core, llm, catalogs) |
| `ecosystem` | rule keys, registry examples and checksum names against the live registry ([Ecosystem contract](/development/ecosystem)) |

## Code style

```bash
ruff check metacrafter tests scripts
mypy metacrafter/reports.py metacrafter/report --ignore-missing-imports
```

Follow existing patterns, add tests for new behavior, and keep changes
backward compatible when possible. Type hints are welcome.

## Documentation

User-facing docs live in `docs/docs/` (Docusaurus). After editing markdown:

```bash
cd docs
npm install
npm start          # live preview
npm run build      # fails on broken links
```

See [`docs/README.md`](https://github.com/apicrafter/metacrafter/blob/master/docs/README.md).

Larger features should go through OpenSpec (`openspec/AGENTS.md` and
`openspec/ROADMAP.md`).

## Related repos

- [metacrafter-rules](https://github.com/apicrafter/metacrafter-rules) — extended YAML rules
- [metacrafter-registry](https://github.com/apicrafter/metacrafter-registry) — datatype catalog
