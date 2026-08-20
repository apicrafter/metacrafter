---
title: "Contributing"
description: "Development setup, tests, and documentation"
---
# Contributing

Thank you for contributing to Metacrafter.

## Development setup

Prerequisites: Python 3.8+, Git, pip, Node.js 18+ (for the docs site).

```bash
git clone https://github.com/apicrafter/metacrafter.git
cd metacrafter
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -e '.[dev]'
```

## Tests

```bash
pytest tests/
# or with coverage (CI currently requires 50%):
pytest --cov=metacrafter --cov-report=term-missing
```

## Code style

```bash
black metacrafter tests
flake8 metacrafter
mypy metacrafter --ignore-missing-imports --no-strict-optional
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
