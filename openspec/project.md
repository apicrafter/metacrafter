# Metacrafter — Project Context

## Purpose

Metacrafter is a Python CLI and library for labeling table fields and data files. It detects PII, person names, identifiers, dates, and semantic data types using rule-based classification, optional LLM/RAG hybrid modes, and registry-linked metadata.

Part of the **Metacrafter ecosystem** with:
- **metacrafter-rules** — extended YAML rules and Python validators
- **metacrafter-registry** — canonical semantic datatype catalog at registry.apicrafter.io

## Tech Stack

- Python 3.8+
- Typer (CLI), Pydantic (config), PyParsing (rule patterns)
- SQLAlchemy, pymongo, iterabledata (data sources)
- Flask (API server), Rich/tabulate (output)
- Optional: OpenAI/Ollama/LM Studio, ChromaDB (LLM/RAG)

## Project Conventions

### Code Style

- Follow existing patterns in surrounding modules
- Type hints where appropriate
- Minimal scope per change; avoid unrelated refactors

### Architecture Patterns

- `CrafterCmd` orchestrates scan, format, and output (target: split into focused modules)
- `RulesProcessor` loads YAML rules and applies text/ppr/func matchers
- Rule `key` values MUST align with registry `id` values for URL linking

### Testing Strategy

- pytest in `tests/` (~283 tests)
- Mock external services (registry, LLM) in CI
- Integration tests for file formats and API

### Git Workflow

- Feature branches; conventional commit messages
- OpenSpec change proposals required before significant features
- See `openspec/ROADMAP.md` for phased ecosystem plan

## Domain Context

- **Rule types:** `text` (field names), `ppr` (PyParsing values), `func` (Python validators)
- **Contexts:** pii, finance, geo, medical, etc. (from registry categories)
- **Confidence scoring:** rules produce match confidence; hybrid mode merges with LLM

## Important Constraints

- Rule `key` ↔ registry `id` contract is critical for metadata URLs
- PyParsing rules compiled in restricted namespace for security
- LLM mode currently requires OpenAI for embeddings even with local LLM providers

## External Dependencies

- **metacrafter-registry:** https://registry.apicrafter.io
- **metacrafter-rules:** optional rulepath extension pack
- iterabledata, qddate, phonenumbers, validators
