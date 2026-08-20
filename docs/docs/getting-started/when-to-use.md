---
title: "When to use Metacrafter"
description: "Metacrafter vs Presidio, YData Profiling, DataProfiler, and similar tools"
---
# When to use Metacrafter

Evaluators often ask which tool to reach for. Short answer: **Metacrafter is a
semantic labeling CLI** for structured files and databases. It combines YAML
rules with optional LLM/RAG classification and links matches to a public
datatype [registry](/integrations/registry).

Use another tool when you want its specialized strengths (anonymization, EDA
reports, pipeline expectations). A fuller matrix lives in
[Comparison](/comparison).

| Need | Prefer |
|------|--------|
| Semantic labels beyond PII (UUID, cadastral IDs, stock symbols, dates) | **Metacrafter** |
| PII redaction / anonymization of unstructured text | **Microsoft Presidio** |
| Visual EDA HTML reports from a DataFrame | **YData Profiling** |
| Data quality tests in CI/CD | **Great Expectations** |
| Crawl a warehouse only for PII | **PII Catcher** |
| ML column-type inference on CSV | **Sherlock** |
| Hybrid rules + LLM on files and SQL | **Metacrafter** |

## Metacrafter strengths

- YAML rules for field names and values (text, PyParsing, Python functions)
- Broad file support via iterabledata (CSV, JSONL, Parquet, Excel, Avro, …)
- SQL (SQLAlchemy) and MongoDB scanning from the CLI
- Registry URLs on every match (`https://registry.apicrafter.io/datatype/...`)
- Optional hybrid LLM classification for ambiguous fields
- Catalog export to DataHub, OpenMetadata, and Apache Atlas

## When another tool wins

- **Presidio**: you need anonymizers, not labels, on free-form text
- **YData Profiling**: you want charts and correlation heatmaps
- **Great Expectations**: you already know the contract and want to enforce it
- **Sherlock**: you want a pretrained ML model rather than inspectable rules

## Related docs

- [Quick start](/getting-started/quick-start)
- [Comparison](/comparison)
- [Format support](/formats/)
