---
title: "Semantic labeling"
description: "Identify emails, identifiers, dates, and other semantic types"
---
# Semantic labeling

Metacrafter is not only a PII scanner. Rules cover identifiers (UUID, email,
phone), dates (via [qddate](https://github.com/ivbeg/qddate)), geographic codes,
organization names, and country-specific types.

## Field vs data rules

- **Field rules** match column names (`email`, `e_mail`, `midname`, …)
- **Data rules** match sampled values (PyParsing patterns or Python functions)

Both can fire on the same column. Confidence is the share of sampled values that
matched.

## Example output

```
key               ftype    tags    matches                                                                datatype_url
----------------  -------  ------  ---------------------------------------------------------------------  ----------------------------------------------------------
Domain            str              fqdn 99.90                                                             https://registry.apicrafter.io/datatype/fqdn
Name              str              name 100.00                                                            https://registry.apicrafter.io/datatype/name
ASN               str              asn 93.77                                                              https://registry.apicrafter.io/datatype/asn
IPs               str              ipv4 96.28                                                             https://registry.apicrafter.io/datatype/ipv4
```

Each `datatype_url` points at the [registry](/integrations/registry) entry for
that type.

## Language and country

```bash
metacrafter scan file data.csv --langs ru --country-codes ru --format full
metacrafter scan file data.csv --langs en --country-codes us,ca --format full
```

Date detection is on by default (312+ patterns). Disable it with `--no-dates`.

## Hybrid classification

When a column has no strong rule match, hybrid mode asks an LLM with registry
context:

```bash
metacrafter scan file data.csv \
  --classification-mode hybrid \
  --llm-provider openai \
  --llm-min-confidence 60.0 \
  --format full
```

See [LLM classification](/integrations/llm).

## Related docs

- [Custom rules](/integrations/rules)
- [rules CLI](/commands/rules)
- [Comparison](/comparison)
