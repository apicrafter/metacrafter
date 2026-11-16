---
title: "Basic usage"
description: "Configuration, output formats, and everyday scan options"
---
# Basic usage

## Configuration file

Metacrafter reads a YAML `.metacrafter` file from the current directory or
`~/.metacrafter`. Command-line flags override the file.

```yaml
# Optional: disable auto-loading of metacrafter-rules when installed
# auto_rules: false

rulepath:
  - ./custom_rules

# Registry (optional; default is https://registry.apicrafter.io)
# registry_url: http://localhost:8089

# LLM Configuration (optional)
classification_mode: hybrid  # rules, llm, or hybrid
llm_provider: openai
llm_model: gpt-4o-mini
llm_min_confidence: 50.0
```

The `rulepath` option lists **additional** rule directories beyond the built-in
`rules/` pack and any auto-discovered `metacrafter-rules` content. When
`auto_rules` is true (default), you do not need to list the extended rules path
manually.

## Output formats

`--format` controls how much classification detail is shown (`short` or `full`).
`--output-format` controls the serialization (`table`, `json`, `yaml`, or `csv`).

```bash
metacrafter scan file data.csv --format full --output-format json -o results.json
metacrafter scan file data.csv --format short --output-format csv --stdout
```

Table rendering uses Rich with PII highlighting when writing to a terminal.
Use `--table-format plain` or set `METACRAFTER_PLAIN=1` for CI and pipes. Scan
headers go to stderr so `--output-format json` stays pipe-safe.

## Filtering rules

- `--contexts pii,finance` — only rules tagged with those contexts
- `--langs en,ru` — language-specific rules (language-independent rules always apply)
- `--country-codes us,ca` — ISO country codes (universal rules always apply)
- `--confidence 80` — drop matches below this score (0–100, default 50; `5` also reports
  weak candidates)
- `--fields email,phone` — classify only named columns

## Empty values and dictionaries

```bash
metacrafter scan file data.csv \
  --empty-values "N/A,NA,NULL,empty" \
  --include-empty \
  --format full
```

By default `None`, `NaN`, `-`, `N/A`, `NA`, the empty string and the Spanish markers of a
missing value (`s/d`, `S/D`, `sin dato`, `sin datos`, `Sin datos`, `sin_informacion`,
`sin información`, `No aplica`, `No registra`, `No reporta`, `SIN DATO`, `SIN INFORMACIÓN`), the
`NULL`, `null` and `<Null>` of SQL and ArcGIS exports, the English markers `n/a`, `Not Applicable`, `Not Assessed`, `Not Recorded`
and `Not Stated` (also in sentence case, lower case and capitals) and the Arabic markers `غير مبين`
(not stated), `غير محدد` (not specified), `غير معروف` (unknown) and `غير متوفر` (not available), also
with the feminine ending `ة`, and the Portuguese markers `Não informado`, `Não se aplica`,
`Não disponível` and `Sem informação` (in four cases) and `----------`, the markers of statistical tables
(the colon of Eurostat, `--`, `- - -`, `…`, a lone full stop and `n.a.`) and the Greek `Δ/Ε` (not
applicable) are empty values: they are not
classified, and a field of them only is
tagged `empty`. `--empty-values` replaces the list.

`--dict-share` controls when a field is tagged as a dictionary (low cardinality).

## Registry URLs

Detected datatypes link to the metacrafter-registry. The base URL is resolved
in this order:

1. `METACRAFTER_REGISTRY_URL`
2. `registry_url` in `.metacrafter`
3. built-in default `https://registry.apicrafter.io`

See [Registry](/integrations/registry) for offline / self-hosted setups.

## Related docs

- [Shared CLI options](/commands/shared-options)
- [scan file](/commands/scan-file)
- [Custom rules](/integrations/rules)
