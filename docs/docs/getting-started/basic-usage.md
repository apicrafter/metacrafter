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
- `--langs en,ru` — language-specific rules
- `--country-codes us,ca` — ISO country codes
- `--confidence 20.0` — drop matches below this score (0–100)
- `--fields email,phone` — classify only named columns

## Empty values and dictionaries

```bash
metacrafter scan file data.csv \
  --empty-values "N/A,NA,NULL,empty" \
  --include-empty \
  --format full
```

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
