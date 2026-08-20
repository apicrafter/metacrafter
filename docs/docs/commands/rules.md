---
title: "rules"
description: "List loaded rules and show aggregate statistics"
---
# rules

Inspect the YAML rules Metacrafter would apply for the current `rulepath`,
country filters, and auto-discovered extras.

## rules list

```bash
metacrafter rules list
metacrafter rules list --output-format json -o rules.json
metacrafter rules list --country-codes us,ca --output-format csv -o us_ca_rules.csv
metacrafter rules list --rulepath ./custom_rules --output-format yaml
```

Columns include rule id, name, type (`field` or `data`), match method
(`text`, `ppr`, `func`), language, country codes, contexts, PII flag, priority,
and length constraints.

## rules stats

```bash
metacrafter rules stats
metacrafter rules stats --rulepath ./custom_rules
metacrafter rules stats --country-codes ru,de
```

Shows counts of field rules, data rules, rules by context / language / country,
and date/time patterns.

## Options

| Flag | Meaning |
|------|---------|
| `--rulepath` | Extra YAML directories |
| `--country-codes` | Restrict the loaded set |
| `--output-format` | `table`, `json`, `yaml`, or `csv` (`list` only) |
| `--output`, `-o` | Output file (`list` only) |
| `--table-format` | Table renderer (`list` only) |

See [Custom rules](/integrations/rules) to add your own.
