---
title: "Custom rules"
description: "YAML rules, match engines, and validator plugins"
---
# Custom rules

Metacrafter loads rules from every directory in the effective `rulepath`:

1. Built-in `rules/` shipped with the package (when `auto_rules` is true)
2. Extended [metacrafter-rules](https://github.com/apicrafter/metacrafter-rules) pack if `metacrafter[rules]` is installed
3. Extra `rulepath` entries and `--rulepath` flags

Duplicate `key`s are skipped with a warning. Set `auto_rules: false` for a
custom-only set.

## Match engines

| `match` | Behavior |
|---------|----------|
| `text` | Exact match against comma-separated tokens |
| `ppr` | PyParsing expression in a restricted namespace |
| `func` | Importable Python function `(value) -> bool` |

Rules apply to **fields** (column names) or **data** (values).

### Function

```yaml
runpabyfunc:
  key: runpa
  name: Russian legal act / law
  maxlen: 500
  minlen: 3
  priority: 1
  match: func
  type: data
  rule: metacrafter.rules.ru.gov.is_ru_law
```

Any importable path works (`mypackage.validators.is_widget_id`) as long as the
module is on `PYTHONPATH`.

### Exact text

```yaml
midname:
  key: person_midname
  name: Person midname by known field name
  rule: midname,secondname,middlename,mid_name,middle_name
  type: field
  match: text
```

### PyParsing

```yaml
rukadastr:
  key: rukadastr
  name: Russian land territory cadastral identifier
  rule: Word(nums, min=1, max=2) + Literal(':').suppress() + Word(nums, min=1, max=2) + Literal(':').suppress() + Word(nums, min=6, max=7) + Literal(':').suppress() + Word(nums, min=1, max=6)
  maxlen: 20
  minlen: 12
  priority: 1
  match: ppr
  type: data
```

`ppr` rules run with PyParsing primitives only (`Word`, `Literal`, `nums`,
`alphas`, …). `import` / `eval` / `exec` are not available.

## Typical metadata

`key`, `name`, `type`, `match`, `rule`, `priority`, `minlen` / `maxlen`,
`contexts`, `langs`, `country`, `is_pii`, optional `validator`.

Align `key` with a [registry](/integrations/registry) datatype id.

## Inspect

```bash
metacrafter rules list --rulepath ./custom_rules
```

See [rules](/commands/rules) and [Architecture](/development/architecture).
