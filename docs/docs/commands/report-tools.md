---
title: "report, check, evaluate"
description: "Compare reports, list PII, enforce policies, explain matches and evaluate detection"
---
# Report and quality tools

These commands work on saved reports (`--output-format json` or `yaml`). They accept report
versions 1, 2 and 3 and aggregate reports from SQL, MongoDB and bulk scans.

## `metacrafter report diff OLD NEW`

Compares two reports field by field: added and removed fields, datatype and PII changes, and
statistics changes.

| Option | Meaning |
|---|---|
| `--fail-on-change` | Exit 1 when the reports differ |
| `--fail-on-incompatible` | Exit 1 when the reports were made with different rules or sampling |
| `--output-format json\|text` | Output format (default `json`) |
| `--output FILE` | Write to a file |

## `metacrafter report pii REPORT`

Lists every PII field with its datatype, confidence and source, plus scan completeness.
`--min-confidence` skips weaker matches.

## `metacrafter check REPORT --policy POLICY`

Checks a report against a YAML or JSON policy and exits 1 on violations:

```yaml
prohibit_pii: true               # any PII field is a violation
prohibited_datatypes: [usssn]    # these datatypes must not appear
required_labels:                 # field -> datatypes it must carry
  customer_email: [email]
min_confidence: 50               # ignore weaker matches
require_complete: true           # an incomplete scan is a violation
unknown: allow                   # allow or fail for fields without a label
```

## `metacrafter scan explain REPORT`

Shows, per field, which rules matched and with what evidence. Scan with `--explain` to also
record rejected candidates; `--include-reasons` prints them.

## Rules

| Command | Purpose |
|---|---|
| `metacrafter rules lint PATH [--strict]` | Load rules and report invalid structure, duplicate ids, missing validators and compile errors. `warnings` lists field rules without `priority`: their name-only match scores 36, below the default confidence 50, so they report only with a value match. `--strict` fails on warnings too |
| `metacrafter rules validate PATH` | Check files against the rule format JSON Schema |
| `metacrafter rules test PATH --examples FILE` | Run rules on your own positive and negative examples |
| `metacrafter rules test --registry-examples` | Run rules on registry examples (see [Ecosystem contract](/development/ecosystem)) |
| `metacrafter rules draft DATATYPE --samples FILE --output RULE.yaml` | Draft a rule from sample values; review it before use |
| `metacrafter rules check --registry` | Check rule keys against the registry |

## `metacrafter evaluate FIXTURE`

Runs scans on a labelled fixture (a list of `{items, labels}` cases, labels mapping field
paths to datatype ids) and reports per-datatype precision, recall, abstentions and time.
`--baseline FILE` fails on a drop larger than `--tolerance` (default 0.02);
`--write-baseline FILE` records the current values. Confidence is evidence share, not a
calibrated probability.

## `metacrafter export dbt REPORT --manifest manifest.json`

Prints the column metadata changes a dbt project would need (dry run): datatype and PII
labels per column, keeping existing metadata.

## Sampling

`scan file` reads the first `--limit` records by default (`--sampling head`).
`--sampling reservoir --seed N` draws a reproducible random sample from the whole file.
Reports record the method and counts in `sampling`.
