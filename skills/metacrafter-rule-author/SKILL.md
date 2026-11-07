---
name: metacrafter-rule-author
description: Write or fix a Metacrafter detection rule (YAML, format version 2) for a Metacrafter Registry datatype and test it against the registry's positive and negative examples. Use when a datatype is not detected, is detected wrongly, or a new identifier needs a rule.
---

# Write a Metacrafter rule

Rules say *how* to find a datatype; the registry says *what* it is. Every rule `key` must
be a registry id or alias.

## Steps

1. **Find the datatype.** Look it up in the registry (MCP `get_datatype`, or
   `https://registry.apicrafter.io/datatype/<id>.md`). Note `regexp`, `checksum`,
   `examples` (`valid: false` ones are negative examples), `status` and `is_pii`.
2. **Check existing coverage.** `metacrafter rules list --output-format json` or the MCP
   tool `list_rules(datatype="<id>")`. By default (`--registry-detectors checksum`) a
   stable record with a checksum is already detected as `registry:<id>`. A stable record
   with only a specific regexp is detected with `--registry-detectors stable`; default
   scans need a rule for it.
3. **Prefer `match: registry`** when the record has a good regexp. The rule adds
   heuristics only:

   ```yaml
   format_version: 2
   name: Payments
   description: Payment identifiers
   context: finances
   lang: common
   rules:
     ibanbyregistry:
       key: iban
       name: IBAN by registry pattern
       type: data
       match: registry
       rule: iban
       fieldrule: iban,account,accountnumber
       fieldrulematch: text
   ```

   Otherwise use `match: regex` (whole-value Python regex), `ppr` (PyParsing),
   `text` (keyword list) or `func` (a Python function). Add
   `validator: checksum:<algorithm>` when the identifier has a check digit.
4. **Field-name rules** (`type: field`, usually `match: text`) recognise column names.
   Use `priority: 1` only for unambiguous names; generic names get `priority: 3`.
5. **Validate**: `metacrafter rules validate ./myrules`.
6. **Test** against registry examples:
   `metacrafter rules test --registry-examples --rulepath ./myrules --rule <ruleid>`.
   Every positive example should match and no negative example may match. Then
   `metacrafter rules check --registry --rulepath ./myrules` to confirm the key resolves.
7. **Scan a sample** with `metacrafter scan file sample.csv --rulepath ./myrules
   --output-format md` and check the confidence and evidence.

## Rules of thumb

- Anchor intent, not format: a 9-digit number is many things. Use `fieldrule` or a
  checksum to make broad patterns precise, or mark the rule `imprecise: true`.
- Set `minlen`/`maxlen`; they also speed up scanning.
- Mark personal data with `is_pii: true` only when the registry agrees or the context
  makes it personal.
- Contribute rules to [metacrafter-rules](https://github.com/apicrafter/metacrafter-rules)
  and missing datatypes to the registry, not to this repository.
