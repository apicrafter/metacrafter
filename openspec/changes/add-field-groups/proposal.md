# Change: Field groups in scan reports

## Why

Flat tables encode structure in field names (apicrafter/metacrafter#12): `place`, `placeURL`,
`placeLocation`, `placeType` describe one object; `reviewScore`, `avgScore` share a value type;
`title`, `title_en` are language variants. Reports listed fields one by one.

## What Changes

- `metacrafter/report/groups.py`: `field_groups(fields)` splits names on case changes and separators
  and groups siblings (same parent path) by first word (`prefix`, words of 3+ letters, not flag words
  such as `is`/`has`), by last word (`suffix`) and by language suffix (`language`).
- `finalize_report` adds `field_groups` to every single-source report; Markdown output lists them.

## Impact

- Affected specs: `scan-report`
- Affected code: `metacrafter/report/groups.py` (new), `metacrafter/report/build.py`, `metacrafter/report/markdown.py`
- Report: new top-level key `field_groups` (report models allow extra keys).
