# Change: Report database primary and foreign keys in SQL scans

## Why

Database tables often have integer keys that the engine generates (`id`, `<table>_id`). They are
local row numbers, not identifiers that link to other datasets, yet value rules label them with
whatever short-number type matches (apicrafter/metacrafter#14). The table schema states which
columns are keys; scans ignored it.

## What Changes

- `scan sql` reflects each table's primary key and foreign keys (SQLAlchemy).
- Key columns get tags `primary_key`, `autoincrement`, `foreign_key` and a `database` object
  (`primary_key`, `autoincrement`, `foreign_key: "<schema>.<table>.<column>"`).
- A single-column integer primary key with autoincrement (serial, identity, SQLite rowid alias) gets
  the match `id` with `ruletype: schema`, confidence 100 and evidence `source: database schema`.
- `scan_data(field_schema=...)` carries these facts; other sources are unchanged.

## Impact

- Affected specs: `source-scanning`
- Affected code: `metacrafter/sources.py`, `metacrafter/scan_service.py`
- Report: new optional field `database` and tags; models allow extra properties.
