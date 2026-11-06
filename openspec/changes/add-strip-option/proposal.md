# Change: Strip whitespace from values before analysis

## Why

Exported CSV files often pad values with spaces to fit fixed-width columns (apicrafter/metacrafter#13).
Padding changes lengths, character classes and pattern matches, so `  alice@example.com  ` is not
an email and `" 42 "` is not a number.

## What Changes

- `--strip` on `scan file`, `scan sql`, `scan mongodb` and `scan bulk`; `CrafterCmd(strip=True)`;
  `scan_data(strip=...)` overrides the `CrafterCmd` setting for one call.
- When enabled, leading and trailing whitespace is removed from every string value, nested values
  included, before statistics and matching. Keys are not changed. Off by default.

## Impact

- Affected specs: `scan-service`
- Affected code: `metacrafter/scan_service.py`, `metacrafter/classify/utils.py`, `metacrafter/cli/commands.py`
