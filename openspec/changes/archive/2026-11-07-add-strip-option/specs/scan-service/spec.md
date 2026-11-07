## ADDED Requirements

### Requirement: Optional Whitespace Stripping

The scan service SHALL accept a `strip` setting (`--strip` on scan commands, `strip` on
`CrafterCmd`, overridable per `scan_data` call), off by default. When it is on, leading and
trailing whitespace SHALL be removed from every string value, including nested values, before
statistics and matching. Field names SHALL NOT change.

#### Scenario: Padded CSV values

- **WHEN** a CSV column contains `  Alice  ` and ` Bob ` and the scan runs with `--strip`
- **THEN** the column statistics report a maximum length of 5

#### Scenario: Default keeps whitespace

- **WHEN** the same file is scanned without `--strip`
- **THEN** values are analysed as they are
