## ADDED Requirements

### Requirement: Database Keys in SQL Scans

SQL scans SHALL read each table's primary key and foreign keys from the database schema. Each
primary-key column SHALL get the tag `primary_key` and `database.primary_key: true`; each
foreign-key column SHALL get the tag `foreign_key` and `database.foreign_key` naming the
referenced column. A single-column integer primary key that the database generates SHALL also get
the tag `autoincrement`, `database.autoincrement: true` and a match for datatype `id` with
`ruletype: schema` and confidence 100.

#### Scenario: Autoincrement key

- **WHEN** a table has `id INTEGER PRIMARY KEY AUTOINCREMENT`
- **THEN** the field `id` reports the tags `primary_key` and `autoincrement`
- **AND** a match `id` with `ruletype: schema` and confidence 100

#### Scenario: Natural key

- **WHEN** a table has `code TEXT PRIMARY KEY`
- **THEN** the field `code` reports the tag `primary_key` and no schema match

#### Scenario: Foreign key

- **WHEN** a column references `parent(id)`
- **THEN** its `database.foreign_key` names the referenced column
