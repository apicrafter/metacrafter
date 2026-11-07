# source-scanning Specification

## Purpose
Reading files and databases: dialect-aware queries, bounded reproducible sampling and honest partial results.
## Requirements
### Requirement: Portable schema-aware queries

SQL scans SHALL construct quoted dialect-aware limited queries, preserve schema-qualified source identity, and close connections on success and failure; MongoDB clients SHALL also close.

#### Scenario: Quoted table in a schema

- **WHEN** a selected schema contains a table named order detail
- **THEN** the correct table is sampled without unsafe identifier interpolation or source collisions

### Requirement: Bounded reproducible sampling

File scans SHALL retain at most the requested positive sample size, support head and seeded reservoir sampling, and record method, seed, retained count and observed count with an explicit statistics scope.

#### Scenario: Large input with head sampling

- **WHEN** a file contains more rows than the requested head sample
- **THEN** the reader stops at the bound and the report identifies statistics as sample statistics

### Requirement: Honest partial results

Multi-source scans SHALL preserve successful reports and source failure diagnostics with completeness status.

#### Scenario: One source fails

- **WHEN** one source fails in an otherwise successful bulk scan
- **THEN** the aggregate is incomplete and identifies the failed source

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

