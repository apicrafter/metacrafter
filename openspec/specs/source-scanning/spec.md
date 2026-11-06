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

