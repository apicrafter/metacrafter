## ADDED Requirements

### Requirement: MCP Server

Metacrafter SHALL provide `metacrafter mcp`, an MCP server over `stdio` and
`streamable-http`. It SHALL expose these tools: `classify_columns`, `scan_file`,
`scan_sql`, `explain_match` and `list_rules`. Tool results SHALL be report v2 objects or
subsets of them. They SHALL use canonical registry ids and registry URLs.

#### Scenario: Classify columns from an agent

- **WHEN** an agent calls `classify_columns` with a column `contact_email` and five email values
- **THEN** the result labels the column with datatype `email`, a confidence and the
  registry URL
- **AND** the result does not contain the submitted values

### Requirement: Scoped Resource Access for Agent Tools

`scan_file` SHALL read only paths inside configured `mcp.file_roots`, after resolving
symlinks. `scan_sql` SHALL accept only connection names configured in `mcp.connections`.
It SHALL run read-only queries and SHALL never return or log connection credentials. With
no configuration, both tools SHALL refuse every request.

#### Scenario: Path outside the allowed roots

- **WHEN** an agent calls `scan_file` with `/etc/passwd`
- **AND** `/etc` is not inside `mcp.file_roots`
- **THEN** the tool returns an access error without reading the file

#### Scenario: Raw connection URL rejected

- **WHEN** an agent calls `scan_sql` with a connection URL instead of a configured name
- **THEN** the tool returns an error listing the configured connection names

### Requirement: Bounded Agent Tool Work

Each MCP tool SHALL enforce a maximum number of columns, values, records and a timeout per
call. A request above a limit SHALL return an error that states the limit.

#### Scenario: Too many records requested

- **WHEN** an agent calls `scan_file` with `limit` above the configured maximum
- **THEN** the tool returns an error stating the maximum
- **AND** no file is read

### Requirement: Agent Skills

The repository SHALL ship agent skills `metacrafter-scan` and `metacrafter-rule-author` in
`skills/`. Each skill SHALL contain a `SKILL.md` with its purpose, steps and how to
interpret confidence, evidence and draft status.

#### Scenario: Rule authoring workflow

- **WHEN** an agent follows `metacrafter-rule-author` for registry datatype `iban`
- **THEN** the skill instructs it to write a format-v2 rule
- **AND** to run `metacrafter rules test --registry-examples` on that rule
