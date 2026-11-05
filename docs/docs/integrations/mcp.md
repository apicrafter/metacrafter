---
title: "MCP server and agent skills"
description: "Use Metacrafter from AI agents through MCP and skills"
---
# MCP server and agent skills

```bash
pip install 'metacrafter[mcp]'
metacrafter mcp                                   # stdio
metacrafter mcp --transport streamable-http --port 8765
```

Example client configuration (stdio):

```json
{
  "mcpServers": {
    "metacrafter": {"command": "metacrafter", "args": ["mcp"]}
  }
}
```

Use it together with the [registry MCP server](https://registry.apicrafter.io/agents)
(`metacrafter-registry mcp`): both use the same canonical datatype ids and URLs.

## Tools

All tools are read-only and run locally.

| Tool | Input | Access |
|---|---|---|
| `classify_columns` | `columns: [{name, values}]` | none: works on samples the agent already has |
| `scan_file` | `path`, `limit` | files under `mcp.file_roots` only |
| `scan_sql` | `connection` (a name), `tables`, `limit`, `schema` | databases in `mcp.connections` only |
| `explain_match` | `datatype`, optional `values`, `field_name` | none |
| `list_rules` | `datatype`, `context`, `rule_type`, `page` | none |

Results list, per field, the canonical datatype id, registry URL, name, confidence,
PII flag, `pii_source` and registry status. They never contain the scanned values.
`explain_match` returns evidence counts and the reasons a rule was rejected.

## Access scope and limits

With no configuration, `scan_file` and `scan_sql` refuse every request. Allow access in
`.metacrafter`:

```yaml
mcp:
  file_roots: [/data/shared]
  connections:
    warehouse: postgresql://readonly@db/warehouse
  max_columns: 200
  max_values: 100
  max_records: 10000
  timeout: 60
```

- Paths are resolved (including symlinks) and must stay inside a root.
- `scan_sql` accepts only connection names; URLs are rejected, and connection URLs never
  appear in results.
- Use a read-only database account for configured connections.
- Requests above a limit fail with a message that states the limit.

## Skills

The repository ships two skills in `skills/`:

- `metacrafter-scan`: scan data, read confidence, evidence and draft status, report PII.
- `metacrafter-rule-author`: write a format-2 rule for a registry datatype and test it
  on registry examples.

## Documentation for agents

The documentation site publishes [`llms.txt`](pathname:///llms.txt), `llms-full.txt` and a
Markdown version of every page (`<page>.md`).
