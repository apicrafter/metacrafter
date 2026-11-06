## Context

Agents use Metacrafter in two ways:
1. They have column samples already (from their own queries) and need labels.
2. They point Metacrafter at a file or database.

The first case carries no access risk. The second exposes local resources through a
model-driven interface, so it needs explicit scoping.

## Goals / Non-Goals

**Goals:**
- One scan service behind SDK, CLI, HTTP and MCP with identical options and results.
- Safe defaults: no file or database access unless configured; bounded work per call.
- Outputs that agents can chain with the registry MCP server: canonical ids and URLs.

**Non-Goals:**
- Write access to catalogs from MCP. Export stays a CLI/SDK operation.
- Authentication beyond the operational token defined in `refactor-scan-service-and-api`.

## Decisions

### Decision: MCP tools

| Tool | Input | Access | Limits |
|---|---|---|---|
| `classify_columns` | column names with up to 100 values each | none | 200 columns, 100 values per column |
| `scan_file` | path under an allowed root | read file | `limit` ≤ 10000 records |
| `scan_sql` | connection name from config, table list | read-only SELECT | `limit` ≤ 10000 rows per table |
| `explain_match` | field + datatype from the last report, or values | none | — |
| `list_rules` | filters | none | 500 entries per page |

Configuration (`.metacrafter`):

```yaml
mcp:
  file_roots: [/data/shared]
  connections:
    warehouse: postgresql://readonly@db/warehouse   # resolved server-side, never echoed
```

- `scan_sql` accepts only connection names, never raw URLs.
- Credentials never appear in tool output or logs.
- `scan_file` resolves symlinks and rejects paths outside `file_roots`.

### Decision: Results returned to the model

Tools return report v2 with `include_samples: false`. Values the model sent to
`classify_columns` are not echoed back. `explain_match` returns evidence counts and rule
ids, not raw values.

### Decision: FastAPI for HTTP

FastAPI provides OpenAPI and pydantic validation that matches the report models. It runs
on uvicorn (`server` extra). The v1 Flask route is reimplemented as a FastAPI route with
the same request and response shape.

## Risks / Trade-offs

- **Prompt-injected agents could try to read arbitrary files.** Mitigation: allowlists,
  path resolution checks, no default roots.
- **Large scans block an agent loop.** Mitigation: hard limits and a timeout per call;
  oversized requests return an error that states the limit.
- **Two HTTP stacks during transition.** Mitigation: Flask is removed in the same release;
  only the v1 route shape is kept.
