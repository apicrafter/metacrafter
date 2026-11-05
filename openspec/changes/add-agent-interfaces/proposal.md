# Change: Agent interfaces — MCP server, skills and HTTP API v2

## Why

`metacrafter-registry` now serves AI agents directly:
- an MCP server (`identify_values`, `search_datatypes`, `get_datatype`, `crosswalk`);
- skills (`registry-lookup`, `registry-contributor`);
- `llms.txt` and Markdown twins of every page.

Agents can look up a datatype but cannot scan a dataset with Metacrafter. They have to
shell out to the CLI and parse untyped output.

The HTTP API has one Flask endpoint (`/api/v1/scan_data`). It ignores most client options,
has no health endpoint and no OpenAPI description (Codex review Q08).

This change is phase 4a of `devdocs/MODERNIZATION_PLAN.md`. Accepted decision 5: the HTTP
API moves from Flask to FastAPI.

## What Changes

- `metacrafter mcp` command (extra `mcp`) running an MCP server over `stdio` or
  `streamable-http`. Tools:
  - `classify_columns(columns=[{name, values[]}], options)`;
  - `scan_file(path, limit, options)`;
  - `scan_sql(url, tables, limit, options)`;
  - `explain_match(field, datatype)`;
  - `list_rules(filter)`.

  All tools are read-only, bounded and return report v2 objects.
- Path and connection allowlists for MCP file and SQL tools. Nothing is reachable by
  default except explicitly configured roots and connection names.
- Agent skills in `skills/`: `metacrafter-scan` (run and interpret a scan) and
  `metacrafter-rule-author` (write a v2 rule for a registry datatype and test it on
  registry examples).
- **BREAKING (server extra)** HTTP API v2 on FastAPI:
  - `POST /v2/classify`, `POST /v2/scan`, `GET /health`, `GET /ready`;
  - OpenAPI at `/openapi.json`;
  - request size and record limits.

  `POST /api/v1/scan_data` stays as a compatibility route with its current request and
  response shape.
- Documentation site publishes `llms.txt`, `llms-full.txt` and Markdown versions of pages.

## Relationship to other changes

- **Depends on** `refactor-scan-service-and-api` (Codex W05/W10). It provides the shared
  scan service and the SDK/API parity contract. This change re-hosts that service on
  FastAPI and adds MCP on the same service.
- **Depends on** `add-report-v2` (tool outputs) and `add-registry-integration` (canonical
  ids and URLs consistent with the registry MCP server).
- **Supersedes** the server items (section 1) of the July `expand-metacrafter-llm-platform`.
  LLM modes are exposed through the same options as the SDK.

## Impact

- Affected specs: `agent-interfaces`, `http-api`
- Affected code: new `metacrafter/interfaces/mcp/`, `metacrafter/interfaces/http/`;
  `metacrafter/server/` (compatibility); `skills/`; `docs/` (llms.txt plugin or build
  step); `pyproject.toml` extras `server` and `mcp`
- Security: MCP and HTTP tools can read local files and databases. Allowlists, read-only
  SQL and limits are mandatory; see design.md.
