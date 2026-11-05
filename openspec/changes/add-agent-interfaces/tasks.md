## 1. MCP server

- [ ] 1.1 Add extra `mcp` and command `metacrafter mcp [--transport stdio|streamable-http]`
- [ ] 1.2 Implement `classify_columns`, `scan_file`, `scan_sql`, `explain_match`, `list_rules` on the shared scan service
- [ ] 1.3 Implement `mcp.file_roots` and named `mcp.connections` with path and credential safety
- [ ] 1.4 Enforce per-tool limits and timeouts; return limit errors with the limit value
- [ ] 1.5 Tests: path traversal and symlink escape rejected; raw SQL URL rejected; credentials absent from output and logs

## 2. Skills

- [ ] 2.1 Write `skills/metacrafter-scan/SKILL.md` (when to scan, how to read confidence, evidence and draft status)
- [ ] 2.2 Write `skills/metacrafter-rule-author/SKILL.md` (v2 rule for a registry id, test on registry examples)
- [ ] 2.3 Cross-link with the registry `registry-lookup` skill

## 3. HTTP API v2

- [ ] 3.1 Replace Flask with FastAPI and uvicorn under the `server` extra
- [ ] 3.2 Implement `POST /v2/classify`, `POST /v2/scan`, `GET /health`, `GET /ready`
- [ ] 3.3 Reimplement `POST /api/v1/scan_data` with its current shape
- [ ] 3.4 Enforce body size and record limits (413 and 400 responses)
- [ ] 3.5 Tests: SDK, HTTP and MCP return identical reports for the same input and options

## 4. Documentation

- [ ] 4.1 Generate `llms.txt`, `llms-full.txt` and Markdown page twins in the docs build
- [ ] 4.2 Document MCP setup for common agent clients and the security configuration
