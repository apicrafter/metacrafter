---
title: "API server"
description: "HTTP API v2 (FastAPI, OpenAPI) and the pre-1.0 scan_data endpoint"
---
# API server

```bash
pip install 'metacrafter[server]'
metacrafter server run --host 127.0.0.1 --port 10399
```

The server is a FastAPI application (`metacrafter.interfaces.http_api`). It loads the
rules and registry data once and serves every request from the same scan service as the
CLI and the Python API. The OpenAPI description is at `/openapi.json` and interactive docs
at `/docs`.

Set `METACRAFTER_API_TOKEN` to require `Authorization: Bearer <token>` on scan routes.

## Routes

| Route | Purpose |
|---|---|
| `POST /v2/scan` | Classify records; returns [report version 3](/integrations/report) |
| `POST /v2/classify` | Classify columns given as samples; returns report version 3 |
| `POST /api/v1/scan_data` | Pre-1.0 endpoint (JSON array body, options as query parameters) |
| `GET /health` | Liveness |
| `GET /ready` | 200 when rules and registry data are loaded, otherwise 503 |

### `POST /v2/scan`

```json
{
  "records": [{"name": "John", "email": "john@example.com"}],
  "options": {"limit": 1000, "confidence": 20, "contexts": "pii", "parse_dates": true}
}
```

### `POST /v2/classify`

```json
{
  "columns": [
    {"name": "contact_email", "values": ["a@example.com", "b@example.org"]},
    {"name": "inn", "values": ["7707083893"]}
  ]
}
```

Options for both routes: `limit`, `confidence`, `contexts`, `langs`, `fields`,
`parse_dates`, `stop_on_match`, `ignore_imprecise`, `except_empty`, `explain`,
`include_samples` (default `false`: statistics in the response omit scanned values).

### `POST /api/v1/scan_data`

Body: a JSON array of objects. Query parameters: `limit`, `confidence`, `dict_share`,
`contexts`, `langs`, `fields`, `stop_on_match`, `parse_dates`, `ignore_imprecise`,
`except_empty`, `stats_only`, `explain`, `empty_values` (JSON array).

## Limits

Requests above 10 MB return 413. More than 10,000 records, nesting deeper than 32 levels
or a malformed body return 400 with a message that names the problem.

## Deployment

The ASGI factory is `metacrafter.interfaces.http_api:create_app`:

```bash
uvicorn --factory metacrafter.interfaces.http_api:create_app --host 0.0.0.0 --port 10399
```

`metacrafter.server.api.MetacrafterApp` remains as an alias of the FastAPI application
class for code written against the Flask server.
