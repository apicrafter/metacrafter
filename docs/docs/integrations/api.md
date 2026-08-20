---
title: "API server"
description: "HTTP classification API and remote CLI scans"
---
# API server

```bash
metacrafter server run --host 127.0.0.1 --port 10399
```

The server is a Flask app (`metacrafter/server/api.py`) with lazy initialization
of the rules processor and date parser.

## Endpoint

**POST `/api/v1/scan_data`**

Body: JSON array of objects.

Query parameters:

- `format` — `short` or `full`
- `langs` — comma-separated language filters
- `contexts` — comma-separated context filters
- `limit` — maximum records per field (default 1000)

Request:

```json
[
  {"email": "user@example.com", "name": "John Doe"},
  {"email": "admin@example.com", "name": "Jane Smith"}
]
```

Response includes a compact `results` table and a detailed `data` array with
matches, stats, and registry URLs.

## Remote CLI usage

```bash
metacrafter scan file somefile.csv \
  --format full \
  --remote http://127.0.0.1:10399 \
  --timeout 60 \
  --retries 2 \
  --output-format json \
  --stdout
```

The CLI serializes records, POSTs them to the server, and formats the response
locally. Use this to share one rule set across many scanners.

## Configuration

- `METACRAFTER_SECRET_KEY` — optional Flask secret
- `--debug` on `server run` enables verbose logging

CLI flags: [server](/commands/server).
