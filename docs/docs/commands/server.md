---
title: "server"
description: "Run the Metacrafter HTTP classification API"
---
# server

```bash
metacrafter server run --host 127.0.0.1 --port 10399
```

Starts a Flask API that exposes `POST /api/v1/scan_data`. See
[API server](/integrations/api) for the request body and response shape.

## Options

| Flag | Meaning |
|------|---------|
| `--host` | Bind address (default `127.0.0.1`) |
| `--port` | Port (default `10399`) |
| `--debug` | Verbose server logging |

Optional: `METACRAFTER_SECRET_KEY` for the Flask secret.

## Remote CLI scans

Point any scan command at the running server:

```bash
metacrafter scan file somefile.csv \
  --format full \
  --remote http://127.0.0.1:10399 \
  --output-format json \
  --stdout
```
