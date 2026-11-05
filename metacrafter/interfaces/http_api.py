"""HTTP API (FastAPI) over the shared scan service.

Routes:

* ``POST /v2/scan``: records -> report version 3
* ``POST /v2/classify``: column samples -> report version 3
* ``POST /api/v1/scan_data``: pre-1.0 request shape (query options, JSON array body)
* ``GET /health``, ``GET /ready``; OpenAPI at ``/openapi.json``

Set ``METACRAFTER_API_TOKEN`` to require ``Authorization: Bearer <token>`` on scan routes.
"""
from __future__ import annotations

import hmac
import json
import logging
import os
from threading import Lock
from typing import Any, Callable, Dict, List, Optional

from metacrafter.exceptions import require_extra

require_extra("fastapi", "server", "The HTTP API")

from fastapi import FastAPI, Request  # noqa: E402
from fastapi.concurrency import run_in_threadpool  # noqa: E402
from fastapi.exceptions import RequestValidationError  # noqa: E402
from fastapi.responses import JSONResponse  # noqa: E402
from pydantic import BaseModel, Field  # noqa: E402

from metacrafter.interfaces.common import columns_to_rows, json_safe  # noqa: E402
from metacrafter.report import redact_samples  # noqa: E402

logger = logging.getLogger(__name__)

DEFAULT_MAX_BODY = 10 * 1024 * 1024
DEFAULT_MAX_RECORDS = 10000
DEFAULT_MAX_DEPTH = 32
MAX_COLUMNS = 500
MAX_COLUMN_VALUES = 10000


class ScanOptions(BaseModel):
    limit: int = Field(default=1000, ge=1, le=DEFAULT_MAX_RECORDS)
    confidence: Optional[float] = Field(default=None, ge=0, le=100)
    contexts: Optional[str] = None
    langs: Optional[str] = None
    fields: Optional[str] = None
    parse_dates: bool = True
    stop_on_match: bool = False
    ignore_imprecise: bool = True
    except_empty: bool = True
    explain: bool = False
    include_samples: bool = False


class ScanRequest(BaseModel):
    records: List[Dict[str, Any]] = Field(max_length=DEFAULT_MAX_RECORDS)
    options: ScanOptions = Field(default_factory=ScanOptions)


class Column(BaseModel):
    name: str = Field(min_length=1)
    values: List[Any] = Field(default_factory=list, max_length=MAX_COLUMN_VALUES)


class ClassifyRequest(BaseModel):
    columns: List[Column] = Field(min_length=1, max_length=MAX_COLUMNS)
    options: ScanOptions = Field(default_factory=ScanOptions)


def _depth(value: Any) -> int:
    if isinstance(value, dict):
        return 1 + max((_depth(v) for v in value.values()), default=0)
    if isinstance(value, list):
        return 1 + max((_depth(v) for v in value), default=0)
    return 0


def _bad_request(message: str, error: str = "Invalid request data") -> JSONResponse:
    return JSONResponse({"error": error, "message": message}, status_code=400)


class MetacrafterAPI:
    """FastAPI application bound to one lazily created scan service."""

    def __init__(
        self,
        service=None,
        auth_token: Optional[str] = None,
        service_factory: Optional[Callable[[], Any]] = None,
        max_body: int = DEFAULT_MAX_BODY,
        max_records: int = DEFAULT_MAX_RECORDS,
        max_depth: int = DEFAULT_MAX_DEPTH,
    ) -> None:
        self.service = service
        self.auth_token = auth_token or os.environ.get("METACRAFTER_API_TOKEN")
        self._factory = service_factory or self._default_factory
        self.max_body = max_body
        self.max_records = max_records
        self.max_depth = max_depth
        self._lock = Lock()
        self.app = self._build()

    @staticmethod
    def _default_factory():
        from metacrafter.core import CrafterCmd

        return CrafterCmd(quiet=True)

    def initialize(self):
        with self._lock:
            if self.service is None:
                self.service = self._factory()
        return self.service

    # --- helpers -----------------------------------------------------------

    def _authorized(self, request: Request) -> bool:
        if not self.auth_token:
            return True
        supplied = request.headers.get("Authorization", "")
        return hmac.compare_digest(supplied, "Bearer " + self.auth_token)

    async def _read_json(self, request: Request) -> Any:
        length = request.headers.get("content-length")
        if length and length.isdigit() and int(length) > self.max_body:
            raise OverflowError
        body = await request.body()
        if len(body) > self.max_body:
            raise OverflowError
        if not body:
            raise ValueError("Request data is empty")
        return json.loads(body)

    def _check_records(self, items: Any) -> None:
        if not isinstance(items, list) or any(not isinstance(item, dict) for item in items):
            raise ValueError("Payload must be an array of objects")
        if len(items) > self.max_records:
            raise ValueError(f"Payload exceeds the record limit ({self.max_records})")
        if _depth(items) > self.max_depth:
            raise ValueError(f"Payload exceeds the nesting limit ({self.max_depth})")

    async def _scan(self, items: List[Dict[str, Any]], options: Dict[str, Any], include_samples: bool):
        service = await run_in_threadpool(self.initialize)
        report = await run_in_threadpool(lambda: service.scan_data(items, **options))
        if not include_samples:
            report = redact_samples(report)
        return json_safe(report)

    # --- application -------------------------------------------------------

    def _build(self) -> FastAPI:
        try:
            from importlib.metadata import version

            api_version = version("metacrafter")
        except Exception:  # pragma: no cover
            api_version = "unknown"
        app = FastAPI(
            title="Metacrafter API",
            version=api_version,
            description="Label fields of tabular data with semantic datatypes from metacrafter-registry.",
        )

        @app.exception_handler(RequestValidationError)
        async def validation_error(request: Request, exc: RequestValidationError):
            return JSONResponse(
                {"error": "Invalid request data", "message": json_safe(exc.errors())},
                status_code=400,
            )

        @app.middleware("http")
        async def body_limit(request: Request, call_next):
            length = request.headers.get("content-length")
            if length and length.isdigit() and int(length) > self.max_body:
                return JSONResponse({"error": "Payload too large"}, status_code=413)
            return await call_next(request)

        @app.get("/health", tags=["service"])
        async def health():
            return {"status": "ok"}

        @app.get("/ready", tags=["service"])
        async def ready():
            try:
                await run_in_threadpool(self.initialize)
            except Exception:  # noqa: BLE001
                logger.exception("Scan service initialization failed")
                return JSONResponse({"status": "unavailable"}, status_code=503)
            return {"status": "ready"}

        @app.post("/v2/scan", tags=["scan"])
        async def scan_v2(body: ScanRequest, request: Request):
            """Classify records (row objects) and return report version 3."""
            if not self._authorized(request):
                return JSONResponse({"error": "Unauthorized"}, status_code=401)
            try:
                self._check_records(body.records)
                options = body.options.model_dump(exclude={"include_samples"})
                options = {k: v for k, v in options.items() if v is not None}
                return await self._scan(body.records, options, body.options.include_samples)
            except (ValueError, TypeError) as error:
                return _bad_request(str(error))

        @app.post("/v2/classify", tags=["scan"])
        async def classify_v2(body: ClassifyRequest, request: Request):
            """Classify columns given as ``{name, values}`` samples; returns report version 3."""
            if not self._authorized(request):
                return JSONResponse({"error": "Unauthorized"}, status_code=401)
            try:
                rows = columns_to_rows(c.model_dump() for c in body.columns)
                self._check_records(rows)
                options = body.options.model_dump(exclude={"include_samples"})
                options = {k: v for k, v in options.items() if v is not None}
                return await self._scan(rows, options, body.options.include_samples)
            except (ValueError, TypeError) as error:
                return _bad_request(str(error))

        @app.post("/api/v1/scan_data", tags=["legacy"])
        async def scan_data_v1(request: Request):
            """Pre-1.0 endpoint: JSON array body, options as query parameters."""
            if not self._authorized(request):
                return JSONResponse({"error": "Unauthorized"}, status_code=401)
            try:
                items = await self._read_json(request)
                self._check_records(items)
                options = _v1_options(request.query_params)
                service = await run_in_threadpool(self.initialize)
                report = await run_in_threadpool(lambda: service.scan_data(items, **options))
                return json_safe(report)
            except OverflowError:
                return JSONResponse({"error": "Payload too large"}, status_code=413)
            except json.JSONDecodeError:
                return JSONResponse({"error": "Invalid JSON"}, status_code=400)
            except (ValueError, TypeError, RecursionError) as error:
                message = "Payload nesting limit exceeded" if isinstance(error, RecursionError) else str(error)
                return _bad_request(message)
            except Exception:  # noqa: BLE001
                logger.exception("Scan request failed")
                return JSONResponse({"error": "Internal server error"}, status_code=500)

        return app


def _v1_options(args) -> Dict[str, Any]:
    options: Dict[str, Any] = {}
    if "limit" in args:
        options["limit"] = int(args["limit"])
    for key in ("confidence", "dict_share", "llm_min_confidence"):
        if key in args:
            options[key] = float(args[key])
    for key in ("contexts", "langs", "fields"):
        if key in args:
            options[key] = args[key]
    for key in ("stop_on_match", "parse_dates", "ignore_imprecise", "except_empty", "stats_only", "explain"):
        if key in args:
            value = args[key].lower()
            if value not in ("true", "false", "1", "0"):
                raise ValueError(key + " must be true or false")
            options[key] = value in ("true", "1")
    if "empty_values" in args:
        options["empty_values"] = json.loads(args["empty_values"])
        if not isinstance(options["empty_values"], list):
            raise ValueError("empty_values must be an array")
    return options


def create_app(**kwargs) -> FastAPI:
    """ASGI application factory (for uvicorn ``--factory``)."""
    return MetacrafterAPI(**kwargs).app


def run_server(host: str = "127.0.0.1", port: int = 10399, debug: bool = False) -> None:
    require_extra("uvicorn", "server", "The HTTP API server")
    import uvicorn

    api = MetacrafterAPI()
    api.initialize()
    uvicorn.run(api.app, host=host, port=port, log_level="debug" if debug else "info")
