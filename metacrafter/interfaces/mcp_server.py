"""MCP server for AI agents (``metacrafter mcp``).

Requires ``pip install 'metacrafter[mcp]'``. Tools are read-only and bounded:

* ``classify_columns``: label column samples an agent already has (no file access);
* ``scan_file``: scan a file under a configured ``mcp.file_roots`` directory;
* ``scan_sql``: scan tables of a database named in ``mcp.connections``;
* ``explain_match``: show how a datatype is (or is not) detected;
* ``list_rules``: browse loaded rules.

With no ``mcp`` configuration, file and SQL access are refused. Tool results contain
labels, registry links and evidence counts, never the scanned values.
"""
from __future__ import annotations

import concurrent.futures
import io
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

from metacrafter.interfaces.common import columns_to_rows, compact_match, compact_report, json_safe

logger = logging.getLogger(__name__)

INSTRUCTIONS = (
    "Metacrafter labels columns of tabular data with semantic datatypes from the "
    "Metacrafter Registry (registry.apicrafter.io) and flags personal data (PII). "
    "Use classify_columns when you already have sample values; scan_file and scan_sql "
    "read only configured locations. Each label has a canonical registry id and URL you "
    "can cite, a confidence (0-100, heuristic) and evidence. Records with status 'draft' "
    "are not authoritative; say so when you report them. Use explain_match to see why a "
    "datatype was or was not detected."
)

LIST_RULES_PAGE = 500


class AccessError(Exception):
    """Raised when a tool request is outside the configured scope or limits."""


class MetacrafterTools:
    """Tool implementations, independent of the MCP SDK (used by the server and tests)."""

    def __init__(self, service=None, settings=None) -> None:
        from metacrafter.config import ConfigLoader

        self.settings = settings or ConfigLoader.get_mcp_settings()
        self._service = service

    @property
    def service(self):
        if self._service is None:
            from metacrafter.core import CrafterCmd

            self._service = CrafterCmd(quiet=True)
        return self._service

    def _run(self, func, *args, **kwargs):
        """Run ``func`` with the configured timeout."""
        pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        future = pool.submit(func, *args, **kwargs)
        try:
            return future.result(timeout=self.settings.timeout)
        except concurrent.futures.TimeoutError as exc:
            raise AccessError(f"The request exceeded the time limit of {self.settings.timeout:g} seconds") from exc
        finally:
            pool.shutdown(wait=False, cancel_futures=True)

    def _check_limit(self, name: str, value: int, maximum: int) -> None:
        if value > maximum:
            raise AccessError(f"{name} is {value}; the maximum is {maximum}")

    # --- tools -------------------------------------------------------------

    def classify_columns(self, columns: List[Dict[str, Any]], country: Optional[str] = None) -> Dict[str, Any]:
        if not isinstance(columns, list) or not columns:
            raise AccessError("columns must be a non-empty list of {name, values}")
        self._check_limit("Number of columns", len(columns), self.settings.max_columns)
        for column in columns:
            if not isinstance(column, dict) or not column.get("name"):
                raise AccessError("Each column needs a name and a list of values")
            self._check_limit(
                f"Number of values in column '{column['name']}'",
                len(column.get("values") or []),
                self.settings.max_values,
            )
        rows = columns_to_rows(columns)
        report = self._run(self.service.scan_data, rows, parse_dates=True)
        return json_safe(compact_report(report))

    def _resolve_file(self, path: str) -> Path:
        roots = [Path(root).expanduser().resolve() for root in self.settings.file_roots]
        if not roots:
            raise AccessError("File access is disabled: no mcp.file_roots are configured in .metacrafter")
        resolved = Path(path).expanduser().resolve()
        for root in roots:
            if resolved == root or root in resolved.parents:
                if not resolved.is_file():
                    raise AccessError(f"Not a file: {path}")
                return resolved
        raise AccessError(f"Path is outside the allowed roots: {path}")

    def scan_file(self, path: str, limit: int = 1000) -> Dict[str, Any]:
        self._check_limit("limit", int(limit), self.settings.max_records)
        resolved = self._resolve_file(path)
        report = self._run(
            self.service.scan_file,
            str(resolved),
            limit=int(limit),
            output=io.StringIO(),
            output_format="json",
        )
        return json_safe(compact_report(report))

    def scan_sql(
        self,
        connection: str,
        tables: Optional[List[str]] = None,
        limit: int = 1000,
        schema: Optional[str] = None,
    ) -> Dict[str, Any]:
        self._check_limit("limit", int(limit), self.settings.max_records)
        connections = self.settings.connections or {}
        if "://" in str(connection) or connection not in connections:
            names = ", ".join(sorted(connections)) or "none configured"
            raise AccessError(f"Unknown connection '{connection}'. Configured connections: {names}")
        url = connections[connection]
        report = self._run(
            self.service.scan_db,
            url,
            schema=schema,
            limit=int(limit),
            output=io.StringIO(),
            output_format="json",
            tables=tables,
        )
        result = json_safe(compact_report(report))
        for error in result.get("errors", []):
            error.pop("url", None)
        return result

    def explain_match(
        self,
        datatype: str,
        values: Optional[List[Any]] = None,
        field_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        service = self.service
        registry = service.registry
        resolution = registry.resolve(datatype)
        canonical = resolution.canonical_id or datatype
        ref = registry.describe(canonical) if resolution.known else None
        rules = [
            {"ruleid": r["id"], "key": r["key"], "type": r["type"], "match": r["match"],
             "imprecise": r.get("imprecise", False), "priority": r.get("priority")}
            for r in service.processor.field_rules + service.processor.data_rules
            if (registry.resolve(r["key"]).canonical_id or r["key"]) == canonical
        ]
        result: Dict[str, Any] = {
            "datatype": canonical,
            "known_to_registry": resolution.known,
            "registry": ref.asdict() if ref else None,
            "rules": rules,
        }
        if values:
            self._check_limit("Number of values", len(values), self.settings.max_values)
            name = field_name or "value"
            report = self._run(
                service.scan_data,
                [{name: v} for v in values],
                parse_dates=True,
                explain=True,
                confidence=0,
                ignore_imprecise=False,
            )
            field = next((f for f in report["data"] if f["field"] == name), {"matches": [], "rejected": []})
            matched = [compact_match(m) | {"evidence": m.get("evidence")} for m in field.get("matches", [])
                       if (m.get("datatype") or m.get("dataclass")) == canonical]
            rejected = [r for r in field.get("rejected", []) if registry.resolve(r.get("dataclass", "")).canonical_id == canonical
                        or r.get("dataclass") == canonical]
            result["detected"] = bool(matched)
            result["matches"] = matched
            result["rejected"] = rejected
            result["other_matches"] = [compact_match(m) for m in field.get("matches", [])
                                       if (m.get("datatype") or m.get("dataclass")) != canonical]
        return json_safe(result)

    def list_rules(
        self,
        datatype: Optional[str] = None,
        context: Optional[str] = None,
        rule_type: Optional[str] = None,
        page: int = 1,
    ) -> Dict[str, Any]:
        processor = self.service.processor
        registry = self.service.registry
        wanted = (registry.resolve(datatype).canonical_id or datatype) if datatype else None
        rules = []
        for rule in processor.field_rules + processor.data_rules:
            if rule_type and rule["type"] != rule_type:
                continue
            if context and context not in rule.get("context", []):
                continue
            canonical = registry.resolve(rule["key"]).canonical_id
            if wanted and (canonical or rule["key"]) != wanted:
                continue
            rules.append({
                "ruleid": rule["id"],
                "key": rule["key"],
                "datatype": canonical,
                "type": rule["type"],
                "match": rule["match"],
                "context": rule.get("context"),
                "lang": rule.get("lang"),
                "priority": rule.get("priority"),
                "imprecise": rule.get("imprecise", False),
                "source": os.path.basename(str(rule.get("source") or "")),
            })
        page = max(1, int(page))
        start = (page - 1) * LIST_RULES_PAGE
        return {
            "total": len(rules),
            "page": page,
            "page_size": LIST_RULES_PAGE,
            "rules": rules[start:start + LIST_RULES_PAGE],
        }


def create_server(tools: Optional[MetacrafterTools] = None):
    """Build the MCP server (``mcp`` SDK 2.x)."""
    from metacrafter.exceptions import require_extra

    require_extra("mcp", "mcp", "The MCP server")
    from mcp.server.mcpserver import MCPServer
    from mcp.server.mcpserver.exceptions import ToolError
    from mcp.types import ToolAnnotations

    tools = tools or MetacrafterTools()
    read_only = ToolAnnotations(readOnlyHint=True, idempotentHint=True, openWorldHint=False)
    try:
        from importlib.metadata import version

        server_version = version("metacrafter")
    except Exception:  # pragma: no cover
        server_version = ""
    server = MCPServer(
        name="metacrafter",
        title="Metacrafter",
        instructions=INSTRUCTIONS,
        website_url="https://apicrafter.github.io/metacrafter/",
        version=server_version,
    )

    def guarded(func, *args, **kwargs):
        try:
            return func(*args, **kwargs)
        except AccessError as exc:
            raise ToolError(str(exc)) from exc
        except ValueError as exc:
            raise ToolError(str(exc)) from exc

    @server.tool(annotations=read_only)
    def classify_columns(columns: list[dict[str, Any]], country: str | None = None) -> dict[str, Any]:
        """Label columns from sample values. columns is a list of {"name": str, "values": [...]}.

        Returns, per column, the detected datatypes with canonical registry ids, URLs,
        confidence (0-100), PII flags and status. The values are not echoed back.
        """
        return guarded(tools.classify_columns, columns, country)

    @server.tool(annotations=read_only)
    def scan_file(path: str, limit: int = 1000) -> dict[str, Any]:
        """Scan a data file (CSV, JSON Lines, Parquet, ...) under an allowed directory and label its fields."""
        return guarded(tools.scan_file, path, limit)

    @server.tool(annotations=read_only)
    def scan_sql(connection: str, tables: list[str] | None = None, limit: int = 1000, schema: str | None = None) -> dict[str, Any]:
        """Scan tables of a configured database connection (by name, never a URL) and label their fields."""
        return guarded(tools.scan_sql, connection, tables, limit, schema)

    @server.tool(annotations=read_only)
    def explain_match(datatype: str, values: list[str] | None = None, field_name: str | None = None) -> dict[str, Any]:
        """Explain how a datatype is detected: its registry record, the rules for it and, given
        sample values, whether it matches and why not (evidence counts, no values)."""
        return guarded(tools.explain_match, datatype, values, field_name)

    @server.tool(annotations=read_only)
    def list_rules(datatype: str | None = None, context: str | None = None, rule_type: str | None = None, page: int = 1) -> dict[str, Any]:
        """List loaded rules, optionally for one datatype, context (e.g. pii) or type (field, data)."""
        return guarded(tools.list_rules, datatype, context, rule_type, page)

    return server


def serve(transport: str = "stdio", host: str = "127.0.0.1", port: int = 8765) -> None:
    server = create_server()
    if transport == "stdio":
        server.run(transport="stdio")
    else:
        server.run(transport="streamable-http", host=host, port=port)
