# -*- coding: utf8 -*-
"""Console output rendering helpers for the Metacrafter CLI.

This module centralises how scan results, statistics and rule listings are
rendered to the terminal. When ``rich`` is available and the target stream is
an interactive terminal, output is rendered with colour, confidence bars and
PII highlighting. Otherwise it gracefully falls back to plain ``tabulate``
tables so that piping output to files, ``grep`` or other tools keeps working
exactly as before.
"""
import os
import sys
from typing import Any, Dict, List, Optional, Sequence

from tabulate import tabulate

try:  # pragma: no cover - rich is an optional (transitive) dependency
    from rich.box import ROUNDED, SIMPLE_HEAVY
    from rich.console import Console
    from rich.panel import Panel
    from rich.table import Table
    from rich.text import Text

    _RICH_AVAILABLE = True
except ImportError:  # pragma: no cover
    _RICH_AVAILABLE = False


# Table format sentinels that mean "render with rich when possible".
AUTO_TABLE_FORMAT = "auto"
RICH_TABLE_FORMAT = "rich"
DEFAULT_TABLE_FORMAT = AUTO_TABLE_FORMAT


def rich_available() -> bool:
    """Return True if the rich library could be imported."""
    return _RICH_AVAILABLE


def _stream_is_tty(stream: Any) -> bool:
    try:
        return bool(stream.isatty())
    except Exception:
        return False


def should_use_rich(table_format: Optional[str], stream: Any = None) -> bool:
    """Decide whether rich rendering should be used.

    Rich is used when:
      * the library is importable,
      * rich output is not force-disabled via ``METACRAFTER_PLAIN``,
      * the table format is ``rich`` (always), or
      * the table format is ``auto`` and the target stream is a terminal.

    ``NO_COLOR`` is intentionally *not* checked here: rich's own ``Console``
    already honours it by dropping ANSI colour while keeping the table layout.
    Set ``METACRAFTER_PLAIN`` (or pass an explicit tabulate format such as
    ``plain``/``simple``) to fully opt out of rich rendering.
    """
    if not _RICH_AVAILABLE:
        return False
    if os.environ.get("METACRAFTER_PLAIN"):
        return False
    fmt = (table_format or AUTO_TABLE_FORMAT).lower()
    stream = stream or sys.stdout
    if fmt == RICH_TABLE_FORMAT:
        return True
    if fmt == AUTO_TABLE_FORMAT:
        return _stream_is_tty(stream)
    return False


def tabulate_format(table_format: Optional[str]) -> str:
    """Map the configured table format onto a concrete tabulate format."""
    fmt = (table_format or AUTO_TABLE_FORMAT).lower()
    if fmt in (AUTO_TABLE_FORMAT, RICH_TABLE_FORMAT):
        return "simple"
    return table_format


def get_console(stream: Any = None) -> "Console":
    """Create a rich Console bound to the given stream (defaults to stdout).

    The console width is derived from the terminal size (honouring the
    ``COLUMNS`` environment variable) with a generous fallback so that tables
    stay readable even when stdout is not a directly-detectable terminal.
    """
    import shutil

    size = shutil.get_terminal_size(fallback=(120, 25))
    return Console(
        file=stream or sys.stdout,
        highlight=False,
        soft_wrap=False,
        width=max(size.columns, 100),
        height=max(size.lines, 25),
    )


# ---------------------------------------------------------------------------
# Helpers shared by rich renderers
# ---------------------------------------------------------------------------

def _confidence_style(pct: float) -> str:
    if pct >= 90:
        return "green"
    if pct >= 50:
        return "yellow"
    return "red"


def _confidence_text(pct: float) -> "Text":
    """Render a compact coloured confidence bar, e.g. ``██████████ 100%``."""
    pct = max(0.0, min(100.0, float(pct)))
    filled = int(round(pct / 10.0))
    filled = max(0, min(10, filled))
    bar = "█" * filled + "░" * (10 - filled)
    return Text(f"{bar} {pct:5.1f}%", style=_confidence_style(pct))


def _dedupe_matches(matches: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Collapse duplicate dataclasses, keeping the highest confidence each.

    Returns a list of dicts (dataclass, confidence, format, is_pii, classurl)
    sorted by confidence descending.
    """
    grouped: Dict[str, Dict[str, Any]] = {}
    for match in matches:
        dataclass = match.get("dataclass")
        if not dataclass:
            continue
        confidence = float(match.get("confidence", 0.0) or 0.0)
        existing = grouped.get(dataclass)
        if existing is None or confidence > existing["confidence"]:
            grouped[dataclass] = {
                "dataclass": dataclass,
                "confidence": confidence,
                "format": match.get("format"),
                "is_pii": bool(match.get("is_pii")),
                "classurl": match.get("classurl") or "",
            }
        elif match.get("is_pii"):
            existing["is_pii"] = True
    return sorted(grouped.values(), key=lambda m: m["confidence"], reverse=True)


def _linked(name: str, url: str, style: str = "") -> "Text":
    text = Text(name, style=style)
    if url:
        text.stylize(f"link {url}")
    return text


# ---------------------------------------------------------------------------
# Scan results
# ---------------------------------------------------------------------------

def _filter_records(records: List[Dict[str, Any]], dformat: str) -> List[Dict[str, Any]]:
    if dformat == "short":
        return [r for r in records if r.get("matches")]
    return records


def render_scan_results_rich(
    console: "Console",
    records: List[Dict[str, Any]],
    dformat: str,
    title: Optional[str] = None,
) -> None:
    """Render structured scan results using rich."""
    visible = _filter_records(records, dformat)
    if not visible:
        console.print("No classified fields.", style="yellow")
        return

    table = Table(
        box=ROUNDED,
        header_style="bold cyan",
        title=title,
        title_style="bold",
        expand=False,
        pad_edge=False,
    )
    table.add_column("Field", style="bold", no_wrap=True)
    table.add_column("Storage", style="cyan", no_wrap=True)
    table.add_column("Detected type", no_wrap=True)
    table.add_column("Confidence", no_wrap=True)
    table.add_column("Alt. types", no_wrap=True, overflow="ellipsis", max_width=26)
    table.add_column("Tags", no_wrap=True, overflow="ellipsis", max_width=20)

    pii_count = 0
    for record in records:
        record["_matches_deduped"] = _dedupe_matches(record.get("matches") or [])

    for record in visible:
        field = record.get("field", "")
        ftype = record.get("ftype", "")
        tags = record.get("tags") or []
        deduped = record.get("_matches_deduped") or []
        field_is_pii = any(m["is_pii"] for m in deduped)
        if field_is_pii:
            pii_count += 1

        field_text = Text(str(field), style="bold")
        if field_is_pii:
            field_text.append("  PII", style="bold white on red")

        if deduped:
            top = deduped[0]
            name = top["dataclass"]
            if top.get("format"):
                name = f"{name} ({top['format']})"
            detected = _linked(
                name,
                top["classurl"],
                style="bold red" if top["is_pii"] else "bold green",
            )
            confidence = _confidence_text(top["confidence"])
            others = [m["dataclass"] for m in deduped[1:]]
        else:
            detected = Text("—")
            confidence = Text("")
            others = []

        if others:
            shown = ", ".join(others[:2])
            if len(others) > 2:
                shown += f" (+{len(others) - 2})"
        else:
            shown = ""

        table.add_row(
            field_text,
            str(ftype),
            detected,
            confidence,
            shown,
            ", ".join(str(t) for t in tags),
        )

    console.print(table)

    total = len(records)
    classified = len([r for r in records if r.get("matches")])
    summary = Text()
    summary.append("Fields: ", style="cyan")
    summary.append(f"{total}", style="bold")
    summary.append("    Classified: ", style="cyan")
    summary.append(f"{classified}", style="bold green" if classified else "bold")
    summary.append("    PII fields: ", style="cyan")
    summary.append(
        f"{pii_count}", style="bold red" if pii_count else "bold green"
    )
    console.print(summary)


def render_scan_results_plain(
    rows: List[Sequence[Any]],
    headers: Sequence[str],
    table_format: str,
) -> str:
    """Render scan result rows with tabulate (plain fallback)."""
    return tabulate(rows, headers=headers, tablefmt=tabulate_format(table_format))


# ---------------------------------------------------------------------------
# Statistics
# ---------------------------------------------------------------------------

_BOOL_COLUMNS = {
    "is_dictkey",
    "is_uniq",
    "has_digit",
    "has_alphas",
    "has_special",
    "has_any_digit",
    "has_any_alphas",
    "has_any_special",
}


def _format_bool_cell(value: Any) -> "Text":
    truthy = value in (True, "True", "true", 1, "1")
    if truthy:
        return Text("✓", style="green")
    return Text("·", style="yellow")


def render_stats_rich(
    console: "Console",
    stats_table: List[Sequence[Any]],
    headers: Sequence[str],
    title: Optional[str] = None,
) -> None:
    if not stats_table:
        console.print("No statistics available.", style="yellow")
        return
    table = Table(
        box=SIMPLE_HEAVY,
        header_style="bold cyan",
        title=title,
        title_style="bold",
    )
    for idx, header in enumerate(headers):
        justify = "left" if idx == 0 else "right"
        style = "bold" if idx == 0 else None
        table.add_column(str(header), justify=justify, style=style, no_wrap=True)
    for row in stats_table:
        cells: List[Any] = []
        for idx, value in enumerate(row):
            header = headers[idx] if idx < len(headers) else ""
            if header in _BOOL_COLUMNS:
                cells.append(_format_bool_cell(value))
            elif isinstance(value, (list, tuple)):
                cells.append(", ".join(str(v) for v in value))
            else:
                cells.append("" if value is None else str(value))
        table.add_row(*cells)
    console.print(table)


# ---------------------------------------------------------------------------
# Rules listing
# ---------------------------------------------------------------------------

def render_rules_rich(
    console: "Console",
    rules: List[Dict[str, Any]],
    title: Optional[str] = None,
) -> None:
    table = Table(
        box=ROUNDED,
        header_style="bold cyan",
        title=title,
        title_style="bold",
    )
    table.add_column("ID", style="bold", no_wrap=True)
    table.add_column("Name")
    table.add_column("Type", style="cyan", no_wrap=True)
    table.add_column("Match", style="cyan", no_wrap=True)
    table.add_column("Lang", no_wrap=True)
    table.add_column("Country", no_wrap=True)
    table.add_column("Contexts")
    for rule in rules:
        contexts = rule.get("contexts") or []
        contexts_text = Text(" | ".join(sorted(contexts)))
        if "pii" in contexts:
            contexts_text.stylize("red")
        table.add_row(
            str(rule.get("id", "")),
            str(rule.get("name", "")),
            str(rule.get("type", "")),
            str(rule.get("match", "")),
            str(rule.get("lang_display", rule.get("lang", ""))),
            str(rule.get("country_display", "")),
            contexts_text,
        )
    console.print(table)
    footer = Text()
    footer.append("Total rules: ", style="cyan")
    footer.append(str(len(rules)), style="bold")
    console.print(footer)
