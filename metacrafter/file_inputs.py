"""Look at a data file before iterabledata opens it.

iterabledata chooses the reader and its defaults from the file name. Common open data files
break those defaults (since iterabledata 1.2 the readers handle PC-Axis encodings, WFS GML,
zipped Shapefiles and sheet options themselves, #90):

- compressed CSV in a Windows or East Asian code page is read as UTF-8, and short plain
  Western files are detected as Mac code pages (#81);
- PC-Axis files declare their encoding in ``CODEPAGE`` / ``CHARSET`` and carry their
  metadata keywords in every record (#82);
- JSON files keep their records in a list under a key (OData ``value``, JSON-LD ``@graph``,
  API wrappers) (#83);
- WFS GML wraps features in ``wfs:member`` or a namespaced ``featureMember`` (#84);
- GTFS and other delimited tables are named ``.txt`` (#85);
- CSV files start with an Excel ``sep=;`` line or with title rows above the header (#87).

:func:`prepare_input` reads the start of the file (decompressed) and returns a
:class:`InputPlan`: iterabledata arguments plus the clean-up of the records. It only adds
what the caller did not set, so explicit ``delimiter``, ``encoding`` and ``tagname`` win.
"""
from __future__ import annotations

import bz2
import codecs
import csv
import gzip
import io
import itertools
import json
import logging
import lzma
import math
import os
import re
import zipfile
from collections import Counter
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple

logger = logging.getLogger(__name__)

SAMPLE_BYTES = 1 << 20
JSON_MAX_BYTES = 256 << 20
SAMPLE_RECORDS = 60
MAX_HEADER_OFFSET = 30
MAX_SHEETS = 20
MIN_TABLE_COLUMNS = 3
MIN_TABLE_ROWS = 3
CODEC_EXTENSIONS = {"gz", "bz2", "xz", "zst", "zip", "lz4", "br"}
TABLE_EXTENSIONS = {"csv", "tsv", "psv", "tab", "txt"}
DELIMITERS = (",", ";", "\t", "|")
# Code pages tried for text that is not UTF-8. Mac and DOS code pages are left out on
# purpose: detectors pick them for short Western samples ("Straße" -> "Straşe"). Big5 text is
# tried as cp950, the Windows superset that Taiwanese files are written in: strict big5
# fails on its extensions (0xF9D6-0xF9FE) and the file came out in a Latin code page (#99).
CANDIDATE_ENCODINGS = (
    "cp1252", "cp1250", "cp1251", "cp1253", "cp1254", "cp1255", "cp1256", "cp1257",
    "cp932", "euc_jp", "cp949", "cp950", "gb18030", "koi8_r",
)
_LATIN_NAMES = {"cp1252", "cp1250", "cp1254", "cp1257", "latin_1", "iso8859_1", "iso8859_2", "iso8859_4",
                "iso8859_9", "iso8859_13", "iso8859_15"}
_SEP_HINT = re.compile(r"^sep=(.)\s*$", re.IGNORECASE)
_PX_CODEPAGE = re.compile(rb'CODEPAGE\s*=\s*"([\w\-]+)"', re.IGNORECASE)
_PX_CHARSET = re.compile(rb'CHARSET\s*=\s*"([\w\-]+)"', re.IGNORECASE)
_NUMBER = re.compile(r"^[\s+\-]?[\d\s.,]+%?$")
_YEAR = re.compile(r"^(?:1[89]|20)\d{2}\s*[*¹²³)]*$")
_DATE = re.compile(r"^(?:\d{1,2}[./]\d{1,2}[./]\d{4}|\d{4}-\d{2}-\d{2})$")


def _alphabet(letters: str) -> frozenset:
    return frozenset(letters) | frozenset(letters.upper())


# Non-ASCII letters of languages written with each single-byte Latin code page.
LATIN_ALPHABETS = (
    ("cp1252", [_alphabet(a) for a in (
        "àâæçèéêëîïôœùûüÿ",  # French
        "áéíóúüñ",  # Spanish
        "äöüß",  # German
        "àèéìíîòóùú",  # Italian
        "àáâãçéêíóôõú",  # Portuguese
        "éèëïöüáóú",  # Dutch
        "åäöæøéü",  # Nordic
    )]),
    ("cp1250", [_alphabet(a) for a in (
        "áčďéěíňóřšťúůýž",  # Czech
        "áäčďéíĺľňóôŕšťúýž",  # Slovak
        "ąćęłńóśźż",  # Polish
        "áéíóöőúüű",  # Hungarian
        "ăâîşţ",  # Romanian
        "čćđšž",  # Croatian, Slovene
    )]),
    ("cp1257", [_alphabet(a) for a in ("ąčęėįšųūž", "āčēģīķļņšūž", "äõöüšž")]),
    ("cp1254", [_alphabet("çğıöşüâîûİ")]),
)


@dataclass
class InputPlan:
    """How to open one file and how to clean its records."""

    iterableargs: dict
    skip_records: int = 0
    records: Optional[list] = None
    notes: List[str] = field(default_factory=list)
    # What the file says about its fields, by field name (SPSS labels), for the report.
    field_info: Dict[str, Dict[str, Any]] = field(default_factory=dict)

    def apply(self, records: Iterable[Any]) -> Iterator[Any]:
        """Skip the title and header lines read as records."""
        iterator = iter(records)
        if self.skip_records:
            iterator = itertools.islice(iterator, self.skip_records, None)
        return iterator


class RecordSource:
    """Records read here, with the parts of the iterabledata API that scan_file uses."""

    def __init__(self, records: Iterable[Any], kind: str):
        self._records = records
        self._kind = kind

    def __iter__(self):
        return iter(self._records)

    def id(self) -> str:  # noqa: A003 - mirrors iterabledata
        return self._kind

    def totals(self) -> Optional[int]:
        return len(self._records) if isinstance(self._records, list) else None

    def close(self) -> None:
        close = getattr(self._records, "close", None)
        if callable(close):
            close()


# ---------------------------------------------------------------- file names and bytes
def split_name(filename: str) -> Tuple[str, Optional[str]]:
    """Data extension and compression codec from a file name: ``x.csv.gz`` -> ``("csv", "gz")``."""
    parts = os.path.basename(str(filename)).lower().split(".")
    codec = None
    if len(parts) > 2 and parts[-1] in CODEC_EXTENSIONS:
        codec = parts.pop()
    ext = parts[-1] if len(parts) > 1 else ""
    return ext, codec


def _read_up_to(fobj, limit: int) -> bytes:
    chunks, size = [], 0
    while size < limit:
        chunk = fobj.read(min(1 << 16, limit - size))
        if not chunk:
            break
        chunks.append(chunk)
        size += len(chunk)
    return b"".join(chunks)


def read_bytes(filename: str, codec: Optional[str], limit: int = SAMPLE_BYTES) -> Optional[bytes]:
    """Up to ``limit`` decompressed bytes of the file, or None when they cannot be read."""
    try:
        if codec is None:
            with open(filename, "rb") as fh:
                return _read_up_to(fh, limit)
        if codec == "gz":
            with gzip.open(filename, "rb") as fh:
                return _read_up_to(fh, limit)
        if codec == "bz2":
            with bz2.open(filename, "rb") as fh:
                return _read_up_to(fh, limit)
        if codec == "xz":
            with lzma.open(filename, "rb") as fh:
                return _read_up_to(fh, limit)
        if codec == "zst":
            import zstandard

            with open(filename, "rb") as raw:
                with zstandard.ZstdDecompressor().stream_reader(raw) as fh:
                    return _read_up_to(fh, limit)
        if codec == "zip":
            with zipfile.ZipFile(filename) as zf:
                names = [n for n in zf.namelist() if not n.endswith("/")]
                if not names:
                    return None
                with zf.open(names[0]) as fh:
                    return _read_up_to(fh, limit)
        if codec == "lz4":
            import lz4.frame

            with lz4.frame.open(filename, "rb") as fh:
                return _read_up_to(fh, limit)
        if codec == "br":
            import brotli

            decompressor = brotli.Decompressor()
            out = b""
            with open(filename, "rb") as raw:
                while len(out) < limit:
                    chunk = raw.read(1 << 16)
                    if not chunk:
                        break
                    out += decompressor.process(chunk)
            return out[:limit]
    except Exception as exc:  # noqa: BLE001 - detection is best effort; iterabledata reports real errors
        logger.debug("Could not read a sample of %s: %s", filename, exc)
    return None


# ---------------------------------------------------------------- encodings
def _decodes(sample: bytes, encoding: str) -> bool:
    try:
        sample.decode(encoding)
        return True
    except UnicodeDecodeError:
        return False


def _latin_choice(sample: bytes) -> str:
    """Single-byte Latin code page whose letters best fit one language's alphabet.

    Detectors cannot tell cp1252, cp1250, cp1257 and cp1254 apart on short samples (German
    and French come out as cp1250). Decoded with the right code page, the non-ASCII letters
    of a text belong to one alphabet (French, Polish, ...); decoded with a wrong one they
    mix alphabets (Polish "ś" -> French "œ" next to Spanish "ñ"). Ties go to cp1252.
    """
    best = None
    for encoding, alphabets in LATIN_ALPHABETS:
        try:
            text = sample.decode(encoding)
        except UnicodeDecodeError:
            continue
        letters = Counter(ch for ch in text if ord(ch) > 127 and ch.isalpha())
        total = sum(letters.values())
        if not total:
            return encoding
        cover = max(sum(n for ch, n in letters.items() if ch in alphabet) for alphabet in alphabets) / total
        if best is None or cover > best[0] + 1e-9:
            best = (cover, encoding)
    return best[1] if best else "latin-1"


def detect_encoding(sample: bytes, truncated: bool = True) -> str:
    """Encoding of a byte sample: BOM, then UTF-8, then the most likely common code page."""
    if sample.startswith(codecs.BOM_UTF8):
        return "utf-8-sig"
    if sample.startswith((codecs.BOM_UTF16_LE, codecs.BOM_UTF16_BE)):
        return "utf-16"
    if not re.search(rb"[\x80-\xff]", sample):
        return "utf-8"
    try:
        sample.decode("utf-8")
        return "utf-8"
    except UnicodeDecodeError as exc:
        if truncated and exc.start >= len(sample) - 3:
            return "utf-8"  # the sample ends inside a multi-byte character
    lines = b"\n".join(ln for ln in sample.split(b"\n") if re.search(rb"[\x80-\xff]", ln))[:500000]
    detected = None
    try:
        from charset_normalizer import from_bytes

        best = from_bytes(lines, cp_isolation=list(CANDIDATE_ENCODINGS)).best()
        if best is not None:
            detected = best.encoding
    except ImportError:  # pragma: no cover - charset-normalizer comes with requests
        import chardet

        detected = (chardet.detect(lines) or {}).get("encoding")
    if detected:
        detected = codecs.lookup(detected).name.replace("-", "_")
    if detected is None or detected in _LATIN_NAMES or detected.startswith(("mac", "cp8")):
        return _latin_choice(sample)
    return detected


# ---------------------------------------------------------------- CSV layout
@dataclass
class TableLayout:
    delimiter: Optional[str]
    columns: int
    header_index: int  # non-empty records before the header
    header: List[str]  # column names (upper header rows joined in)
    sep_hint: bool
    consistent: float  # share of sampled records that have ``columns`` fields


def _rows(text: str, delimiter: str, truncated: bool) -> List[List[str]]:
    try:
        rows = list(itertools.islice(csv.reader(io.StringIO(text), delimiter=delimiter), SAMPLE_RECORDS + 1))
    except csv.Error:
        return []
    if truncated and len(rows) > 1:
        rows = rows[:-1]  # the sample may end inside the last record
    return [r for r in rows if r][:SAMPLE_RECORDS]


def _width(row: List[str]) -> int:
    """Number of cells up to the last filled one (sheets pad rows with empty cells)."""
    for index in range(len(row) - 1, -1, -1):
        if str(row[index]).strip():
            return index + 1
    return 0


def _is_label(cell: str) -> bool:
    """A header cell: text, a year or a date (statistics tables use them as column labels)."""
    return bool(cell) and (not _NUMBER.match(cell) or bool(_YEAR.match(cell)) or bool(_DATE.match(cell)))


def _signature(row: List[str], columns: int) -> Tuple[bool, ...]:
    return tuple(_is_label(str(c).strip()) for c in row[:columns])


def _looks_like_header(row: List[str], columns: int, following: Optional[List[str]] = None) -> bool:
    """Wide enough, mostly labels, and not shaped like the record after it (then it is data)."""
    cells = [str(c).strip() for c in row[:columns]]
    filled = [c for c in cells if c]
    if abs(_width(row) - columns) > max(1, columns // 10) or len(filled) < max(2, math.ceil(columns / 2)):
        return False
    if sum(1 for c in filled if _is_label(c)) <= len(filled) / 2:
        return False
    if following is not None and any(not _is_label(c) for c in filled):
        # "1;A;x" above "2;B;y": the same mix of numbers and text in every column is data.
        return _signature(row, columns) != _signature(following, columns)
    return True


def _filled(row: List[str]) -> int:
    return sum(1 for c in row if str(c).strip())


def _is_title_row(row: List[str], columns: int = 0) -> bool:
    """Blank, one filled cell ("Tabelle 1: ..."), a note like "Quelle:;Statistisches Amt",
    or two cells far narrower than the table (a letterhead: address and date)."""
    filled = [str(c).strip() for c in row if str(c).strip()]
    if len(filled) <= 1:
        return True
    return len(filled) == 2 and (filled[0].endswith(":") or (columns >= 8 and _width(row) <= columns // 4))


def _is_band_row(row: List[str]) -> bool:
    """An upper header row: two or more label cells and no numbers (merged group headings)."""
    filled = [str(c).strip() for c in row if str(c).strip()]
    return len(filled) >= 2 and all(_is_label(c) for c in filled) and not _is_title_row(row)


def _combine_header(band: List[List[str]], header: List[str], columns: int) -> List[str]:
    """Column names from upper header rows (merged cells filled to the right) and the header."""
    filled_band = []
    for row in band:
        cells, last = [], ""
        for i in range(columns):
            cell = str(row[i]).strip() if i < len(row) else ""
            last = cell or last
            cells.append(last)
        filled_band.append(cells)
    names = []
    for i in range(columns):
        parts = [cells[i] for cells in filled_band] + [str(header[i]).strip() if i < len(header) else ""]
        words: List[str] = []
        for part in parts:
            if part and part not in words:
                words.append(part)
        names.append(" ".join(words))
    return names


def header_row(rows: List[List[str]]) -> Tuple[int, int, Optional[List[str]]]:
    """(header row index, body column count, combined names or None) of the first table rows.

    The header is the first row with the body's column count (the most common width), if
    every row above it is a title or note row, or an upper header row (merged group
    headings) directly above it; then the names join the upper rows and the header row.
    Otherwise the header is the first row.
    """
    widths = Counter(w for w in (_width(r) for r in rows) if w > 1)
    if not widths:
        return 0, 0, None
    columns = max(widths.items(), key=lambda kv: (kv[1], kv[0]))[0]
    for index, row in enumerate(rows[: MAX_HEADER_OFFSET + 1]):
        following = next((r for r in rows[index + 1:] if _width(r) > 1), None)
        if not _looks_like_header(row, columns, following):
            continue
        top = index
        while top > 0 and _is_band_row(rows[top - 1]):
            top -= 1
        # Header rows below it (GENESIS: "Stichtag" / "31.12.2025" / "Insgesamt"): label
        # rows with fewer filled cells than the first data row continue the header.
        while index + 1 < len(rows) and _is_band_row(rows[index + 1]):
            data = next((r for r in rows[index + 2:] if _width(r) > 1 and not _is_band_row(r)), None)
            if data is None or _filled(rows[index + 1]) >= _filled(data):
                break
            index += 1
        if all(_is_title_row(r, columns) for r in rows[:top]):
            if top < index:
                return index, columns, _combine_header(rows[top:index], rows[index], columns)
            return index, columns, None
        break
    return 0, columns, None


def table_layout(text: str, delimiter: Optional[str] = None, truncated: bool = True) -> Optional[TableLayout]:
    """Delimiter, header position and header of delimited text (first records only)."""
    first, _, rest = text.partition("\n")
    sep_hint = False
    hint = _SEP_HINT.match(first.strip("\ufeff\r"))
    if hint:
        sep_hint = True
        delimiter = delimiter or hint.group(1)
        text = rest
    candidates = [delimiter] if delimiter else list(DELIMITERS)
    best = None
    for delim in candidates:
        rows = _rows(text, delim, truncated)
        widths = Counter(len(r) for r in rows if len(r) > 1)
        if not widths:
            continue
        columns, freq = max(widths.items(), key=lambda kv: (kv[1], kv[0]))
        if best is None or (freq, columns) > (best[1], best[0]):
            best = (columns, freq, delim, rows)
    if best is None:
        return None
    columns, freq, delim, rows = best
    header_index, _, names = header_row(rows)
    header = names or (rows[header_index] if rows else [])
    return TableLayout(delim, columns, header_index, header, sep_hint, freq / max(len(rows), 1))


def _unique(names: List[str]) -> List[str]:
    seen: Counter = Counter()
    out = []
    for name in names:
        name = name.strip()
        seen[name] += 1
        out.append(name if seen[name] == 1 else f"{name}_{seen[name]}")
    return out


# ---------------------------------------------------------------- JSON
def _records_path(obj: Any, depth: int = 0, path: Tuple[str, ...] = ()) -> Optional[Tuple[int, Tuple[str, ...]]]:
    """(length, key path) of the longest list of objects in nested dicts (3 levels)."""
    if not isinstance(obj, dict) or depth > 2:
        return None
    best = None
    for key, value in obj.items():
        candidate = None
        if isinstance(value, list) and value:
            head = value[:50]
            if sum(isinstance(item, dict) for item in head) >= 0.8 * len(head):
                candidate = (len(value), path + (str(key),))
        elif isinstance(value, dict):
            candidate = _records_path(value, depth + 1, path + (str(key),))
        if candidate and (best is None or candidate[0] > best[0]):
            best = candidate
    return best


def _split_table(obj: dict) -> Optional[Tuple[list, str]]:
    """Records of a column-split table: a list of column names next to rows as lists.

    Shapes: ``{"columns": [...], "data": [[...], ...]}`` (pandas ``split``, many APIs) and
    ``{"indices": [...], "data": {"id": [...], ...}}`` (Umweltbundesamt).
    """
    names = [(k, v) for k, v in obj.items()
             if isinstance(v, list) and len(v) >= 2 and all(isinstance(x, str) for x in v)
             and len(set(v)) == len(v)]
    for row_key, rows in obj.items():
        if isinstance(rows, dict):
            rows = list(rows.values())
        if not isinstance(rows, list) or not rows or not all(isinstance(r, list) for r in rows[:50]):
            continue
        for name_key, columns in names:
            if name_key == row_key:
                continue
            head = rows[:50]
            if sum(1 for r in head if len(r) == len(columns)) >= 0.8 * len(head):
                records = [dict(zip(columns, r)) for r in rows if isinstance(r, list)]
                return records, f"rows under {row_key} with the column names under {name_key}"
    return None


def json_records(data: bytes) -> Optional[Tuple[list, str]]:
    """Records of a JSON document whose root is an object, and where they were found."""
    encoding = detect_encoding(data, truncated=False)
    try:
        obj = json.loads(data.decode(encoding))
    except (ValueError, UnicodeDecodeError) as exc:
        logger.debug("JSON sample could not be parsed: %s", exc)
        return None
    if not isinstance(obj, dict):
        return None
    found = _records_path(obj)
    if found is None:
        split = _split_table(obj)
        if split is not None:
            return split
        return [obj], "the root object as one record"
    node: Any = obj
    for key in found[1]:
        node = node[key]
    return [item for item in node if isinstance(item, dict)], "the list under " + ".".join(found[1])


# ---------------------------------------------------------------- the plan
def prepare_input(filename: str, iterableargs: dict) -> InputPlan:
    """Arguments and record clean-up for opening ``filename`` with iterabledata."""
    args = dict(iterableargs)
    plan = InputPlan(iterableargs=args)
    if args.get("format"):
        return plan
    ext, codec = split_name(filename)
    if "compression" in args:  # forced by the caller
        codec = args["compression"] if args["compression"] in CODEC_EXTENSIONS else None
    if ext in TABLE_EXTENSIONS:
        _plan_table(filename, ext, codec, plan)
    elif ext == "px":
        # iterabledata reads CODEPAGE / CHARSET; its metadata copies are not table fields.
        args.setdefault("metadata", False)
    elif ext == "json" and not args.get("tagname"):
        _plan_json(filename, codec, plan)
    elif ext in ("xlsx", "xls") and codec is None and "start_line" not in args and "keys" not in args:
        _plan_sheet(filename, ext, plan)
    for note in plan.notes:
        logger.info("%s: %s", filename, note)
    return plan


def _plan_table(filename: str, ext: str, codec: Optional[str], plan: InputPlan) -> None:
    args = plan.iterableargs
    sample = read_bytes(filename, codec)
    if not sample:
        return
    truncated = len(sample) >= SAMPLE_BYTES
    if not args.get("encoding"):
        args["encoding"] = detect_encoding(sample, truncated)
        if args["encoding"] not in ("utf-8", "utf-8-sig"):
            plan.notes.append(f"encoding {args['encoding']}")
    try:
        text = sample.decode(args["encoding"], errors="replace")
    except LookupError:
        return
    given = args.get("delimiter") or ("\t" if ext in ("tsv", "tab") else "|" if ext == "psv" else None)
    layout = table_layout(text, given, truncated)
    if ext == "txt":
        if layout is None or layout.consistent < 0.8 or layout.columns < 2:
            return  # plain text: iterabledata reads it line by line
        args["format"] = "csv"
        args["delimiter"] = layout.delimiter
        plan.notes.append(f"read as a table delimited by {layout.delimiter!r}")
    if layout is None:
        return
    if layout.sep_hint or layout.header_index:
        args["delimiter"] = layout.delimiter
        args["keys"] = _unique(layout.header)
        plan.skip_records = layout.header_index + 1 + (1 if layout.sep_hint else 0)
        skipped = plan.skip_records - 1
        plan.notes.append(f"header on record {skipped + 1}; skipped {skipped} line(s) before it")


def sheet_rows(filename: str, ext: str, page: int = 0, limit: int = SAMPLE_RECORDS) -> Optional[List[List[str]]]:
    """First rows of a spreadsheet sheet as strings ("" for empty cells)."""
    try:
        if ext == "xlsx":
            import openpyxl

            workbook = openpyxl.load_workbook(filename, read_only=True, data_only=True)
            try:
                sheet = workbook[workbook.sheetnames[page]]
                rows = sheet.iter_rows(max_row=limit, values_only=True)
                return [["" if v is None else str(v) for v in row] for row in rows]
            finally:
                workbook.close()
        import xlrd

        book = xlrd.open_workbook(filename, on_demand=True)
        try:
            sheet = book.sheet_by_index(page)
            return [[str(v) for v in sheet.row_values(i)] for i in range(min(limit, sheet.nrows))]
        finally:
            book.release_resources()
    except Exception as exc:  # noqa: BLE001 - best effort; iterabledata reports real errors
        logger.debug("Could not read the first rows of %s: %s", filename, exc)
        return None


def sheet_names(filename: str, ext: str) -> List[str]:
    """Sheet names of a workbook, [] when they cannot be read."""
    try:
        if ext == "xlsx":
            import openpyxl

            workbook = openpyxl.load_workbook(filename, read_only=True)
            try:
                return list(workbook.sheetnames)
            finally:
                workbook.close()
        import xlrd

        book = xlrd.open_workbook(filename, on_demand=True)
        try:
            return list(book.sheet_names())
        finally:
            book.release_resources()
    except Exception as exc:  # noqa: BLE001 - best effort
        logger.debug("Could not list the sheets of %s: %s", filename, exc)
        return []


def _is_table(rows: List[List[str]], index: int, columns: int) -> bool:
    """At least three columns and three filled rows below the header."""
    body = [r for r in rows[index + 1:] if _width(r) > 1]
    return columns >= MIN_TABLE_COLUMNS and len(body) >= MIN_TABLE_ROWS


def _plan_sheet(filename: str, ext: str, plan: InputPlan) -> None:
    given = plan.iterableargs.get("page")
    if given is not None:
        if not str(given).isdigit():
            return  # a sheet name: leave it to iterabledata
        candidates = [int(given)]
    else:
        # Notes, a title page or contents often come first (INSEE, Destatis): scan the
        # first sheet that holds a table (#89).
        candidates = list(range(max(1, min(len(sheet_names(filename, ext)), MAX_SHEETS))))
    page, rows, index, names = candidates[0], None, 0, None
    for candidate in candidates:
        found = sheet_rows(filename, ext, candidate)
        if not found:
            continue
        found_index, columns, found_names = header_row(found)
        if rows is None:
            page, rows, index, names = candidate, found, found_index, found_names
        if given is not None or _is_table(found, found_index, columns):
            page, rows, index, names = candidate, found, found_index, found_names
            break
    if rows is None or (not index and page == 0):
        return
    if page != 0:
        titles = sheet_names(filename, ext)
        name = titles[page] if page < len(titles) else str(page)
        plan.notes.append(f"scanned sheet {page + 1} ({name}), the first one that holds a table")
    # The readers skip start_line rows; with joined names they get them as keys and start at
    # the first data row, else they take the next non-blank row as the header.
    plan.iterableargs["page"] = page
    if names:
        plan.iterableargs["keys"] = _unique(names)
        plan.iterableargs["start_line"] = index + 1
    elif index:
        plan.iterableargs["start_line"] = index
    if index:
        plan.notes.append(f"header on sheet row {index + 1}; skipped {index} row(s) above it")


def _plan_json(filename: str, codec: Optional[str], plan: InputPlan) -> None:
    head = read_bytes(filename, codec, 4096)
    if not head or not head.lstrip(codecs.BOM_UTF8 + b" \t\r\n").startswith(b"{"):
        return
    data = read_bytes(filename, codec, JSON_MAX_BYTES + 1)
    if data is None or len(data) > JSON_MAX_BYTES:
        return
    found = json_records(data)
    if found is None:
        return
    plan.records, where = found
    plan.notes.append(f"records from {where}")
