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
- CSV files start with an Excel ``sep=;`` line or with title rows above the header (#87);
- SPSS, Stata and SAS files need pandas in iterabledata's readers; they are read here with
  pyreadstat's dict output, and their variable and value labels go to the report (#97, #110);
- KML keeps attributes in ``SchemaData/SimpleData`` (GDAL, ogr2ogr) or in the HTML table of
  the description (ArcGIS), which iterabledata's reader drops (#100);
- XML tables list the column names in a header element and each record as a flat list of
  identically named elements (``<header><column>Name</column>`` ... ``<body><column>``) (#101);
- dBase files name their code page in a ``.cpg`` file or a language driver id, or in neither
  (cp1251 or cp866 text), and iterabledata reads them as UTF-8 (#110);
- GeoPackage files need fiona in iterabledata's reader; they are read here with sqlite3. In
  a GeoPackage or another SQLite database the first table by name is a metadata table
  (``gpkg_contents``, ``geometry_columns``) rather than the data (#110);
- iterabledata's iCalendar reader turns the ``BEGIN`` / ``END`` lines of every event into
  fields; events are read here with icalendar (#110).

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
import sqlite3
import struct
import zipfile
from collections import Counter
from dataclasses import dataclass, field
from html.parser import HTMLParser
from typing import Any, Callable, Dict, Iterable, Iterator, List, Optional, Tuple

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
# A unit note next to a table title: "単位：人", "(Unit: persons)", "Einheit: 1000".
_UNIT_NOTE = re.compile(r"^[\s(（]*(?:単位|单位|unit|units|einheit|unité|unidad|unidade|unità|единица|одиниця)\s*[:：]",
                        re.IGNORECASE)
# A line that announces the header on the next line: the field list of exports of Mexico's
# Plataforma Nacional de Transparencia (SIPOT) follows a "Tabla Campos" line, after the format
# id, title, type code and field id rows (#98).
HEADER_MARKERS = {"tabla campos"}


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
    kind: str = "json"  # reader id reported for ``records`` read here
    transform: Optional[Callable[[Any], Any]] = None  # applied to every record

    def apply(self, records: Iterable[Any]) -> Iterator[Any]:
        """Skip the title and header lines read as records; reshape the records."""
        iterator = iter(records)
        if self.skip_records:
            iterator = itertools.islice(iterator, self.skip_records, None)
        if self.transform is not None:
            iterator = map(self.transform, iterator)
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
    if detected in _KOREAN_NAMES and not _HANGUL.search(lines.decode(detected, errors="ignore")):
        return _non_latin_choice(lines) or detected
    return detected


# Korean code pages, and the Hangul that every Korean text has. A few bytes of another script
# decode as Hanja in EUC-KR: the Hebrew header "מספר מקלט" in cp1255 came out as "前初 展麟",
# because detectors guess on so few bytes (#105).
_KOREAN_NAMES = {"euc_kr", "cp949", "johab"}
_HANGUL = re.compile("[ᄀ-ᇿ㄰-㆏가-힣]")
# Hebrew final letters (kaf, mem, nun, pe, tsadi) end a word; their other forms never do.
_HEBREW_FINAL = "ךםןףץ"
_HEBREW_MEDIAL = "כמנפצ"
# Single-byte code pages of non-Latin scripts and the Unicode range of their letters.
_NON_LATIN_PAGES = (("cp1255", "א", "ת"), ("cp1256", "ؠ", "ۿ"),
                    ("cp1253", "Ά", "ώ"), ("cp1251", "Ѐ", "ӿ"))
_GREEK_VOWELS = set("αεηιουωάέήίόύώϊϋΑΕΗΙΟΥΩΆΈΉΊΌΎΏ")


def _hebrew_words(words: List[str]) -> bool:
    """Words that keep the Hebrew rule of final letters: a final form at the end only."""
    return all(not any(ch in _HEBREW_FINAL for ch in w[:-1]) and w[-1] not in _HEBREW_MEDIAL for w in words)


def _non_latin_choice(sample: bytes) -> Optional[str]:
    """Single-byte code page whose decoding of ``sample`` is the letters of one script.

    Hebrew, Arabic, Greek and Cyrillic are tried in that order. Each non-ASCII character must
    be a letter of the code page's script, Hebrew words must keep the rule of final letters
    (Cyrillic text in cp1251 breaks it: н, к, у and п are final forms in cp1255, о and р
    medial ones), and Greek words need a vowel. None fits: None.
    """
    for encoding, first, last in _NON_LATIN_PAGES:
        try:
            text = sample.decode(encoding)
        except UnicodeDecodeError:
            continue
        words = re.findall(r"[^\x00-\x7f]+", text)
        letters = "".join(words)
        if not letters or not all(first <= ch <= last and ch.isalpha() for ch in letters):
            continue
        if encoding == "cp1255" and not _hebrew_words(words):
            continue
        if encoding == "cp1253" and not all(_GREEK_VOWELS & set(w) for w in words if len(w) > 1):
            continue
        return encoding
    return None


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
    a title with its unit ("(2) 年齢別人口;単位：人"), or two cells far narrower than the
    table (a letterhead: address and date)."""
    filled = [str(c).strip() for c in row if str(c).strip()]
    if len(filled) <= 1:
        return True
    if any(_UNIT_NOTE.match(c) for c in filled) and (len(filled) == 2 or len(filled) <= columns // 4):
        return True  # titles with their unit, repeated over the blocks of a wide sheet
    return len(filled) == 2 and (filled[0].endswith((":", "：")) or (columns >= 8 and _width(row) <= columns // 4))


def _is_band_row(row: List[str], columns: int = 0) -> bool:
    """An upper header row: two or more label cells and no numbers (merged group headings)."""
    filled = [str(c).strip() for c in row if str(c).strip()]
    return len(filled) >= 2 and all(_is_label(c) for c in filled) and not _is_title_row(row, columns)


def _text_columns(body: List[List[str]], columns: int) -> set:
    """Columns whose filled body cells are mostly text (the row labels of a statistics table)."""
    out = set()
    for i in range(columns):
        cells = [str(r[i]).strip() for r in body if i < len(r) and str(r[i]).strip()]
        if cells and sum(1 for c in cells if not _NUMBER.match(c)) > len(cells) / 2:
            out.add(i)
    return out


def _is_section_row(row: List[str], text_columns: set) -> bool:
    """A heading over the row labels only ("Batken oblast" above "Items;1990;1991"): a section
    or table title, not a group heading, which spans value columns."""
    cells = [i for i, c in enumerate(row) if str(c).strip()]
    return bool(cells) and bool(text_columns) and all(i in text_columns for i in cells)


def _marker_header(rows: List[List[str]]) -> Optional[int]:
    """Index of the row after a header marker line (SIPOT exports: "Tabla Campos")."""
    for index, row in enumerate(rows[:MAX_HEADER_OFFSET]):
        filled = [str(c).strip() for c in row if str(c).strip()]
        if len(filled) == 1 and filled[0].casefold() in HEADER_MARKERS and index + 1 < len(rows):
            return index + 1
    return None


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
    every row above it is a title or note row, a section heading over the row label columns
    ("Batken oblast"), or an upper header row (merged group headings) directly above it;
    then the names join the upper rows and the header row. A header marker line ("Tabla
    Campos") puts the header on the next row. Otherwise the header is the first row.
    """
    widths = Counter(w for w in (_width(r) for r in rows) if w > 1)
    if not widths:
        return 0, 0, None
    columns = max(widths.items(), key=lambda kv: (kv[1], kv[0]))[0]
    marked = _marker_header(rows)
    if marked is not None:
        return marked, columns, None
    for index, row in enumerate(rows[: MAX_HEADER_OFFSET + 1]):
        following = next((r for r in rows[index + 1:] if _width(r) > 1), None)
        if not _looks_like_header(row, columns, following):
            continue
        # Headings over the row label columns only are section titles (#98).
        text_columns = _text_columns([r for r in rows[index + 1:] if _width(r) > 1], columns)
        top = index
        while top > 0 and _is_band_row(rows[top - 1], columns) and not _is_section_row(rows[top - 1], text_columns):
            top -= 1
        # Header rows below it (GENESIS: "Stichtag" / "31.12.2025" / "Insgesamt"): label
        # rows with fewer filled cells than the first data row continue the header.
        while index + 1 < len(rows) and _is_band_row(rows[index + 1]):
            data = next((r for r in rows[index + 2:] if _width(r) > 1 and not _is_band_row(r)), None)
            if data is None or _filled(rows[index + 1]) >= _filled(data):
                break
            index += 1
        if all(_is_title_row(r, columns) or _is_section_row(r, text_columns) for r in rows[:top]):
            if top < index:
                return index, columns, _combine_header(rows[top:index], rows[index], columns)
            return index, columns, None
        break
    wide = _wide_header(rows, columns)
    if wide is not None:
        return wide[0], wide[1], None
    return 0, columns, None


def _wide_header(rows: List[List[str]], columns: int) -> Optional[Tuple[int, int]]:
    """(index, width) of a header below title rows that is wider than most records.

    The records of a sheet may leave their last cells empty: in the KSH gazetteer the 13
    columns of minority self-governments are blank in most rows, so the most common width is
    12 of 26 and the header row does not have it (#105). The header is then the first row
    after the title rows, if it is at least as wide as every row below it and looks like a
    header for its own width. A first row that is no title is the header anyway.
    """
    for index, row in enumerate(rows[: MAX_HEADER_OFFSET + 1]):
        if _is_title_row(row, columns):
            continue
        width = _width(row)
        below = [r for r in rows[index + 1:] if _width(r) > 1]
        if index and width > columns and below and width >= max(_width(r) for r in below):
            if _looks_like_header(row, width, below[0]):
                return index, width
        return None
    return None


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
    elif ext in STATS_READERS and codec is None:
        _plan_stats(filename, ext, plan)
    elif ext == "dbf" and codec is None:
        _plan_dbf(filename, plan)
    elif ext in SQLITE_EXTENSIONS and codec is None:
        _plan_sqlite(filename, ext, plan)
    elif ext == "ics" and codec is None:
        _plan_ical(filename, plan)
    elif ext in ("kml", "kmz") and codec is None:
        _plan_kml(filename, ext, plan)
    elif ext == "xml" and args.get("tagname"):
        _plan_xml(filename, codec, plan)
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
            return [[_xls_cell(v) for v in sheet.row_values(i)] for i in range(min(limit, sheet.nrows))]
        finally:
            book.release_resources()
    except Exception as exc:  # noqa: BLE001 - best effort; iterabledata reports real errors
        logger.debug("Could not read the first rows of %s: %s", filename, exc)
        return None


def _xls_cell(value: Any) -> str:
    """Cell text as the sheet shows it: xlrd gives every number as a float (1990.0)."""
    if isinstance(value, float) and value.is_integer() and abs(value) < 1e15:
        return str(int(value))
    return str(value)


def sheet_names(filename: str, ext: str) -> List[str]:
    """Sheet names of a workbook, [] when they cannot be read."""
    try:
        if ext == "ods":
            from odf.opendocument import load
            from odf.table import Table

            tables = load(filename).spreadsheet.getElementsByType(Table)
            return [t.getAttribute("name") or f"Sheet{i}" for i, t in enumerate(tables)]
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


def _is_table(rows: List[List[str]], index: int, columns: int, min_columns: int = MIN_TABLE_COLUMNS) -> bool:
    """At least three columns and three filled rows below the header."""
    body = [r for r in rows[index + 1:] if _width(r) > 1]
    return columns >= min_columns and len(body) >= MIN_TABLE_ROWS


SHEET_EXTENSIONS = ("xlsx", "xls", "ods")


def select_sheets(filename: str, sheet: Any) -> Optional[List[Tuple[int, str]]]:
    """(0-based page, sheet name) of the workbook sheets that ``--sheet`` names (#102).

    ``sheet`` is a sheet name, a sheet number counted from 1, or ``all``: every sheet of an
    XLSX or XLS workbook that holds a table (a header and three rows below it; two columns
    are enough here), else the first sheet; every sheet of an ODS workbook. ``all`` on a file
    that is not a workbook gives None (the file is one table); a name or number there, or one
    that the workbook lacks, raises ``ValueError``.
    """
    ext, codec = split_name(filename)
    value = str(sheet).strip()
    if ext not in SHEET_EXTENSIONS or codec is not None:
        if value.lower() == "all":
            return None
        raise ValueError(f"--sheet applies to XLSX, XLS and ODS workbooks, not {os.path.basename(filename)}")
    names = sheet_names(filename, ext)
    if value.lower() == "all":
        if ext == "ods":
            return list(enumerate(names)) or [(0, "1")]
        found = []
        for page, name in enumerate(names):
            rows = sheet_rows(filename, ext, page)
            if not rows:
                continue
            index, columns, _ = header_row(rows)
            if _is_table(rows, index, columns, min_columns=2):
                found.append((page, name))
        return found or [(0, names[0] if names else "1")]
    if value in names:
        return [(names.index(value), value)]
    if value.isdigit() and 1 <= int(value) <= len(names):
        return [(int(value) - 1, names[int(value) - 1])]
    listed = ", ".join(names) or "none could be read"
    raise ValueError(f"No sheet {value!r} in {os.path.basename(filename)}; sheets: {listed}")


def _plan_sheet(filename: str, ext: str, plan: InputPlan) -> None:
    given = plan.iterableargs.get("page")
    if given is not None:
        if not str(given).isdigit():
            names = sheet_names(filename, ext)
            if given not in names:
                return  # leave the error to iterabledata
            given = names.index(given)
        candidates = [int(given)]
    else:
        # Notes, a title page or contents often come first (INSEE, Destatis): scan the
        # first sheet that holds a table (#89). --sheet all scans every one (#102).
        count = len(sheet_names(filename, ext))
        if count > 1:
            plan.notes.append(f"the workbook has {count} sheets; --sheet all scans every sheet that holds a table")
        candidates = list(range(max(1, min(count, MAX_SHEETS))))
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
    elif index and ext == "xls":
        # The XLS reader names columns after str(cell): years in a header become "1990.0".
        plan.iterableargs["keys"] = list(rows[index])
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


# ---------------------------------------------------------------- SPSS, Stata and SAS
STATS_CHUNK_ROWS = 1000
#: pyreadstat's reader of each statistical file format. iterabledata's SPSS, Stata and SAS
#: readers ask pyreadstat for pandas data frames; its dict output needs neither pandas nor polars.
STATS_READERS = {
    "sav": "read_sav",
    "zsav": "read_sav",
    "dta": "read_dta",
    "sas7bdat": "read_sas7bdat",
    "xpt": "read_xport",
}


def _label_key(value: Any) -> str:
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value)


def _stats_value(value: Any) -> Any:
    return None if isinstance(value, float) and math.isnan(value) else value


def _stats_rows(data: dict) -> Iterator[dict]:
    columns = list(data)
    for i in range(len(data[columns[0]]) if columns else 0):
        yield {name: _stats_value(data[name][i]) for name in columns}


def stats_records(filename: str, reader: str = "read_sav", first: Optional[dict] = None,
                  chunk: Optional[int] = None, **options: Any) -> Iterator[dict]:
    """Records of an SPSS, Stata or SAS file, read in chunks of rows by ``pyreadstat.<reader>``.

    pyreadstat's dict output (pyreadstat >= 1.3.1) needs neither pandas nor polars;
    system-missing values become None. ``first`` is the first chunk, already read.
    ``options`` go to the reader (``encoding``).
    """
    import pyreadstat

    read = getattr(pyreadstat, reader)
    chunk = chunk or STATS_CHUNK_ROWS
    offset = 0
    data = first
    while True:
        if data is None:
            data, _ = read(filename, row_offset=offset, row_limit=chunk, output_format="dict", **options)
        rows = len(next(iter(data.values()), []))
        yield from _stats_rows(data)
        if rows < chunk:
            return
        offset += rows
        data = None


def spss_records(filename: str, first: Optional[dict] = None, chunk: Optional[int] = None) -> Iterator[dict]:
    """Records of an SPSS ``.sav`` / ``.zsav`` file (see :func:`stats_records`)."""
    return stats_records(filename, "read_sav", first, chunk)


def _plan_stats(filename: str, ext: str, plan: InputPlan) -> None:
    try:
        import pyreadstat
    except ImportError:
        return  # iterabledata names the missing package
    reader = STATS_READERS[ext]
    options = {"encoding": plan.iterableargs["encoding"]} if plan.iterableargs.get("encoding") else {}
    try:
        first, meta = getattr(pyreadstat, reader)(filename, row_limit=STATS_CHUNK_ROWS, output_format="dict",
                                                  **options)
    except TypeError:
        return  # pyreadstat < 1.3.1 has no dict output: iterabledata's reader needs pandas
    except Exception as exc:  # noqa: BLE001 - iterabledata reports the error
        logger.debug("pyreadstat could not read %s: %s", filename, exc)
        return
    plan.records = stats_records(filename, reader, first, **options)
    plan.kind = "sav" if ext in ("sav", "zsav") else ext
    labels = getattr(meta, "column_names_to_labels", None) or {}
    value_labels = getattr(meta, "variable_value_labels", None) or {}
    for name in getattr(meta, "column_names", None) or list(first):
        info: Dict[str, Any] = {}
        if labels.get(name):
            info["label"] = labels[name]
        if value_labels.get(name):
            info["value_labels"] = {_label_key(k): v for k, v in value_labels[name].items()}
        if info:
            plan.field_info[name] = info
    plan.notes.append(f"read with pyreadstat ({getattr(meta, 'number_columns', len(first))} variables)")


# ---------------------------------------------------------------- dBase
def cpg_encoding(text: str) -> Optional[str]:
    """Python codec of the code page a ``.cpg`` file names: ``UTF-8``, ``1251``, ``ANSI 1251``,
    ``88591``; None when it names none."""
    text = text.strip()
    if not text:
        return None
    candidates = [text, text.split()[-1]]
    last = candidates[-1]
    if last.isdigit():
        if last == "65001":
            candidates.append("utf-8")
        elif last.startswith("8859") and len(last) > 4:
            candidates.append("iso8859_" + last[4:])
        else:
            candidates.append("cp" + last)
    for name in candidates:
        try:
            return codecs.lookup(name).name
        except LookupError:
            continue
    return None


def dbf_encoding(filename: str) -> Optional[str]:
    """Encoding of the text of a dBase file: the code page of its ``.cpg`` file (Shapefile
    convention), else of its language driver id, else detected from the records.

    A file without a language driver id (byte 29 is 0) is read as ASCII by dbfread and as
    UTF-8 by iterabledata, but such files are often in a Windows or DOS code page. None when
    the text is ASCII or the file cannot be read.
    """
    stem = os.path.splitext(filename)[0]
    for suffix in (".cpg", ".CPG"):
        if os.path.exists(stem + suffix):
            try:
                with open(stem + suffix, encoding="ascii", errors="replace") as fh:
                    found = cpg_encoding(fh.read(100))
            except OSError:
                found = None
            if found:
                return found
    try:
        with open(filename, "rb") as fh:
            header = fh.read(32)
            if len(header) < 32:
                return None
            if header[29]:
                try:
                    from dbfread.codepages import guess_encoding

                    return guess_encoding(header[29])
                except (ImportError, LookupError):
                    return None
            fh.seek(int.from_bytes(header[8:10], "little"))
            sample = fh.read(SAMPLE_BYTES)
    except OSError:
        return None
    if not re.search(rb"[\x80-\xff]", sample):
        return None
    return detect_encoding(sample, truncated=len(sample) >= SAMPLE_BYTES)


def _plan_dbf(filename: str, plan: InputPlan) -> None:
    args = plan.iterableargs
    if args.get("encoding"):
        return
    encoding = dbf_encoding(filename)
    if encoding:
        args["encoding"] = encoding
        plan.notes.append(f"encoding {encoding}")


# ---------------------------------------------------------------- GeoPackage and SQLite
SQLITE_EXTENSIONS = ("gpkg", "sqlite", "db")
# Tables of SQLite, GeoPackage and SpatiaLite about the data (virtual tables, such as the
# R-tree indexes rtree_* and idx_*, and their shadow tables are found by their SQL).
_SQLITE_META = re.compile(
    r"^(?:sqlite_|gpkg_|gpkgext_)|^(?:spatial_ref_sys(?:_aux)?|spatialite_history|sql_statements_log|"
    r"data_licenses|elementarygeometries|spatialindex|knn2?|(?:views_|virts_)?geometry_columns(?:_\w+)?)$",
    re.IGNORECASE,
)
# Geometry types of Well-Known Binary (ISO and EWKB codes).
_WKB_TYPES = {1: "Point", 2: "LineString", 3: "Polygon", 4: "MultiPoint", 5: "MultiLineString",
              6: "MultiPolygon", 7: "GeometryCollection"}
# Bytes of the envelope in a GeoPackage geometry header, by the envelope code of its flags.
_GPKG_ENVELOPE = {0: 0, 1: 32, 2: 48, 3: 48, 4: 64}


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _sqlite_connect(filename: str) -> sqlite3.Connection:
    """A read-only connection: scanning never changes the database."""
    from pathlib import Path

    return sqlite3.connect(Path(filename).resolve().as_uri() + "?mode=ro", uri=True)


def wkb_geometry(data: bytes, offset: int = 0) -> Tuple[Optional[dict], int]:
    """GeoJSON geometry of the Well-Known Binary at ``offset`` (ISO WKB, EWKB) and the offset
    after it. Coordinates keep x, y and z; m values are dropped. An empty point is None."""
    order = "<" if data[offset] == 1 else ">"
    (code,) = struct.unpack_from(order + "I", data, offset + 1)
    offset += 5
    has_z, has_m = bool(code & 0x80000000), bool(code & 0x40000000)
    if code & 0x20000000:  # EWKB with an SRID
        offset += 4
    code &= 0x0FFFFFFF
    base, dims_code = code % 1000, code // 1000
    has_z = has_z or dims_code in (1, 3)
    has_m = has_m or dims_code in (2, 3)
    kind = _WKB_TYPES.get(base)
    if kind is None:
        raise ValueError(f"WKB geometry type {code}")
    dims = 2 + has_z + has_m
    keep = 3 if has_z else 2

    def coordinates(at: int) -> list:
        return list(struct.unpack_from(order + "d" * dims, data, at)[:keep])

    def count(at: int) -> int:
        return struct.unpack_from(order + "I", data, at)[0]

    def line(at: int) -> Tuple[list, int]:
        n = count(at)
        at += 4
        return [coordinates(at + i * 8 * dims) for i in range(n)], at + n * 8 * dims

    if kind == "Point":
        point = coordinates(offset)
        geometry = None if any(math.isnan(v) for v in point) else {"type": kind, "coordinates": point}
        return geometry, offset + 8 * dims
    if kind == "LineString":
        points, offset = line(offset)
        return {"type": kind, "coordinates": points}, offset
    if kind == "Polygon":
        rings = []
        n = count(offset)
        offset += 4
        for _ in range(n):
            ring, offset = line(offset)
            rings.append(ring)
        return {"type": kind, "coordinates": rings}, offset
    parts = []
    n = count(offset)
    offset += 4
    for _ in range(n):
        part, offset = wkb_geometry(data, offset)
        if part is not None:
            parts.append(part)
    if kind == "GeometryCollection":
        return {"type": kind, "geometries": parts}, offset
    return {"type": kind, "coordinates": [part["coordinates"] for part in parts]}, offset


def gpkg_geometry(blob: Any) -> Optional[dict]:
    """GeoJSON geometry of a GeoPackage geometry blob (``GP`` header, envelope, WKB)."""
    if not isinstance(blob, (bytes, bytearray, memoryview)):
        return None
    blob = bytes(blob)
    if len(blob) < 8 or blob[:2] != b"GP" or blob[3] & 0x10:  # not a GeoPackage geometry, or empty
        return None
    try:
        return wkb_geometry(blob, 8 + _GPKG_ENVELOPE.get((blob[3] >> 1) & 0x07, 0))[0]
    except (struct.error, ValueError, IndexError):
        return None


def sqlite_data_tables(connection: sqlite3.Connection) -> Tuple[List[str], List[str]]:
    """The GeoPackage layers (feature tables, then attribute tables, as ``gpkg_contents`` lists
    them) and the other tables of a database, by name, without metadata, virtual and shadow
    tables."""
    rows = connection.execute("SELECT name, sql FROM sqlite_master WHERE type = 'table' ORDER BY name").fetchall()
    virtual = [name for name, sql in rows if (sql or "").lstrip().upper().startswith("CREATE VIRTUAL TABLE")]
    tables = [name for name, _ in rows if name not in virtual and not _SQLITE_META.match(name)
              and not any(name.startswith(v + "_") for v in virtual)]
    layers: List[str] = []
    if any(name == "gpkg_contents" for name, _ in rows):
        try:
            contents = connection.execute("SELECT table_name, data_type FROM gpkg_contents ORDER BY rowid").fetchall()
        except sqlite3.Error:
            contents = []
        for kind in ("features", "attributes"):
            layers += [name for name, data_type in contents if data_type == kind and name in tables]
    return layers, [name for name in tables if name not in layers]


def sqlite_records(filename: str, table: str, geometry_columns: Iterable[str] = ()) -> Iterator[dict]:
    """Rows of a table, read one at a time. With ``geometry_columns`` (a GeoPackage feature
    table) a row is a GeoJSON-like feature, as fiona reads it: the first geometry column
    decoded to ``geometry``, the other columns except the primary key in ``properties``."""
    connection = _sqlite_connect(filename)
    try:
        geometry_columns = set(geometry_columns)
        key = set()
        if geometry_columns:
            info = connection.execute(f"PRAGMA table_info({_quote_ident(table)})").fetchall()
            key = {row[1] for row in info if row[5]}
        cursor = connection.execute(f"SELECT * FROM {_quote_ident(table)}")  # nosec B608 - quoted catalog name
        names = [d[0] for d in cursor.description]
        for row in cursor:
            if not geometry_columns:
                yield dict(zip(names, row))
                continue
            properties: Dict[str, Any] = {}
            geometry = None
            for name, value in zip(names, row):
                if name in geometry_columns:
                    geometry = geometry or gpkg_geometry(value)
                elif name not in key:
                    properties[name] = value
            yield {"type": "Feature", "properties": properties, "geometry": geometry}
    finally:
        connection.close()


def _geometry_columns(connection: sqlite3.Connection, table: str) -> List[str]:
    """Geometry columns of a GeoPackage feature table."""
    try:
        return [row[0] for row in connection.execute(
            "SELECT column_name FROM gpkg_geometry_columns WHERE table_name = ?", (table,))]
    except sqlite3.Error:
        return []


def _plan_sqlite(filename: str, ext: str, plan: InputPlan) -> None:
    """Read the first GeoPackage layer, or name the first data table of another database for
    iterabledata (it reads the first table by name, which may hold metadata). A table named by
    the caller (``scan file --table``) is read the same way: a GeoPackage layer as features."""
    args = plan.iterableargs
    if args.get("query"):
        return
    given = args.get("table")
    try:
        connection = _sqlite_connect(filename)
        try:
            layers, others = sqlite_data_tables(connection)
            table = given or (layers + others or [None])[0]
            geometry_columns = _geometry_columns(connection, table) if table in layers else []
            first = connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name LIMIT 1").fetchone()
        finally:
            connection.close()
    except sqlite3.Error as exc:  # not a database: iterabledata reports the error
        logger.debug("Could not read %s as SQLite: %s", filename, exc)
        return
    if table is None:
        return
    tables = layers + others
    rest = f"; not scanned: {', '.join(t for t in tables if t != table)}" if len(tables) > 1 and not given else ""
    if table in layers or ext == "gpkg":  # iterabledata reads GeoPackage files with fiona
        plan.records = sqlite_records(filename, table, geometry_columns)
        plan.kind = "gpkg" if layers else "sqlite"
        plan.notes.append(f"{'GeoPackage layer' if table in layers else 'table'} {table!r}{rest}")
        args.pop("table", None)
    elif given:
        return
    elif first is None or first[0] != table or rest:
        args["table"] = table
        plan.notes.append(f"table {table!r}{rest}")


def select_tables(filename: str, table: Any) -> Optional[List[str]]:
    """Names of the tables of a SQLite database or GeoPackage that ``--table`` names.

    ``table`` is a table name, a number counted from 1 in the order of ``all``, or ``all``:
    the GeoPackage layers (as ``gpkg_contents`` lists them), then the other tables by name,
    without metadata, virtual and shadow tables. A name may also be one of those. ``all`` on a
    file that is not a database gives None (the file is one table); a name or number there,
    or one that the database lacks, raises ``ValueError``.
    """
    ext, codec = split_name(filename)
    value = str(table).strip()
    base = os.path.basename(filename)
    if ext not in SQLITE_EXTENSIONS or codec is not None:
        if value.lower() == "all":
            return None
        raise ValueError(f"--table applies to SQLite databases and GeoPackages, not {base}")
    try:
        connection = _sqlite_connect(filename)
        try:
            layers, others = sqlite_data_tables(connection)
            names = [row[0] for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")]
        finally:
            connection.close()
    except sqlite3.Error as exc:
        raise ValueError(f"{base} is not a SQLite database: {exc}") from exc
    tables = layers + others
    if value.lower() == "all":
        return tables
    if value in names:
        return [value]
    if value.isdigit() and 1 <= int(value) <= len(tables):
        return [tables[int(value) - 1]]
    listed = ", ".join(tables) or "none"
    raise ValueError(f"No table {value!r} in {base}; data tables: {listed}")


# ---------------------------------------------------------------- iCalendar
ICAL_COMPONENTS = ("VEVENT",)


def _ical_value(value: Any) -> Any:
    """Text of an iCalendar property value; dates and times in ISO format."""
    if isinstance(value, list):
        return [_ical_value(item) for item in value]
    moment = getattr(value, "dt", None)
    if moment is not None and hasattr(moment, "isoformat"):
        return moment.isoformat()
    if isinstance(value, str):  # vText, vCalAddress, vUri
        return str(value)
    to_ical = getattr(value, "to_ical", None)
    if callable(to_ical):
        text = to_ical()
        return text.decode("utf-8", errors="replace") if isinstance(text, bytes) else str(text)
    return str(value)


def ical_records(filename: str) -> List[dict]:
    """Events of an iCalendar file: one record per VEVENT, keyed by the lower-case names of its
    own properties (iterabledata's names), without the properties of nested components such
    as VALARM and without the BEGIN / END lines."""
    from icalendar import Calendar

    with open(filename, "rb") as fh:
        calendar = Calendar.from_ical(fh.read())
    records = []
    for component in calendar.walk():
        if component.name in ICAL_COMPONENTS:
            records.append({name.lower(): _ical_value(value) for name, value in component.items()})
    return records


def _plan_ical(filename: str, plan: InputPlan) -> None:
    try:
        import icalendar  # noqa: F401
    except ImportError:
        return  # iterabledata names the missing package (or reads the file with ics)
    try:
        records = ical_records(filename)
    except Exception as exc:  # noqa: BLE001 - iterabledata reports the error
        logger.debug("Could not read %s as iCalendar: %s", filename, exc)
        return
    plan.records = records
    plan.kind = "ical"


# ---------------------------------------------------------------- KML
KML_NULLS = {"<Null>", "<Nulo>"}  # ArcGIS writes empty attributes so in description tables


def _local(tag: Any) -> str:
    return tag.rsplit("}", 1)[-1] if isinstance(tag, str) else ""


class _CellRows(HTMLParser):
    """Cell texts of the table rows of an HTML fragment (rows of nested tables included)."""

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.rows: List[List[str]] = []
        self._row: Optional[List[str]] = None
        self._cell: Optional[List[str]] = None

    def handle_starttag(self, tag, attrs):
        if tag == "tr":
            self._row = []
        elif tag in ("td", "th") and self._row is not None:
            self._cell = []

    def handle_endtag(self, tag):
        if tag in ("td", "th") and self._row is not None and self._cell is not None:
            self._row.append(" ".join("".join(self._cell).split()))
            self._cell = None
        elif tag == "tr" and self._row is not None:
            self.rows.append(self._row)
            self._row = None

    def handle_data(self, data):
        if self._cell is not None:
            self._cell.append(data)


def description_attributes(html: str) -> Dict[str, Optional[str]]:
    """Name / value rows of an attribute table in a KML description (ArcGIS pop-ups:
    ``<tr><td>COD_DIS</td><td>17</td></tr>``); {} unless two or more such rows."""
    if "<td" not in html.lower():
        return {}
    parser = _CellRows()
    try:
        parser.feed(html)
        parser.close()
    except Exception:  # noqa: BLE001 - malformed markup: no attributes
        return {}
    pairs = [row for row in parser.rows if len(row) == 2 and row[0]]
    if len(pairs) < 2:
        return {}
    return {name: (None if value in KML_NULLS else value) for name, value in pairs}


def _kml_coordinates(element) -> Optional[list]:
    for child in element.iter():
        if _local(child.tag) == "coordinates" and child.text:
            coords = []
            for item in child.text.split():
                parts = item.split(",")
                if len(parts) < 2:
                    continue
                try:
                    coords.append([float(p) for p in parts[:3]])
                except ValueError:
                    continue
            return coords
    return None


def _kml_geometry(placemark) -> Optional[dict]:
    """The first Point, else LineString, else Polygon of a placemark (as iterabledata has it)."""
    found: Dict[str, Any] = {}
    for element in placemark.iter():
        name = _local(element.tag)
        if name in ("Point", "LineString", "Polygon"):
            found.setdefault(name, element)
    if "Point" in found:
        coords = _kml_coordinates(found["Point"])
        return {"type": "Point", "coordinates": coords[0]} if coords else None
    if "LineString" in found:
        coords = _kml_coordinates(found["LineString"])
        return {"type": "LineString", "coordinates": coords} if coords else None
    if "Polygon" in found:
        outer = next((e for e in found["Polygon"].iter() if _local(e.tag) == "outerBoundaryIs"), None)
        coords = _kml_coordinates(outer) if outer is not None else None
        return {"type": "Polygon", "coordinates": [coords]} if coords else None
    return None


def kml_feature(placemark) -> dict:
    """GeoJSON-like feature of a KML Placemark, as iterabledata reads it plus the attributes
    of ``SchemaData/SimpleData`` and of an attribute table in the description."""
    properties: Dict[str, Any] = {}
    for element in placemark.iter():
        tag = _local(element.tag)
        if tag == "Data" and element.get("name"):
            value = next((c for c in element if _local(c.tag) == "value"), None)
            if value is not None:
                properties[element.get("name")] = value.text or ""
        elif tag == "SimpleData" and element.get("name"):
            properties[element.get("name")] = element.text or ""
    description = None
    for child in placemark:
        tag = _local(child.tag)
        if tag == "name" and child.text:
            properties["name"] = child.text
        elif tag == "description" and child.text:
            description = properties["description"] = child.text
    if description is not None:
        for key, value in description_attributes(description).items():
            properties.setdefault(key, value)
    return {"type": "Feature", "properties": properties, "geometry": _kml_geometry(placemark)}


def _kml_stream(filename: str, ext: str):
    if ext == "kml":
        return open(filename, "rb")
    archive = zipfile.ZipFile(filename)
    names = [n for n in archive.namelist() if n.lower().endswith(".kml")]
    if not names:
        archive.close()
        raise ValueError("no .kml file in the KMZ archive")
    name = "doc.kml" if "doc.kml" in names else names[0]
    stream = archive.open(name)
    original_close = stream.close

    def close():
        original_close()
        archive.close()

    stream.close = close  # type: ignore[method-assign]
    return stream


def _iterparse(stream, events: Tuple[str, ...] = ("end",)):
    """Element events of an XML stream: lxml without entities, DTDs or network access (it
    reads every declared encoding, Shift_JIS included), else the standard library."""
    try:
        from lxml import etree

        return etree.iterparse(stream, events=events, resolve_entities=False, no_network=True,
                               load_dtd=False, recover=True, huge_tree=False)
    except ImportError:  # lxml is in the formats extra
        from xml.etree import ElementTree

        return ElementTree.iterparse(stream, events=events)


def kml_records(filename: str, ext: str = "kml") -> Iterator[dict]:
    """Features of the placemarks that have a geometry, read one placemark at a time."""
    with _kml_stream(filename, ext) as stream:
        try:
            for _, element in _iterparse(stream):
                if _local(element.tag) != "Placemark":
                    continue
                feature = kml_feature(element)
                element.clear()
                if feature["geometry"] is not None:
                    yield feature
        except Exception as exc:  # noqa: BLE001 - keep the placemarks read so far
            logger.warning("%s: KML parsing stopped: %s", filename, exc)


def _plan_kml(filename: str, ext: str, plan: InputPlan) -> None:
    records = kml_records(filename, ext)
    try:
        first = next(records)
    except StopIteration:
        return  # no placemark read here: let iterabledata try
    except Exception as exc:  # noqa: BLE001 - iterabledata reports the error
        logger.debug("Could not read %s as KML: %s", filename, exc)
        return
    plan.records = _prepend(first, records)
    plan.kind = ext


def _prepend(first: Any, rest: Iterator[Any]) -> Iterator[Any]:
    """``first`` and then ``rest``; closing it closes ``rest`` (and its file)."""
    yield first
    yield from rest


# ---------------------------------------------------------------- XML column lists
def xml_header_names(data: bytes, tagname: str) -> Optional[Tuple[str, List[str]]]:
    """(child tag, column names) when the ``tagname`` records repeat one child element and an
    earlier sibling lists as many distinct names in the same element:
    ``<header><column>選挙名</column>...</header><body><column>市長選挙</column>...</body>``."""
    tagname = _local(tagname)
    depth = 0
    headers: Dict[int, Tuple[str, List[str]]] = {}  # depth -> the last header-like element there
    try:
        for event, element in _iterparse(io.BytesIO(data), ("start", "end")):
            if event == "start":
                depth += 1
                continue
            depth -= 1
            children = list(element)
            tags = {_local(c.tag) for c in children}
            if _local(element.tag) == tagname:
                if len(tags) != 1 or len(children) < 2:
                    return None
                header = headers.get(depth)
                if header and header[0] == next(iter(tags)) and len(header[1]) == len(children):
                    return header
                return None
            if len(tags) == 1 and len(children) >= 2 and not any(len(c) for c in children):
                names = [" ".join((c.text or "").split()) for c in children]
                if all(names) and len(set(names)) == len(names):
                    headers[depth] = (next(iter(tags)), names)
    except Exception:  # noqa: BLE001 - a sample that ends inside the document, or broken XML
        return None
    return None


def _column_record(child: str, names: List[str]) -> Callable[[Any], Any]:
    def pair(record: Any) -> Any:
        if not isinstance(record, dict) or child not in record:
            return record
        values = record[child] if isinstance(record[child], list) else [record[child]]
        out = {k: v for k, v in record.items() if k != child}
        out.update(zip(names, values))
        if len(values) > len(names):
            out[child] = values[len(names):]
        return out

    return pair


def _plan_xml(filename: str, codec: Optional[str], plan: InputPlan) -> None:
    sample = read_bytes(filename, codec)
    if not sample:
        return
    found = xml_header_names(sample, str(plan.iterableargs["tagname"]))
    if found is None:
        return
    child, names = found
    plan.transform = _column_record(child, names)
    plan.notes.append(f"<{child}> values named after the {len(names)} names of the header element")
