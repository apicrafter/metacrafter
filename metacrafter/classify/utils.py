# -*- coding: utf-8 -*-
"""Utility functions that help to work with data"""
import re
from collections import defaultdict, OrderedDict

import chardet
import xmltodict


def dict_generator(indict, pre=None):
    """Flatten objects; dots/backslashes in literal keys are escaped.

    Arrays share a field path; each scalar element is a value. Empty containers
    contribute None. MongoDB _id is intentionally excluded for compatibility.
    """
    pre = list(pre or [])
    if not pre and indict == {}:
        return
    if isinstance(indict, dict) and indict:
        for key, value in indict.items():
            if key == "_id":
                continue
            segment = str(key).replace("\\", "\\\\").replace(".", "\\.")
            yield from dict_generator(value, pre + [segment])
    elif isinstance(indict, (list, tuple)) and indict:
        for value in indict:
            yield from dict_generator(value, pre)
    else:
        yield pre + [None if isinstance(indict, (dict, list, tuple)) else indict]


def headers(data, limit=None):
    """Discover every field in the provided sample in stable encounter order."""
    return list(dict_to_columns(data if limit is None else data[:limit]))


def get_dict_value(object, keys):
    """Returns single value from dictionary object"""
    out = []
    if object is None:
        return out
    #    keys = key.split('.')
    if len(keys) == 1:
        if isinstance(object, dict):
            if keys[0] in object:
                out.append(object[keys[0]])
        else:
            for record in object:
                if record and keys[0] in record:
                    out.append(record[keys[0]])
    #        return out
    else:
        if isinstance(object, dict):
            if keys[0] in object:
                out.extend(get_dict_value(object[keys[0]], keys[1:]))
        else:
            for record in object:
                if keys[0] in record:
                    out.extend(get_dict_value(record[keys[0]], keys[1:]))
    return out


def dict_to_columns(data):
    """Collect scalar values and count missing paths as null for each row."""
    rows = []
    keys = {}
    for row in data:
        current = {}
        for pair in dict_generator(row):
            key = ".".join(pair[:-1])
            current.setdefault(key, []).append(pair[-1])
            keys.setdefault(key, None)
        rows.append(current)
    return {key: [v for row in rows for v in row.get(key, [None])] for key in keys}


def string_to_charrange(s):
    """Returns array of chars from string"""
    chars = {}
    for ch in s:
        v = chars.get(ch, 0)
        chars[ch] = v + 1
    return chars


def string_array_to_charrange(sarr):
    """Returns char map from array"""
    chars = {}
    for s in sarr:
        for ch in s:
            v = chars.get(ch, 0)
            chars[ch] = v + 1
    return chars


def detect_encoding(filename, limit=1000000):
    """Detects encoding of the filename.
    
    Args:
        filename: Path to the file to analyze
        limit: Maximum bytes to read for detection (default: 1MB)
        
    Returns:
        Dictionary with encoding detection results from chardet
        
    Raises:
        IOError: If file cannot be read
    """
    with open(filename, "rb") as f:
        chunk = f.read(limit)
    detected = chardet.detect(chunk)
    return detected


def detect_delimiter(filename, encoding="utf8"):
    """Detects CSV file delimiter by analyzing first line.
    
    Args:
        filename: Path to the CSV file
        encoding: File encoding (default: utf8)
        
    Returns:
        Most likely delimiter character (',', ';', '\t', or '|')
        
    Raises:
        IOError: If file cannot be read
    """
    with open(filename, "r", encoding=encoding) as f:
        line = f.readline()
    dict1 = {
        ",": line.count(","),
        ";": line.count(";"),
        "\t": line.count("\t"),
        "|": line.count("|"),
    }
    delimiter = max(dict1, key=dict1.get)
    return delimiter


def etree_to_dict(t, prefix_strip=True):
    """Lxml etree converted to Python dictionary for JSON serialization"""
    tag = t.tag if not prefix_strip else t.tag.rsplit("}", 1)[-1]
    d = {tag: {} if t.attrib else None}
    children = list(t)
    if children:
        dd = defaultdict(list)
        for dc in map(etree_to_dict, children):
            #            print(dir(dc))
            for k, v in dc.items():
                if prefix_strip:
                    k = k.rsplit("}", 1)[-1]
                dd[k].append(v)
        d = {tag: {k: v[0] if len(v) == 1 else v for k, v in dd.items()}}
    if t.attrib:
        d[tag].update(("@" + k.rsplit("}", 1)[-1], v) for k, v in t.attrib.items())
    if t.text:
        text = t.text.strip()
        if children or t.attrib:
            tag = tag.rsplit("}", 1)[-1]
            if text:
                d[tag]["#text"] = text
        else:
            d[tag] = text
    return d


def _seek_xml_lists(data, level=0, path=None, candidates=None):
    """Seeks XML lists to find items tags"""
    if candidates is None:
        candidates = OrderedDict()
    for key, value in data.items():
        if isinstance(value, list):
            key = f"{path}.{key}" if path is not None else key
            if key not in candidates:
                candidates[key] = {"key": key, "num": len(value)}
        elif isinstance(value, (OrderedDict, dict)):
            res = _seek_xml_lists(
                value, level + 1, f"{path}.{key}" if path else key, candidates
            )
            for k, v in res.items():
                if k not in candidates:
                    candidates[k] = v
        else:
            continue
    return candidates


def xml_quick_analyzer(filename):
    """Discover a record tag from a bounded streaming prefix of XML."""
    import bz2
    import gzip
    import lzma
    from xml.etree import ElementTree
    from contextlib import ExitStack
    suffix = str(filename).lower()
    opener = gzip.open if suffix.endswith(".gz") else bz2.open if suffix.endswith(".bz2") else lzma.open if suffix.endswith((".xz", ".lzma")) else open
    if suffix.endswith(".lz4"):
        import lz4.frame
        opener = lz4.frame.open
    if suffix.endswith(".zst"):
        import zstandard
        opener = lambda path, mode: zstandard.open(path, mode)
    stack, counts, containers = [], {}, []
    events = 0
    with ExitStack() as resources:
        stream = resources.enter_context(opener(filename, "rb"))
        for event, element in ElementTree.iterparse(stream, events=("start", "end")):
            if event == "start":
                if len(stack) >= 32:
                    raise ValueError("XML nesting limit exceeded; supply a record tag")
                stack.append(element.tag)
                key = tuple(stack)
                counts[key] = counts.get(key, 0) + 1
                events += 1
                if len(stack) == 3 and tuple(stack[:2]) not in containers:
                    containers.append(tuple(stack[:2]))
                if len(stack) == 2 and counts[key] > 1:
                    return {"full": ".".join(key), "short": key[-1].rsplit("}", 1)[-1]}
                if events >= 2000:
                    break
            else:
                stack.pop()
                element.clear()
    repeated = [key for key, count in counts.items() if count > 1]
    candidates = sorted(repeated, key=len) or containers
    if candidates:
        key = candidates[0]
        return {"full": ".".join(key), "short": key[-1].rsplit("}", 1)[-1]}
    return None


def get_supported_formats():
    """Get list of all supported file formats.
    
    Returns:
        List of supported format strings (e.g., ['csv', 'json', 'parquet', ...])
    """
    from metacrafter.core import SUPPORTED_FILE_TYPES
    return sorted(set(SUPPORTED_FILE_TYPES))


def get_supported_codecs():
    """Get list of all supported compression codecs.
    
    Returns:
        List of supported codec strings (e.g., ['gz', 'bz2', 'xz', ...])
    """
    from metacrafter.core import CODECS
    return sorted(set(CODECS))


def is_format_supported(filename):
    """Check if file format is supported based on extension.
    
    Args:
        filename: Path to file or filename string
        
    Returns:
        True if format is supported, False otherwise
    """
    from metacrafter.core import SUPPORTED_FILE_TYPES
    
    # Extract extension, handling compressed files
    parts = filename.rsplit(".", 2)
    if len(parts) >= 2:
        # Check if it's a compressed file (e.g., .csv.gz)
        if len(parts) >= 3 and parts[-1].lower() in get_supported_codecs():
            ext = parts[-2].lower()
        else:
            ext = parts[-1].lower()
    else:
        return False
    
    return ext in SUPPORTED_FILE_TYPES


def get_format_from_filename(filename):
    """Extract format from filename, handling compression.
    
    Args:
        filename: Path to file or filename string
        
    Returns:
        Tuple of (format, codec) where format is the data format and
        codec is the compression codec (or None if uncompressed).
        Returns (None, None) if format cannot be determined.
    """
    from metacrafter.core import SUPPORTED_FILE_TYPES, CODECS
    
    parts = filename.rsplit(".", 2)
    if len(parts) < 2:
        return (None, None)
    
    # Check if last part is a compression codec
    last_part = parts[-1].lower()
    if last_part in CODECS and len(parts) >= 3:
        codec = last_part
        format_ext = parts[-2].lower()
    else:
        codec = None
        format_ext = last_part
    
    if format_ext in SUPPORTED_FILE_TYPES:
        return (format_ext, codec)
    
    return (None, None)

# ISO 639-1 codes used as column-name suffixes in multilingual data (title_en, name_ru).
# Codes that are also common English words or abbreviations (id, no, is, am, ms, ca, hi,
# be, to, ...) are left out, so user_id never becomes user.
LANGUAGE_SUFFIXES = frozenset(
    "en ru fr de es pt it nl pl uk zh ja ko ar hy ka az kk ky uz tr fa el cs sk sl hr sr bg "
    "ro hu fi sv da et lv lt mn tg tk vi th".split()
)
_LANGUAGE_SUFFIX = re.compile(r"^(?P<base>.+?)[_\-.](?P<lang>[A-Za-z]{2})$")


def strip_language_suffix(name):
    """Field name without a trailing language code: ``title_en`` -> ``title``.

    Returns ``name`` unchanged when it has no known language suffix.
    """
    match = _LANGUAGE_SUFFIX.match(str(name))
    if match and match.group("lang").lower() in LANGUAGE_SUFFIXES:
        return match.group("base")
    return name


def strip_values(value):
    """Copy of ``value`` with leading and trailing whitespace removed from every string.

    Dicts and lists are walked recursively; keys and non-string values are kept as they are.
    """
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, dict):
        return {key: strip_values(item) for key, item in value.items()}
    if isinstance(value, list):
        return [strip_values(item) for item in value]
    return value


# Largest integer a float holds exactly (2**53); larger whole floats may have lost digits.
_MAX_EXACT_FLOAT_INT = 2 ** 53


def whole_number_texts(values, empty_values=()):
    """Integer text of a float column's values when every value is a whole number.

    Identifiers such as registration numbers reach the scanner as floats when a source
    stores numbers as floats (``.xls`` files, pandas columns with missing values, CSV text
    ``1027700132195.0``). Returns a list with ``str(int(value))`` for each whole number and
    ``""`` for an empty, ``empty_values`` or NaN value, or ``None`` when any value is
    fractional, too large to be exact, or not a number, or when no value is a number.
    """
    texts = []
    for value in values:
        if value is None or (isinstance(value, str) and (not value.strip() or value in empty_values)):
            texts.append("")
            continue
        if isinstance(value, bool):
            return None
        if isinstance(value, int):
            texts.append(str(value))
            continue
        if isinstance(value, str):
            try:
                value = float(value)
            except ValueError:
                return None
        if not isinstance(value, float):
            return None
        if value != value:  # NaN
            texts.append("")
            continue
        if not value.is_integer() or abs(value) >= _MAX_EXACT_FLOAT_INT:
            return None
        texts.append(str(int(value)))
    if not any(texts):
        return None
    return texts
