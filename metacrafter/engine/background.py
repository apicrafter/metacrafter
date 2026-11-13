"""Background token classes: how generic a value pattern is in ordinary data.

Registry specificity (``1 - broadness``) is measured against the examples of other registry
records. Those examples are mostly long, distinctive identifiers, so a pattern such as
``\\d{1,4}`` or ``[A-Za-z]{2,63}`` still scores close to 1 there, while in real tables it
matches every counter, small id, year or plain word.

This module measures a pattern against classes of ordinary tokens instead: integers of
one to twenty digits (one class per length), plain words, title-case words, short
acronyms, two-letter codes and letter-and-digit codes (WGS84, MP3, A320). A pattern that accepts at least ``GENERIC_THRESHOLD`` of any one class cannot
tell its datatype apart from ordinary data by values alone. Long integers are ordinary
too: phone numbers, tax and registration numbers, bank accounts and record ids, so a
pattern such as ``[1-9]\\d{9,11}`` is generic.
"""
from __future__ import annotations

import re
from functools import lru_cache
from typing import Callable, Dict, List, Optional, Tuple

GENERIC_THRESHOLD = 0.3

_WORDS = (
    "data local dataset publication image original html admin public private draft "
    "environment boundaries temperature precipitation water river forest name title value "
    "status type code count total active inactive true false yes none null user group store "
    "layer map file report table record item order price city street country region email "
    "phone date time year month week note description comment source target result error "
    "warning info level score rating share default custom other unknown main home page "
    "service list view open closed pending approved new old high low medium small"
).split()
# Two-letter tokens: abbreviations, US state codes, units and short words.
_CODES2 = "ok no on in id us uk eu un hr pm am tv pc mm cm kg km ny ca tx fl wa il pa oh mi ga nc".split()
# Letter-and-digit codes: standards, formats, versions, models, grades.
_CODES_NUM = (
    "WGS84 UTF8 SHA1 SHA256 MD5 ISO9001 MP3 MP4 H264 B12 E10 GT500 F16 PS4 USB3 TLS12 EA1 EA2 "
    "IPV6 X11 A320 B737 Q3 FY2020 V2 PM10 CO2 NO2 R2"
).split()
_ACRONYMS = (
    "PDF PNG ZIP CSV XML GIF KML API URL USA EUR GIS WMS WFS SQL CSS PHP RSS FAQ CEO "
    "JPEG JSON HTML TIFF GZIP NULL TRUE NONE DATA TEST HTTP UTF8 SHA1 YAML"
).split()


# Integer classes longer than six digits: ``LONG_INT_SAMPLES`` numbers per length, spread
# over the whole range so that every leading digit occurs.
LONG_INT_DIGITS = range(7, 21)
LONG_INT_SAMPLES = 100


def _long_ints(digits: int) -> List[str]:
    low = 10 ** (digits - 1)
    step = 9 * low // LONG_INT_SAMPLES
    return [str(low + i * step + (i * 7919) % step) for i in range(LONG_INT_SAMPLES)]


def _classes() -> Dict[str, List[str]]:
    words = sorted(set(_WORDS))
    return {
        "int1": [str(i) for i in range(0, 10)],
        "int2": [str(i) for i in range(10, 100)],
        "int3": [str(i) for i in range(100, 1000, 3)],
        "int4": [str(i) for i in range(1000, 10000, 29)],
        "int5": [str(i) for i in range(10000, 100000, 293)],
        "int6": [str(i) for i in range(100000, 1000000, 2999)],
        **{f"int{digits}": _long_ints(digits) for digits in LONG_INT_DIGITS},
        "word": words,
        "title_word": [w.capitalize() for w in words],
        "acronym": _ACRONYMS,
        "code2": _CODES2 + [c.upper() for c in _CODES2],
        "code_num": _CODES_NUM,
    }


BACKGROUND_CLASSES: Dict[str, List[str]] = _classes()


@lru_cache(maxsize=8192)
def background_broadness(regexp: str) -> Tuple[float, Optional[str]]:
    """Largest share of one background class that ``regexp`` fully matches, and that class.

    Returns ``(0.0, None)`` for a pattern that does not compile.
    """
    try:
        compiled = re.compile(regexp)
    except re.error:
        return 0.0, None
    best, best_class = 0.0, None
    for name, tokens in BACKGROUND_CLASSES.items():
        share = sum(1 for token in tokens if compiled.fullmatch(token)) / len(tokens)
        if share > best:
            best, best_class = share, name
    return round(best, 3), best_class


#: Tokens per background class that :func:`check_broadness` tries (evenly spaced).
CHECK_SAMPLE = 40


def check_broadness(test: Callable[[str], bool], per_class: int = CHECK_SAMPLE) -> Tuple[float, Optional[str]]:
    """Largest share of one background class whose tokens pass ``test``, and that class.

    The measure of :func:`background_broadness` for any value check: a value rule, a regexp
    with its checksum, a validator. It tries ``per_class`` evenly spaced tokens of each class
    (about 1000 in all), since a check such as a phone number parser is slow. A check that
    raises on a token rejects it.
    """
    best, best_class = 0.0, None
    for name, tokens in BACKGROUND_CLASSES.items():
        tokens = tokens[:: max(1, -(-len(tokens) // per_class))]
        passed = 0
        for token in tokens:
            try:
                passed += bool(test(token))
            except Exception:  # noqa: BLE001 - a failing check rejects the token
                pass
        share = passed / len(tokens)
        if share > best:
            best, best_class = share, name
    return round(best, 3), best_class


def is_generic(regexp: str, threshold: float = GENERIC_THRESHOLD) -> bool:
    """True when ``regexp`` accepts at least ``threshold`` of some background class."""
    return background_broadness(regexp)[0] >= threshold
