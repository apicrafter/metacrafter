"""Background token classes: how generic a value pattern is in ordinary data.

Registry specificity (``1 - broadness``) is measured against the examples of other registry
records. Those examples are mostly long, distinctive identifiers, so a pattern such as
``\\d{1,4}`` or ``[A-Za-z]{2,63}`` still scores close to 1 there, while in real tables it
matches every counter, small id, year or plain word.

This module measures a pattern against classes of ordinary tokens instead: integers of
one to six digits (one class per length), plain words, title-case words and short
acronyms. A pattern that accepts at least ``GENERIC_THRESHOLD`` of any one class cannot
tell its datatype apart from ordinary data by values alone.
"""
from __future__ import annotations

import re
from functools import lru_cache
from typing import Dict, List, Optional, Tuple

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
_ACRONYMS = (
    "PDF PNG ZIP CSV XML GIF KML API URL USA EUR GIS WMS WFS SQL CSS PHP RSS FAQ CEO "
    "JPEG JSON HTML TIFF GZIP NULL TRUE NONE DATA TEST HTTP UTF8 SHA1 YAML"
).split()


def _classes() -> Dict[str, List[str]]:
    words = sorted(set(_WORDS))
    return {
        "int1": [str(i) for i in range(0, 10)],
        "int2": [str(i) for i in range(10, 100)],
        "int3": [str(i) for i in range(100, 1000, 3)],
        "int4": [str(i) for i in range(1000, 10000, 29)],
        "int5": [str(i) for i in range(10000, 100000, 293)],
        "int6": [str(i) for i in range(100000, 1000000, 2999)],
        "word": words,
        "title_word": [w.capitalize() for w in words],
        "acronym": _ACRONYMS,
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


def is_generic(regexp: str, threshold: float = GENERIC_THRESHOLD) -> bool:
    """True when ``regexp`` accepts at least ``threshold`` of some background class."""
    return background_broadness(regexp)[0] >= threshold
