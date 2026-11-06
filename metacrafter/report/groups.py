"""Field groups: sibling fields that share a name prefix or suffix (issue #12).

Flat tables often encode structure in names: ``place``, ``placeURL``, ``placeLocation`` and
``placeType`` describe one object (``place``) with four parts; ``reviewScore`` and ``avgScore``
share a value type (``score``). Names are split on case changes, ``_``, ``-`` and other
separators. Fields are compared with their siblings only (the same parent path), so nested
objects that are already structured are not regrouped across levels. Language variants
(``title``, ``title_en``, ``title_ru``) form ``language`` groups and are compared by their base
name for the other groups.
"""
from __future__ import annotations

import re
from collections import defaultdict
from typing import Any, Dict, Iterable, List, Tuple

_CAMEL = re.compile(r"(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])")
_TOKEN = re.compile(r"[A-Za-z0-9]+")
# Prefixes of boolean flags and other generic words that do not name an object.
PREFIX_STOP = frozenset("is has can did does was the num no not".split())
MIN_PREFIX_LEN = 3
MIN_GROUP = 2


def split_name(name: str) -> List[str]:
    """Lower-case tokens of a field name: ``placeURL`` -> ``["place", "url"]``."""
    return [t.lower() for part in _TOKEN.findall(str(name)) for t in _CAMEL.split(part) if t]


def _split_path(field: str) -> Tuple[str, str]:
    parent, _, name = str(field).rpartition(".")
    return parent, name


def field_groups(fields: Iterable[str]) -> List[Dict[str, Any]]:
    """Language, prefix and suffix groups among sibling fields.

    A prefix group has at least two fields whose first token is the same word of three or
    more letters (not a flag word such as ``is`` or ``has``), and at least one of them has
    more tokens; ``parts`` gives the rest of each name (``""`` for the bare prefix). A suffix
    group has at least two multi-token fields with the same last token.
    """
    siblings: Dict[str, List[str]] = defaultdict(list)
    for field in fields:
        parent, name = _split_path(field)
        if name:
            siblings[parent].append(name)
    from metacrafter.classify.utils import strip_language_suffix

    groups: List[Dict[str, Any]] = []
    for parent, names in sorted(siblings.items()):
        path = (lambda m: f"{parent}.{m}" if parent else m)
        variants: Dict[str, List[str]] = defaultdict(list)
        for name in names:
            variants[strip_language_suffix(name)].append(name)
        for base, members in sorted(variants.items()):
            if len(members) >= MIN_GROUP:
                groups.append({
                    "kind": "language", "name": base, "parent": parent,
                    "fields": [path(m) for m in members],
                    "parts": [m[len(base) + 1:].lower() if m != base else "" for m in members],
                })
        names = sorted(variants)
        tokens = {name: split_name(name) for name in names}
        by_prefix: Dict[str, List[str]] = defaultdict(list)
        by_suffix: Dict[str, List[str]] = defaultdict(list)
        for name in names:
            parts = tokens[name]
            if not parts:
                continue
            by_prefix[parts[0]].append(name)
            if len(parts) > 1:
                by_suffix[parts[-1]].append(name)
        for prefix, members in sorted(by_prefix.items()):
            if (len(members) < MIN_GROUP or len(prefix) < MIN_PREFIX_LEN or prefix in PREFIX_STOP
                    or all(len(tokens[m]) == 1 for m in members)):
                continue
            groups.append({
                "kind": "prefix", "name": prefix, "parent": parent,
                "fields": [path(m) for m in members],
                "parts": ["_".join(tokens[m][1:]) for m in members],
            })
        for suffix, members in sorted(by_suffix.items()):
            if len(members) < MIN_GROUP:
                continue
            groups.append({
                "kind": "suffix", "name": suffix, "parent": parent,
                "fields": [path(m) for m in members],
            })
    return groups
