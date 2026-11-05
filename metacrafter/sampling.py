"""Bounded sampling over one-pass record iterators."""

from typing import Any, Dict, Iterable, List, Tuple
import random
from itertools import islice
from metacrafter.reports import validate_options


def sample_records(records: Iterable[Dict[str, Any]], limit: int = 1000, method: str = "head", seed: int = 0) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    validate_options(limit, 0)
    if method not in ("head", "reservoir"):
        raise ValueError("sampling must be head or reservoir")
    rng = random.Random(seed)
    retained: List[Dict[str, Any]] = []
    observed = 0
    iterator = islice(records, limit) if method == "head" else records
    for observed, row in enumerate(iterator, 1):
        if not isinstance(row, dict):
            raise ValueError("Data records must be objects")
        if len(retained) < limit:
            retained.append(row)
        else:
            index = rng.randrange(observed)
            if index < limit:
                retained[index] = row
    return retained, {
        "method": method,
        "seed": seed if method == "reservoir" else None,
        "observed_count": observed,
        "retained_count": len(retained),
        "population_count_known": method == "reservoir" or observed < limit,
        "statistics_scope": "sample",
    }
