# -*- coding: utf-8 -*-
"""Registry loader for Metacrafter datatypes."""
import json
import logging
from typing import List, Dict, Any, Optional, Union
from pathlib import Path

# Try to use orjson if available for faster JSON parsing
try:
    import orjson
    _json_loads = orjson.loads
except ImportError:
    _json_loads = json.loads

logger = logging.getLogger(__name__)


def load_registry(jsonl_path: Union[str, Path]) -> List[Dict[str, Any]]:
    """
    Load datatypes from registry JSONL file.
    
    Args:
        jsonl_path: Path to datatypes_latest.jsonl file
        
    Returns:
        List of datatype dictionaries
    """
    jsonl_path = Path(jsonl_path)
    if not jsonl_path.exists():
        raise FileNotFoundError(f"Registry file not found: {jsonl_path}")
    
    datatypes = []
    with open(jsonl_path, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                if 'orjson' in globals():
                    datatype = _json_loads(line)
                else:
                    datatype = json.loads(line)
                datatypes.append(datatype)
            except (json.JSONDecodeError, ValueError) as e:
                logger.warning(f"Failed to parse line {line_num} in {jsonl_path}: {e}")
                continue
    
    logger.info(f"Loaded {len(datatypes)} datatypes from {jsonl_path}")
    return datatypes


def filter_datatypes(
    datatypes: List[Dict[str, Any]],
    country: Optional[Union[str, List[str]]] = None,
    langs: Optional[Union[str, List[str]]] = None,
    categories: Optional[Union[str, List[str]]] = None
) -> List[Dict[str, Any]]:
    """
    Filter datatypes by country, language, and/or categories.
    
    Args:
        datatypes: List of datatype dictionaries
        country: Country code(s) to filter by (e.g., 'us', ['us', 'ca'])
        langs: Language code(s) to filter by (e.g., 'en', ['en', 'ru'])
        categories: Category ID(s) to filter by (e.g., 'pii', ['pii', 'finance'])
        
    Returns:
        Filtered list of datatype dictionaries
    """
    filtered = datatypes
    
    # Normalize filters to lists
    if country is not None:
        if isinstance(country, str):
            country = [country.lower()]
        else:
            country = [c.lower() for c in country]
    
    if langs is not None:
        if isinstance(langs, str):
            langs = [langs.lower()]
        else:
            langs = [l.lower() for l in langs]
    
    if categories is not None:
        if isinstance(categories, str):
            categories = [categories.lower()]
        else:
            categories = [c.lower() for c in categories]
    
    # Filter by country
    if country is not None:
        filtered = [
            dt for dt in filtered
            if _matches_filter(dt.get('country', []), country)
        ]
    
    # Filter by language
    if langs is not None:
        filtered = [
            dt for dt in filtered
            if _matches_filter(dt.get('langs', []), langs)
        ]
    
    # Filter by categories
    if categories is not None:
        filtered = [
            dt for dt in filtered
            if _matches_filter(dt.get('categories', []), categories)
        ]
    
    return filtered


def _matches_filter(
    field_value: Union[List[Dict[str, str]], List[str], None],
    filter_values: List[str]
) -> bool:
    """
    Check if a field value matches any of the filter values.
    
    Handles both dict format (with 'id' key) and string format.
    
    Args:
        field_value: Field value from datatype (can be list of dicts or strings)
        filter_values: List of filter values to match against
        
    Returns:
        True if any filter value matches
    """
    if not field_value:
        return False
    
    # Extract IDs if field_value contains dicts
    field_ids = []
    for item in field_value:
        if isinstance(item, dict):
            field_ids.append(item.get('id', '').lower())
        else:
            field_ids.append(str(item).lower())
    
    # Check if any filter value matches
    return any(fv.lower() in field_ids for fv in filter_values)


def get_datatype_text(datatype: Dict[str, Any]) -> str:
    """
    Convert a datatype dictionary to text representation for embedding.
    
    Args:
        datatype: Datatype dictionary
        
    Returns:
        Formatted text string
    """
    parts = []
    
    # ID
    if 'id' in datatype:
        parts.append(f"ID: {datatype['id']}")
    
    # Name
    if 'name' in datatype:
        parts.append(f"Name: {datatype['name']}")
    
    # Description
    if 'doc' in datatype:
        parts.append(f"Description: {datatype['doc']}")
    
    # Categories
    if 'categories' in datatype and datatype['categories']:
        cat_names = [_get_name_or_id(c) for c in datatype['categories']]
        parts.append(f"Categories: {', '.join(cat_names)}")
    
    # Countries
    if 'country' in datatype and datatype['country']:
        country_names = [_get_name_or_id(c) for c in datatype['country']]
        parts.append(f"Countries: {', '.join(country_names)}")
    
    # Languages
    if 'langs' in datatype and datatype['langs']:
        lang_names = [_get_name_or_id(l) for l in datatype['langs']]
        parts.append(f"Languages: {', '.join(lang_names)}")
    
    # Examples
    if 'examples' in datatype and datatype['examples']:
        example_values = []
        for ex in datatype['examples']:
            if isinstance(ex, dict):
                val = ex.get('value', '')
                desc = ex.get('description', '')
                if desc:
                    example_values.append(f"{val} ({desc})")
                else:
                    example_values.append(val)
            else:
                example_values.append(str(ex))
        if example_values:
            parts.append(f"Examples: {', '.join(example_values)}")
    
    # Regexp pattern
    if 'regexp' in datatype and datatype['regexp']:
        parts.append(f"Pattern: {datatype['regexp']}")
    
    # Classification
    if 'classification' in datatype and datatype['classification']:
        parts.append(f"Classification: {datatype['classification']}")
    
    return "\n".join(parts)


def _get_name_or_id(item: Union[Dict[str, str], str]) -> str:
    """Extract name or id from an item."""
    if isinstance(item, dict):
        return item.get('name', item.get('id', ''))
    return str(item)

