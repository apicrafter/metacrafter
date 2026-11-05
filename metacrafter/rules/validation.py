"""Validate rule files against the published rule-format JSON Schema."""
from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Tuple

RULE_SCHEMA_PATH = Path(__file__).with_name("schema") / "rule-v2.schema.json"


@lru_cache(maxsize=1)
def load_rule_schema() -> Dict[str, Any]:
    return json.loads(RULE_SCHEMA_PATH.read_text("utf-8"))


@lru_cache(maxsize=1)
def _validators():
    import jsonschema

    schema = load_rule_schema()
    file_schema = {k: v for k, v in schema.items() if k != "$defs"}
    file_schema = dict(file_schema)
    file_schema["properties"] = dict(schema["properties"])
    file_schema["properties"]["rules"] = {"type": "object", "minProperties": 1}
    rule_schema = dict(schema["$defs"]["rule"])
    cls = jsonschema.validators.validator_for(schema)
    return cls(file_schema), cls(rule_schema)


def _message(error) -> str:
    location = "/".join(str(p) for p in error.absolute_path)
    return f"{location}: {error.message}" if location else error.message


def validate_rule_file(document: Any) -> Tuple[List[str], Dict[str, List[str]]]:
    """Validate a parsed rule file.

    Returns ``(file_errors, rule_errors)``: errors in the file envelope, and errors per
    rule id. A file with envelope errors cannot be loaded; a rule with errors is skipped.
    """
    file_validator, rule_validator = _validators()
    file_errors = [_message(e) for e in file_validator.iter_errors(document)]
    rule_errors: Dict[str, List[str]] = {}
    rules = document.get("rules") if isinstance(document, dict) else None
    if isinstance(rules, dict):
        for rule_id, rule in rules.items():
            errors = [_message(e) for e in rule_validator.iter_errors(rule)]
            if errors:
                rule_errors[str(rule_id)] = errors
    return file_errors, rule_errors
