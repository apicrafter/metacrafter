#!/usr/bin/env python3
"""Write the report v3 JSON Schema to docs/static/schema/report-v3.schema.json."""
import json
from pathlib import Path

from metacrafter.report.models import report_json_schema

target = Path(__file__).resolve().parent.parent / "docs" / "static" / "schema" / "report-v3.schema.json"
target.parent.mkdir(parents=True, exist_ok=True)
target.write_text(json.dumps(report_json_schema(), indent=2, sort_keys=True) + "\n", encoding="utf-8")
print(target)
