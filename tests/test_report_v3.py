"""Report version 3: schema, legacy and Markdown output, sample redaction, exports."""

import io
import json
import sqlite3
from pathlib import Path

import pytest
from typer.testing import CliRunner

from metacrafter.core import CrafterCmd
from metacrafter.integrations.crosswalk import export_labels
from metacrafter.integrations.frictionless import export_frictionless
from metacrafter.report import redact_samples, render_markdown, report_json_schema, to_legacy, validate_report

ROWS = [
    {"contact_email": "a@example.com", "ssn": "123-45-6789", "kind": "x"},
    {"contact_email": "b@example.org", "ssn": "987-65-4321", "kind": "y"},
]


@pytest.fixture(scope="module")
def cmd():
    return CrafterCmd(quiet=True)


@pytest.fixture(scope="module")
def report(cmd):
    return cmd.scan_data(ROWS, parse_dates=False)


def test_sdk_report_is_version_3_and_validates(report):
    assert report["report_version"] == 3
    model = validate_report(json.loads(json.dumps(report, default=str)))
    assert model.tool.name == "metacrafter"
    email = next(f for f in report["data"] if f["field"] == "contact_email")
    assert email["pii"] == {"is_pii": True, "datatypes": ["email"]}


def test_committed_schema_is_current():
    committed = json.loads((Path(__file__).parent.parent / "docs/static/schema/report-v3.schema.json").read_text())
    assert committed == json.loads(json.dumps(report_json_schema(), sort_keys=True))


def test_file_json_output_validates_and_hides_samples(cmd, tmp_path):
    path = tmp_path / "people.jsonl"
    path.write_text("\n".join(json.dumps(r) for r in ROWS))
    out = io.StringIO()
    cmd.scan_file(str(path), output=out, output_format="json", parse_dates=False)
    saved = json.loads(out.getvalue())
    validate_report(saved)
    assert saved["samples_included"] is False
    for stats in saved["stats"].values():
        assert stats["dictvalues"] is None


def test_include_samples_keeps_values(tmp_path):
    cmd = CrafterCmd(quiet=True, include_samples=True)
    path = tmp_path / "people.jsonl"
    path.write_text("\n".join(json.dumps(r) for r in ROWS))
    out = io.StringIO()
    cmd.scan_file(str(path), output=out, output_format="json", parse_dates=False)
    assert "samples_included" not in json.loads(out.getvalue())


def test_sql_aggregate_validates(cmd, tmp_path):
    db = tmp_path / "t.db"
    with sqlite3.connect(db) as conn:
        conn.execute("CREATE TABLE people (contact_email TEXT)")
        conn.executemany("INSERT INTO people VALUES (?)", [("a@example.com",), ("b@example.org",)])
    out = io.StringIO()
    cmd.scan_db("sqlite:///" + str(db), output=out, output_format="json", parse_dates=False)
    saved = json.loads(out.getvalue())
    assert saved["report_version"] == 3
    validate_report(saved)


def test_legacy_format_keeps_pre_1_0_keys(report):
    legacy = to_legacy(report)
    assert set(legacy) == {"results", "data", "stats", "stats_table"}
    match = legacy["data"][0]["matches"][0]
    assert set(match) == {"ruleid", "dataclass", "confidence", "ruletype", "format", "classurl", "is_pii"}
    assert "datatype_url" in legacy["data"][0]


def test_markdown_has_links_and_no_values(report):
    text = render_markdown(report)
    assert "| Field | Datatype |" in text
    assert "/datatype/email" in text
    assert "a@example.com" not in text and "123-45-6789" not in text
    assert "PII fields" in text


def test_redact_samples_does_not_mutate(report):
    redacted = redact_samples(report)
    assert redacted is not report
    assert redacted["samples_included"] is False
    assert "samples_included" not in report


def test_export_labels_presidio(report):
    result = export_labels(report, "presidio")
    labels = {row["field"]: row["label"] for row in result["labels"]}
    assert labels.get("contact_email") == "EMAIL_ADDRESS"
    assert all("label" not in row for row in result["unmapped"])
    with pytest.raises(ValueError):
        export_labels(report, "nope")


def test_export_frictionless(report):
    package = export_frictionless(report)
    fields = {f["name"]: f for f in package["resources"][0]["schema"]["fields"]}
    email = fields["contact_email"]
    assert email["metacrafter:datatype"] == "email"
    assert email["metacrafter:url"].endswith("/datatype/email")
    assert email["metacrafter:pii"] is True


def test_cli_md_and_labels(tmp_path):
    from metacrafter.cli import commands  # noqa: F401
    from metacrafter.core import app

    path = tmp_path / "people.jsonl"
    path.write_text("\n".join(json.dumps(r) for r in ROWS))
    runner = CliRunner()
    result = runner.invoke(app, ["scan", "file", str(path), "--output-format", "md", "--no-dates"])
    assert result.exit_code == 0, result.output
    assert "# Metacrafter scan report" in result.output
    report_path = tmp_path / "report.json"
    result = runner.invoke(app, ["scan", "file", str(path), "--output-format", "json", "-o", str(report_path), "--no-dates"])
    assert result.exit_code == 0, result.output
    result = runner.invoke(app, ["export", "labels", str(report_path), "--system", "presidio"])
    assert result.exit_code == 0, result.output
    assert "EMAIL_ADDRESS" in result.output
