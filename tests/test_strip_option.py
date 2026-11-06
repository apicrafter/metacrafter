"""--strip: whitespace around values is removed before analysis (issue #13)."""

from typer.testing import CliRunner

from metacrafter.classify.utils import strip_values

ROWS = [
    {"contact": "  alice@example.com  ", "pad": "   ", "nested": {"code": " 42 "}, "tags": [" a ", 1]},
    {"contact": "\tbob@example.org\n", "pad": " ", "nested": {"code": "7 "}, "tags": ["b"]},
]


def test_strip_values_walks_nested_values():
    assert strip_values(ROWS[0]) == {"contact": "alice@example.com", "pad": "", "nested": {"code": "42"}, "tags": ["a", 1]}
    assert ROWS[0]["contact"] == "  alice@example.com  "  # input is not modified


def fields(report):
    return {f["field"]: f for f in report["data"]}


def test_scan_without_strip_keeps_whitespace():
    from metacrafter.core import CrafterCmd

    report = fields(CrafterCmd(quiet=True).scan_data(ROWS, parse_dates=False, confidence=50))
    assert report["contact"]["stats"]["minlen"] > len("bob@example.org")


def test_scan_with_strip_parameter():
    from metacrafter.core import CrafterCmd

    report = fields(CrafterCmd(quiet=True).scan_data(ROWS, parse_dates=False, confidence=50, strip=True))
    assert report["contact"]["stats"]["maxlen"] == len("alice@example.com")
    assert report["pad"]["stats"]["maxlen"] == 0
    assert report["nested.code"]["stats"]["ftype"] in ("int", "numstr")


def test_strip_set_on_crafter_cmd_and_overridden_per_scan():
    from metacrafter.core import CrafterCmd

    cmd = CrafterCmd(quiet=True, strip=True)
    assert fields(cmd.scan_data(ROWS, parse_dates=False))["contact"]["stats"]["maxlen"] == len("alice@example.com")
    assert fields(cmd.scan_data(ROWS, parse_dates=False, strip=False))["contact"]["stats"]["maxlen"] > len("alice@example.com")


def test_cli_strip_flag(tmp_path):
    import json

    from metacrafter.cli import commands  # noqa: F401
    from metacrafter.core import app

    data = tmp_path / "padded.csv"
    data.write_text("name,code\n  Alice  , 42 \n Bob ,7 \n", encoding="utf-8")
    out = tmp_path / "out.json"
    result = CliRunner().invoke(app, ["scan", "file", str(data), "--strip", "--output-format", "json", "-o", str(out), "--quiet"])
    assert result.exit_code == 0, result.output
    report = {f["field"]: f for f in json.loads(out.read_text())["data"]}
    assert report["name"]["stats"]["maxlen"] == len("Alice")
