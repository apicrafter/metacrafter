"""MCP tools: scope checks, limits, labels without values; server registration."""

import asyncio
import json
import sqlite3

import pytest

from metacrafter.config import McpSettings
from metacrafter.interfaces.mcp_server import AccessError, MetacrafterTools


@pytest.fixture(scope="module")
def service():
    from metacrafter.core import CrafterCmd

    return CrafterCmd(quiet=True)


def tools_for(service, **settings):
    return MetacrafterTools(service=service, settings=McpSettings(**settings))


def test_classify_columns_labels_without_echoing_values(service):
    tools = tools_for(service)
    result = tools.classify_columns([
        {"name": "contact_email", "values": ["alice@example.com", "bob@example.org"]},
    ])
    field = result["fields"][0]
    assert field["field"] == "contact_email" and field["is_pii"]
    assert field["matches"][0]["datatype"] == "email"
    assert field["matches"][0]["url"].endswith("/datatype/email")
    assert "alice@example.com" not in json.dumps(result)


def test_classify_columns_limits(service):
    tools = tools_for(service, max_columns=1, max_values=2)
    with pytest.raises(AccessError, match="maximum is 1"):
        tools.classify_columns([{"name": "a", "values": []}, {"name": "b", "values": []}])
    with pytest.raises(AccessError, match="maximum is 2"):
        tools.classify_columns([{"name": "a", "values": [1, 2, 3]}])


def test_scan_file_requires_configured_roots(service, tmp_path):
    data = tmp_path / "data.jsonl"
    data.write_text('{"contact_email": "a@example.com"}\n')
    with pytest.raises(AccessError, match="disabled"):
        tools_for(service).scan_file(str(data))
    tools = tools_for(service, file_roots=[str(tmp_path / "allowed")])
    (tmp_path / "allowed").mkdir()
    with pytest.raises(AccessError, match="outside the allowed roots"):
        tools.scan_file(str(data))


def test_scan_file_rejects_symlink_escape(service, tmp_path):
    allowed = tmp_path / "allowed"
    allowed.mkdir()
    secret = tmp_path / "secret.jsonl"
    secret.write_text('{"x": 1}\n')
    (allowed / "link.jsonl").symlink_to(secret)
    tools = tools_for(service, file_roots=[str(allowed)])
    with pytest.raises(AccessError, match="outside"):
        tools.scan_file(str(allowed / "link.jsonl"))


def test_scan_file_inside_root(service, tmp_path):
    (tmp_path / "data.jsonl").write_text('{"contact_email": "a@example.com"}\n{"contact_email": "b@example.org"}\n')
    tools = tools_for(service, file_roots=[str(tmp_path)])
    result = tools.scan_file(str(tmp_path / "data.jsonl"), limit=10)
    assert result["fields"][0]["matches"][0]["datatype"] == "email"
    with pytest.raises(AccessError, match="maximum"):
        tools.scan_file(str(tmp_path / "data.jsonl"), limit=10**9)


def test_scan_sql_only_named_connections(service, tmp_path):
    db = tmp_path / "t.db"
    with sqlite3.connect(db) as conn:
        conn.execute("CREATE TABLE people (contact_email TEXT)")
        conn.execute("CREATE TABLE other (x TEXT)")
        conn.execute("INSERT INTO people VALUES ('a@example.com')")
    url = "sqlite:///" + str(db)
    tools = tools_for(service, connections={"local": url})
    with pytest.raises(AccessError, match="Configured connections: local"):
        tools.scan_sql(url)
    result = tools.scan_sql("local", tables=["people"])
    assert [s["source"] for s in result["sources"]] == ["main.people"]
    assert url not in json.dumps(result)


def test_explain_match_and_list_rules(service):
    tools = tools_for(service)
    explained = tools.explain_match("email", values=["a@example.com", "not an email"], field_name="contact")
    assert explained["datatype"] == "email"
    assert explained["rules"]
    assert "a@example.com" not in json.dumps(explained)
    alias = tools.explain_match("ssn")
    assert alias["datatype"] == "usssn" and alias["known_to_registry"]
    listing = tools.list_rules(datatype="email")
    assert listing["total"] >= 1 and all(r["datatype"] == "email" for r in listing["rules"])


def test_server_registers_tools(service):
    pytest.importorskip("mcp")
    from metacrafter.interfaces.mcp_server import create_server

    server = create_server(tools_for(service))
    names = {tool.name for tool in asyncio.run(server.list_tools())}
    assert names == {"classify_columns", "scan_file", "scan_sql", "explain_match", "list_rules"}
