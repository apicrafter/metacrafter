"""Database primary and foreign keys in SQL scans (issue #14)."""

import json
import sqlite3

import pytest


@pytest.fixture
def database(tmp_path):
    path = tmp_path / "keys.db"
    connection = sqlite3.connect(path)
    connection.executescript("""
        create table parent (id integer primary key autoincrement, name text);
        create table country (code text primary key, title text);
        create table child (cid integer primary key, parent_id integer references parent(id),
                            country_code text references country(code));
        create table pair (a integer, b integer, primary key (a, b));
        insert into parent (name) values ('alpha'), ('beta'), ('gamma');
        insert into country values ('AM', 'Armenia'), ('GE', 'Georgia');
        insert into child (parent_id, country_code) values (1, 'AM'), (2, 'GE'), (3, 'AM');
        insert into pair values (1, 2), (2, 3);
    """)
    connection.commit()
    connection.close()
    return path


def scan(database, tmp_path):
    from metacrafter.core import CrafterCmd

    out = tmp_path / "out.json"
    CrafterCmd(quiet=True).scan_db(f"sqlite:///{database}", output_format="json", output=str(out))
    report = json.loads(out.read_text())
    return {
        source["table"].split(".")[-1]: {field["field"]: field for field in source["data"]}
        for source in report["sources"]
    }


def schema_matches(field):
    return [m for m in field["matches"] if m["ruletype"] == "schema"]


def test_autoincrement_primary_key_is_a_database_id(database, tmp_path):
    tables = scan(database, tmp_path)
    for table, column in (("parent", "id"), ("child", "cid")):
        field = tables[table][column]
        assert {"primary_key", "autoincrement"} <= set(field["tags"])
        assert field["database"] == {"primary_key": True, "autoincrement": True}
        match = schema_matches(field)[0]
        assert (match["dataclass"], match["confidence"], match["ruleid"]) == ("id", 100.0, "_db_autoincrement_key")
        assert match["evidence"]["source"] == "database schema"


def test_natural_and_composite_keys_are_not_database_ids(database, tmp_path):
    tables = scan(database, tmp_path)
    code = tables["country"]["code"]
    assert "primary_key" in code["tags"] and "autoincrement" not in code["tags"]
    assert schema_matches(code) == []
    for column in ("a", "b"):
        assert tables["pair"][column]["database"] == {"primary_key": True}
        assert schema_matches(tables["pair"][column]) == []


def test_foreign_keys_name_their_target(database, tmp_path):
    tables = scan(database, tmp_path)
    assert tables["child"]["parent_id"]["database"] == {"foreign_key": "main.parent.id"}
    assert tables["child"]["country_code"]["database"] == {"foreign_key": "main.country.code"}
    assert "foreign_key" in tables["child"]["parent_id"]["tags"]
    assert "database" not in tables["parent"]["name"]


def test_scan_data_without_schema_is_unchanged():
    from metacrafter.core import CrafterCmd

    report = CrafterCmd(quiet=True).scan_data([{"id": 1}, {"id": 2}], parse_dates=False)
    field = report["data"][0]
    assert "database" not in field and all(m["ruletype"] != "schema" for m in field["matches"])
