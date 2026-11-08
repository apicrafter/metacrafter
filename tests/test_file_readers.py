# -*- coding: utf-8 -*-
"""Readers and field names of open data files (issues #96-#103)."""
import types

from metacrafter.core import CrafterCmd


def _scan(path, **kwargs):
    cmd = CrafterCmd(remote=None, debug=False, quiet=True)
    return cmd.scan_file(str(path), limit=50, dformat="full", output_format="json", **kwargs)


def _fields(report):
    rows = report.get("fields") or report["data"]
    return {f["field"].replace("\\.", "."): [m.get("datatype") or m["dataclass"] for m in f["matches"]] for f in rows}


# ---------------------------------------------------------------- #96 _id
def test_id_field_of_jsonl_and_csv_is_reported(tmp_path):
    jsonl = tmp_path / "ids.jsonl"
    jsonl.write_text('{"_id": "6167df39d9af277471403f52", "id": 1, "name": "a"}\n'
                     '{"_id": "5c542a2f34b6ed2f7e45289e", "id": 2, "name": "b"}\n')
    fields = _fields(_scan(jsonl))
    assert list(fields) == ["_id", "id", "name"]
    assert "mongodbid" in fields["_id"]
    csv_path = tmp_path / "ids.csv"
    csv_path.write_text("_id,name\n1,Alpha\n2,Beta\n")
    assert list(_fields(_scan(csv_path))) == ["_id", "name"]


def test_mongodb_scan_still_leaves_collection_ids_out(monkeypatch):
    from metacrafter import sources

    docs = [{"_id": object(), "email": f"user{i}@example.com", "tags": [{"_id": object(), "name": "x"}]}
            for i in range(5)]

    class Cursor(list):
        def batch_size(self, n):
            return self

        def limit(self, n):
            return self

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    class Client:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def __getitem__(self, name):
            return Database()

    class Database:
        def list_collection_names(self):
            return ["users"]

        def __getitem__(self, name):
            return types.SimpleNamespace(find=lambda: Cursor(docs))

    monkeypatch.setattr(sources, "require_extra", lambda *args: types.SimpleNamespace(MongoClient=Client))
    cmd = CrafterCmd(remote=None, debug=False, quiet=True)
    reports, errors = sources.scan_mongo(cmd, "localhost", 27017, "db", None, None, 10, {})
    assert not errors
    assert [f["field"] for f in reports["db.users"]["data"]] == ["email", "tags.name"]


# ---------------------------------------------------------------- #103 field names
def test_field_names_are_stripped(tmp_path):
    path = tmp_path / "spaces.csv"
    path.write_text("stop_id,stop_name       ,wheelchair_boarding   ,stop  desc\n1,A,0,x\n2,B,1,y\n")
    report = _scan(path)
    assert list(_fields(report)) == ["stop_id", "stop_name", "wheelchair_boarding", "stop  desc"]
    entries = {f["field"]: f for f in report["data"]}
    assert entries["wheelchair_boarding"]["name_in_source"] == "wheelchair_boarding   "
    assert "name_in_source" not in entries["stop_id"]


def test_stripping_keeps_names_apart_and_nested_paths():
    from metacrafter.cmd.helpers import strip_key_whitespace

    items = [{"a ": 1, "a": 2, " ": 3, "props": {" NAME ": "x"}, "list": [{"k ": 1}]}]
    renamed = strip_key_whitespace(items)
    assert items == [{"a ": 1, "a": 2, " ": 3, "props": {"NAME": "x"}, "list": [{"k": 1}]}]
    assert renamed == {"props.NAME": "props. NAME ", "list.k": "list.k "}
