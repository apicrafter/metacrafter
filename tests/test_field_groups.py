"""Field groups by shared name prefix, suffix or language variant (issue #12)."""

from metacrafter.report.groups import field_groups, split_name


def by_kind(groups, kind):
    return {g["name"]: g for g in groups if g["kind"] == kind}


def test_split_name():
    assert split_name("placeURL") == ["place", "url"]
    assert split_name("HTTPStatus_code") == ["http", "status", "code"]
    assert split_name("bbox-x0") == ["bbox", "x0"]


def test_prefix_and_suffix_groups_from_the_issue_example():
    groups = field_groups("title text date place placeURL placeLocation placeType reviewScore avgScore".split())
    place = by_kind(groups, "prefix")["place"]
    assert place["fields"] == ["place", "placeLocation", "placeType", "placeURL"]
    assert place["parts"] == ["", "location", "type", "url"]
    assert by_kind(groups, "suffix")["score"]["fields"] == ["avgScore", "reviewScore"]


def test_flag_prefixes_and_short_prefixes_are_ignored():
    groups = field_groups(["is_approved", "is_published", "has_time", "has_elevation", "x_a", "x_b"])
    assert by_kind(groups, "prefix") == {}


def test_language_variants():
    groups = field_groups(["title", "title_en", "title_ru", "abstract_en", "user_id", "group_id"])
    language = by_kind(groups, "language")
    assert language["title"]["parts"] == ["", "en", "ru"]
    assert "abstract" not in language  # one variant only
    assert "en" not in by_kind(groups, "suffix")
    assert by_kind(groups, "suffix")["id"]["fields"] == ["group_id", "user_id"]


def test_groups_stay_within_parent_objects():
    groups = field_groups(["owner.first_name", "owner.last_name", "first_name"])
    name = by_kind(groups, "suffix")["name"]
    assert name["parent"] == "owner" and name["fields"] == ["owner.first_name", "owner.last_name"]


def test_scan_report_has_field_groups():
    from metacrafter.core import CrafterCmd

    rows = [{"place": "Paris", "placeType": "city", "placeURL": "https://example.com/p", "score": 1}]
    report = CrafterCmd(quiet=True).scan_data(rows, parse_dates=False)
    assert by_kind(report["field_groups"], "prefix")["place"]["fields"] == ["place", "placeType", "placeURL"]


def test_markdown_lists_field_groups():
    from metacrafter.report.markdown import render_markdown

    report = {"source": "t", "data": [{"field": "a", "matches": []}],
              "field_groups": [{"kind": "prefix", "name": "place", "parent": "", "fields": ["place", "placeURL"]}]}
    assert "- prefix `place`: `place`, `placeURL`" in render_markdown(report)
