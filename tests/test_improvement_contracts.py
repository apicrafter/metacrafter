"""Regression scenarios from the repository review and approved OpenSpecs."""

import io
import json
import math
import sqlite3
from pathlib import Path
from unittest.mock import Mock

import pytest
import yaml
from typer.testing import CliRunner

from metacrafter import CrafterCmd
from metacrafter.classify.processor import RulesProcessor, _safe_eval_pyparsing_rule
from metacrafter.classify.utils import dict_to_columns, _seek_xml_lists
from metacrafter.config import ConfigLoader, DEFAULT_RULEPATH
from metacrafter.report import redact_samples
from metacrafter.product_tools import (
    check_policy,
    dbt_plan,
    diff_reports,
    draft_rule,
    evaluate_fixture,
    lint_rules,
    pii_inventory,
    test_rule_examples as run_rule_examples,
)
from metacrafter.reports import aggregate_reports, normalize_report, report_sources
from metacrafter.sampling import sample_records
from metacrafter.server.api import MetacrafterApp


@pytest.fixture
def service(monkeypatch):
    monkeypatch.setattr(ConfigLoader, "load_config", staticmethod(lambda: None))
    return CrafterCmd(quiet=True)


@pytest.fixture
def rule_file(tmp_path):
    path = tmp_path / "rules.yaml"
    path.write_text(
        yaml.safe_dump(
            {
                "name": "test",
                "description": "test",
                "context": "common",
                "lang": "common",
                "rules": {
                    "national_id": {
                        "key": "national_id",
                        "type": "data",
                        "match": "ppr",
                        "rule": "Word(nums, exact=3)",
                        "minlen": 3,
                        "maxlen": 3,
                        "is_pii": True,
                    },
                    "literal": {
                        "key": "open_label",
                        "type": "data",
                        "match": "ppr",
                        "rule": "Literal('open')",
                        "minlen": 4,
                        "maxlen": 4,
                    },
                },
            }
        )
    )
    return path


def test_named_statistics_and_every_valid_field(service):
    report = service.scan_data(
        [{"x": 1, "1": "01", "amount": 1}, {"x": 2, "1": "02", "amount": 2}],
        parse_dates=False,
    )
    assert set(report["stats"]) == {"x", "1", "amount"}
    stats = report["stats"]["amount"]
    assert (stats["minval"], stats["maxval"], stats["dictvalues"]) == (1, 2, None)
    assert {field["field"] for field in report["data"]} == set(report["stats"])


def test_nested_array_missing_and_collision_semantics(service):
    items = [
        {"a.b": "literal", "a": {"b": "nested"}, "array": [0, False, "z"]},
        {"late": "new"},
    ]
    columns = dict_to_columns(items)
    assert columns["a\\.b"] == ["literal", None]
    assert columns["a.b"] == ["nested", None]
    assert columns["array"] == [0, False, "z", None]
    report = service.scan_data(items, parse_dates=False)
    assert set(report["stats"]) == set(columns)
    with pytest.raises(ValueError, match="Unknown selected"):
        service.scan_data(items, fields=["missing"])


@pytest.mark.parametrize(
    "limit,confidence",
    [
        (0, 5),
        (-1, 5),
        (True, 5),
        (1, float("nan")),
        (1, float("inf")),
        (1, 101),
        (1, -1),
    ],
)
def test_invalid_options_fail_before_scan(service, limit, confidence):
    with pytest.raises(ValueError):
        service.scan_data([], limit=limit, confidence=confidence)


def test_inclusive_threshold_boolean_pii_and_empty_values(rule_file):
    processor = RulesProcessor()
    processor.import_rules(str(rule_file))
    report = processor.match_dict(
        [{"x": "123"}, {"x": "-"}], confidence=100, parse_dates=False
    )
    assert report.results[0].matches[0].dataclass == "national_id"
    assert report.results[0].matches[0].is_pii is True
    assert (
        processor.match_dict(
            [{"x": None}], confidence=0, except_empty=False, parse_dates=False
        )
        .results[0]
        .matches
        == []
    )


def test_required_validator_failure_does_not_claim_id(rule_file):
    document = yaml.safe_load(rule_file.read_text())
    document["rules"]["national_id"]["validator"] = "missing_package.validate"
    invalid = rule_file.with_name("invalid.yaml")
    invalid.write_text(yaml.safe_dump(document))
    processor = RulesProcessor()
    processor.import_rules(str(invalid))
    assert not any(rule["id"] == "national_id" for rule in processor.data_rules)
    assert any(
        d["error"] == "Required validator unavailable" for d in processor.diagnostics
    )
    processor.import_rules(str(rule_file))
    assert any(rule["id"] == "national_id" for rule in processor.data_rules)


@pytest.mark.parametrize(
    "expression",
    [
        "__import__('os')",
        "Literal('x').__class__",
        "[x for x in nums]",
        "Word(nums)*1000000",
        "42",
    ],
)
def test_unsafe_patterns_rejected(expression):
    with pytest.raises((ValueError, SyntaxError)):
        _safe_eval_pyparsing_rule(expression)


def test_literal_open_and_builtin_defaults_are_valid():
    assert (
        _safe_eval_pyparsing_rule("Literal('open')").parse_string("open")[0] == "open"
    )
    processor, diagnostics = lint_rules(DEFAULT_RULEPATH[0])
    # The default pack (metacrafter-rules) must compile; invalid upstream rule
    # metadata is reported as a diagnostic instead of loading an unchecked rule.
    assert not [d for d in diagnostics if d["error"] == "Pattern compilation failed"]
    assert all({"ruleid", "source", "error"} <= set(d) for d in diagnostics)
    assert len(processor.field_rules) + len(processor.data_rules) >= 1000


def test_sampling_is_bounded_and_reproducible():
    seen = []

    def records():
        for number in range(100):
            seen.append(number)
            yield {"x": number}

    sample, provenance = sample_records(records(), 3)
    assert len(seen) == len(sample) == 3
    assert provenance["population_count_known"] is False
    first, metadata = sample_records(records(), 3, "reservoir", 7)
    second, _ = sample_records(records(), 3, "reservoir", 7)
    assert first == second and len(first) == 3 and metadata["observed_count"] == 100


def test_file_roundtrip_and_bulk_machine_output(service, tmp_path, capsys):
    source = tmp_path / "one.jsonl"
    source.write_text("\n".join(json.dumps({"x": value}) for value in range(8)))
    out = io.StringIO()
    report = service.scan_file(
        str(source), limit=2, output=out, output_format="json", parse_dates=False
    )
    saved = json.loads(out.getvalue())
    # Serialized output omits raw values unless --include-samples (privacy default).
    assert saved["data"] == saved["fields"] == redact_samples(report)["data"]
    assert saved["samples_included"] is False
    assert saved["sampling"]["retained_count"] == 2
    (tmp_path / "two.jsonl").write_text('{"email":"a@b.test"}\n')
    output = io.StringIO()
    aggregate = service.scan_bulk(
        str(tmp_path), output=output, output_format="json", parse_dates=False
    )
    assert json.loads(output.getvalue()) == redact_samples(aggregate)
    assert len(aggregate["sources"]) == 2
    assert capsys.readouterr().out == ""


def test_sql_quoted_identifiers_and_schema_identity(service, tmp_path):
    path = tmp_path / "quoted.db"
    with sqlite3.connect(path) as connection:
        connection.execute('CREATE TABLE "order detail" ("x" TEXT)')
        connection.execute('INSERT INTO "order detail" VALUES (?)', ("123",))
    output = io.StringIO()
    report = service.scan_db(
        "sqlite:///" + str(path),
        schema="main",
        output=output,
        output_format="json",
        parse_dates=False,
    )
    assert report["complete"] and report["sources"][0]["source"] == "main.order detail"
    assert json.loads(output.getvalue()) == redact_samples(report)
    with pytest.raises(ValueError, match="Schema"):
        service.scan_db(
            "sqlite:///" + str(path), schema="missing", output=io.StringIO()
        )


def test_api_parity_initialization_auth_and_bounds(service):
    factory = MetacrafterApp(service.processor, service.dparser, auth_token="token")
    client = factory.app.test_client()
    payload = [{"x": 1, "email": "a@b.test"}]
    assert client.get("/health").status_code == 200
    assert client.post("/api/v1/scan_data", json=payload).status_code == 401
    headers = {"Authorization": "Bearer token"}
    response = client.post(
        "/api/v1/scan_data?confidence=100&fields=email&parse_dates=false",
        json=payload,
        headers=headers,
    )
    assert response.status_code == 200
    expected = service.scan_data(
        payload, confidence=100, fields="email", parse_dates=False
    )
    assert response.get_json()["data"] == expected["data"]
    initialized = factory.service
    assert client.get("/ready").status_code == 200
    assert factory.service is initialized
    for payload in (3, {"x": 1}, [3]):
        assert (
            client.post("/api/v1/scan_data", json=payload, headers=headers).status_code
            == 400
        )
    assert (
        client.post("/api/v1/scan_data?limit=0", json=[], headers=headers).status_code
        == 400
    )
    factory.app.config["MAX_RECORDS"] = 1
    assert (
        client.post("/api/v1/scan_data", json=[{}, {}], headers=headers).status_code
        == 400
    )


def synthetic_report(pii=True, confidence=100):
    return {
        "table": "main.users",
        "fields": [
            {
                "field": "id",
                "ftype": "str",
                "stats": {"minlen": 3},
                "matches": [
                    {
                        "dataclass": "national_id",
                        "confidence": confidence,
                        "is_pii": pii,
                        "ruleid": "custom",
                    }
                ],
            }
        ],
    }


def test_legacy_reports_diff_inventory_and_policy():
    old, new = synthetic_report(False), synthetic_report(True)
    new["fields"][0]["stats"]["minlen"] = 4
    diff = diff_reports(old, new)
    assert {c["kind"] for c in diff["changes"]} == {"pii", "statistic"}
    inventory = pii_inventory(
        aggregate_reports([normalize_report(new)], [{"source": "failed"}])
    )
    assert inventory["pii_fields"][0]["datatype"] == "national_id"
    assert inventory["complete"] is False
    policy = check_policy(
        new, {"prohibit_pii": True, "required_labels": {"missing": ["email"]}}
    )
    assert {v["code"] for v in policy["violations"]} == {"prohibited", "required_label"}
    assert not check_policy(
        {"sources": [], "complete": False}, {"require_complete": True}
    )["passed"]
    with pytest.raises(ValueError):
        normalize_report({"report_version": 999, "fields": []})


def test_rule_tools_drafts_and_evaluation(service, rule_file, tmp_path):
    result = run_rule_examples(
        rule_file,
        [{"ruleid": "national_id", "positive": ["001"], "negative": ["1", "abc"]}],
    )
    assert result == {"passed": True, "tested": 3, "failures": []}
    draft = draft_rule("code", ["001", "002"])
    assert draft["review_required"] and draft["provenance"]["leading_zero"]
    draft_path = tmp_path / "draft.yaml"
    draft_path.write_text(yaml.safe_dump(draft))
    processor, _ = lint_rules(draft_path)
    assert not processor.field_rules
    result = evaluate_fixture(
        service, [{"items": [{"email": "a@b.test"}], "labels": {"email": ["email"]}}]
    )
    assert result["datatypes"]["email"]["true_positive"] == 1
    assert result["rules_version"]


def test_dbt_plan_preserves_metadata():
    manifest = {
        "nodes": {
            "model.test.users": {
                "resource_type": "model",
                "schema": "main",
                "name": "users",
                "columns": {
                    "id": {
                        "data_type": "text",
                        "meta": {"owner": "team", "metacrafter": {"reviewed": True}},
                    }
                },
            }
        }
    }
    plan = dbt_plan(manifest, synthetic_report())
    assert plan["dry_run"] and plan["updates"][0]["meta"]["owner"] == "team"
    assert plan["updates"][0]["meta"]["metacrafter"]["reviewed"] is True


def test_product_cli_exit_codes_and_json(tmp_path):
    from metacrafter.cli import commands
    from metacrafter.core import app

    report = tmp_path / "report.json"
    report.write_text(json.dumps(synthetic_report()))
    policy = tmp_path / "policy.yaml"
    policy.write_text("prohibit_pii: true\n")
    runner = CliRunner()
    result = runner.invoke(app, ["report", "pii", str(report)])
    assert result.exit_code == 0, result.output
    assert json.loads(result.output)["pii_fields"][0]["datatype"] == "national_id"
    result = runner.invoke(app, ["check", str(report), "--policy", str(policy)])
    assert result.exit_code == 1 and json.loads(result.output)["passed"] is False


def test_xml_helpers_do_not_retain_prior_state():
    assert set(_seek_xml_lists({"first": [1]})) == {"first"}
    assert set(_seek_xml_lists({"second": [2]})) == {"second"}


def test_atlas_failed_write_dry_run_and_custom_pii(monkeypatch):
    from metacrafter.integrations.atlas import AtlasExporter

    exporter = AtlasExporter("https://atlas.invalid")
    monkeypatch.setattr(exporter, "_resolve_column_entity", lambda *args: "guid")
    monkeypatch.setattr(exporter, "_get_entity_classifications", lambda *args: [])
    response = Mock(status_code=500, text="server failure")
    monkeypatch.setattr(
        "metacrafter.integrations.atlas.requests.post", Mock(return_value=response)
    )
    stats = exporter.export_scan_results(
        "main.users",
        synthetic_report(),
        add_datatype_classifications=False,
        add_attributes=False,
    )
    assert stats["attempted"] == stats["failed"] == 1
    assert stats["succeeded"] == stats["classifications_added"] == 0
    assert stats["errors"]
    stats = exporter.export_scan_results(
        "main.users",
        synthetic_report(),
        add_datatype_classifications=False,
        add_attributes=False,
        dry_run=True,
    )
    assert stats["attempted"] == 0 and len(stats["planned"]) == 1
    stats = exporter.export_scan_results(
        "main.users", synthetic_report(confidence=10), min_confidence=50
    )
    assert stats["attempted"] == 0


def test_secret_redaction_and_sample_policy():
    from metacrafter.privacy import redact, disclose_samples

    assert redact({"api_key": "secret", "url": "postgres://u:password@host/db"}) == {
        "api_key": "[REDACTED]",
        "url": "postgres://u:[REDACTED]@host/db",
    }
    assert disclose_samples(["123", False, 0]) == []
    assert disclose_samples(["a@b.test", "123"], "redacted") == ["x@x.xxxx", "000"]


def test_llm_selected_nested_fields_pii_and_threshold(service):
    classifier = Mock()
    classifier.classify_field.return_value = {
        "datatype_id": "national_id",
        "confidence": 0.9,
        "is_pii": True,
        "reason": "registered type",
    }
    service.llm_classifier = classifier
    report = service.scan_data(
        [{"nested": {"id": "001"}, "ignored": "private"}],
        fields=["nested.id"],
        classification_mode="llm",
        confidence=80,
    )
    assert [f["field"] for f in report["data"]] == ["nested.id"]
    assert report["data"][0]["matches"][0]["is_pii"]
    assert classifier.classify_field.call_args.kwargs["sample_values"] == []
    report = service.scan_data(
        [{"nested": {"id": "001"}}], classification_mode="llm", confidence=100
    )
    assert report["data"][0]["matches"] == []
    classifier.classify_field.return_value["confidence"] = float("nan")
    with pytest.raises(RuntimeError):
        service.scan_data([{"x": "001"}], classification_mode="llm")


def test_local_embeddings_manifest_and_atomic_rebuild(tmp_path, monkeypatch):
    pytest.importorskip("chromadb")
    pytest.importorskip("openai")
    from metacrafter.classify.llm.classifier import LLMClassifier

    registry = tmp_path / "registry.jsonl"
    registry.write_text(
        json.dumps(
            {
                "id": "national_id",
                "name": "National ID",
                "is_pii": True,
                "langs": ["ru"],
            }
        )
        + "\n"
    )
    client = Mock()
    client.embeddings.create.side_effect = lambda **kwargs: Mock(
        data=[
            Mock(embedding=[0.1, 0.2])
            for _ in range(
                len(kwargs["input"]) if isinstance(kwargs["input"], list) else 1
            )
        ]
    )
    monkeypatch.setattr(
        "metacrafter.classify.llm.embedder.OpenAI", Mock(return_value=client)
    )
    provider = Mock()
    provider.classify.return_value = {
        "datatype_id": "national_id",
        "confidence": 0.9,
        "reason": "example",
    }
    monkeypatch.setattr(
        "metacrafter.classify.llm.classifier.get_provider",
        lambda *args, **kwargs: provider,
    )
    classifier = LLMClassifier(registry, tmp_path / "index", llm_provider="ollama")
    result = classifier.classify_field(
        "id", sample_values=["secret"], langs=["en", "ru"]
    )
    assert result["is_pii"] and classifier.embedder.provider == "ollama"
    assert "secret" not in provider.classify.call_args.args[0]
    count, collection = (
        classifier.vector_store.count(),
        classifier.vector_store.collection_name,
    )
    client.embeddings.create.side_effect = RuntimeError("embedding failure")
    with pytest.raises(RuntimeError):
        classifier.rebuild_index()
    assert (
        classifier.vector_store.count() == count
        and classifier.vector_store.collection_name == collection
    )
    provider.classify.return_value["datatype_id"] = "unknown"
    client.embeddings.create.side_effect = lambda **kwargs: Mock(
        data=[Mock(embedding=[0.1, 0.2])]
    )
    with pytest.raises(ValueError, match="Unknown"):
        classifier.classify_field("id")
    registry.write_text(
        registry.read_text() + json.dumps({"id": "new", "name": "New"}) + "\n"
    )
    with pytest.raises(ValueError, match="Incompatible"):
        LLMClassifier(registry, tmp_path / "index", llm_provider="ollama")
