"""LLM verification of rule value matches (--llm-verify, issue #40)."""

import pytest

from metacrafter.classify.llm.verifier import LLMVerifier, verification_schema

GUIDS = ["3f2504e0-4f89-11d3-9a0c-0305e82c3301", "6fa459ea-ee8a-3ca4-894e-db77e160355e"]
ROWS = [
    {"package_id": GUIDS[0], "contact": "alice@example.com"},
    {"package_id": GUIDS[1], "contact": "bob@example.org"},
]


class FakeProvider:
    is_local = False
    model = "fake"

    def __init__(self, decide):
        self.decide = decide
        self.prompts, self.schemas = [], []

    def complete_json(self, prompt, schema=None, temperature=0.0, max_tokens=2000):
        self.prompts.append(prompt)
        self.schemas.append(schema)
        return self.decide(schema)


def keep_all_but(rejected):
    def decide(schema):
        return {field: [i for i in spec["items"]["enum"] if i not in rejected] for field, spec in schema["properties"].items()}
    return decide


def cmd_with(decide, policy="names-only", **kwargs):
    from metacrafter.core import CrafterCmd

    cmd = CrafterCmd(quiet=True, sample_policy=policy, **kwargs)
    cmd.llm_verifier = LLMVerifier(cmd.registry, FakeProvider(decide), sample_policy=policy, max_requests=cmd.llm_max_requests)
    return cmd


def by_field(report):
    return {f["field"]: f for f in report["data"]}


def test_rejected_value_matches_are_removed():
    cmd = cmd_with(keep_all_but({"uuid"}))
    report = cmd.scan_data(ROWS, parse_dates=False, explain=True)
    fields = by_field(report)
    assert all(m["dataclass"] != "uuid" for m in fields["package_id"]["matches"])
    assert any(r["reason"] == "Rejected by the LLM verifier" for r in fields["package_id"]["rejected_candidates"])
    email = [m for m in fields["contact"]["matches"] if m["datatype"] == "email" and m["ruletype"] == "data"]
    assert email and email[0]["evidence"]["llm_verified"] is True
    assert report["llm_verifier"]["removed"] >= 1 and report["llm_verifier"]["columns"] == 2
    assert len(cmd.llm_verifier.provider.prompts) == 1  # one request per table


def test_schema_allows_only_candidates():
    cmd = cmd_with(keep_all_but(set()))
    cmd.scan_data(ROWS, parse_dates=False)
    schema = cmd.llm_verifier.provider.schemas[0]
    assert set(schema["required"]) == {"package_id", "contact"}
    assert "uuid" in schema["properties"]["package_id"]["items"]["enum"]
    assert verification_schema({"a": ["x"]})["additionalProperties"] is False


def test_field_name_matches_are_not_sent_or_removed():
    cmd = cmd_with(lambda schema: {field: [] for field in schema["properties"]})
    report = cmd.scan_data([{"email": "a@example.com"}, {"email": "b@example.com"}], parse_dates=False)
    field = by_field(report)["email"]
    assert any(m["ruletype"] == "field" for m in field["matches"])
    assert all(m["ruletype"] not in ("data", "registry") for m in field["matches"])


def test_names_only_policy_sends_no_values():
    cmd = cmd_with(keep_all_but(set()))
    cmd.scan_data(ROWS, parse_dates=False)
    prompt = cmd.llm_verifier.provider.prompts[0]
    assert "alice@example.com" not in prompt and GUIDS[0] not in prompt and "package_id" in prompt


def test_redacted_policy_sends_masked_values():
    cmd = cmd_with(keep_all_but(set()), policy="redacted")
    cmd.scan_data(ROWS, parse_dates=False)
    prompt = cmd.llm_verifier.provider.prompts[0]
    assert "alice@example.com" not in prompt and "aaaaa@aaaaaaa.aaa" in prompt


def test_answers_are_cached():
    cmd = cmd_with(keep_all_but({"uuid"}))
    cmd.scan_data(ROWS, parse_dates=False)
    report = cmd.scan_data(ROWS, parse_dates=False)
    assert report["llm_verifier"]["cached"] is True
    assert len(cmd.llm_verifier.provider.prompts) == 1


def test_missing_column_in_answer_keeps_candidates():
    cmd = cmd_with(lambda schema: {})
    report = cmd.scan_data(ROWS, parse_dates=False)
    assert any(m["dataclass"] == "uuid" for m in by_field(report)["package_id"]["matches"])
    assert report["llm_verifier"]["removed"] == 0


def test_budget_error_and_fallback():
    cmd = cmd_with(keep_all_but({"uuid"}))
    cmd.llm_verifier.max_requests = 0
    cmd.llm_max_requests = 1
    with pytest.raises(RuntimeError, match="budget"):
        cmd.scan_data(ROWS, parse_dates=False)
    fallback = cmd_with(keep_all_but({"uuid"}), llm_fallback="rules")
    fallback.llm_verifier.max_requests = 0
    report = fallback.scan_data(ROWS, parse_dates=False)
    assert any(m["dataclass"] == "uuid" for m in by_field(report)["package_id"]["matches"])
    assert report["llm_verifier"]["error"] == "LLMBudgetExceeded" and report["complete"] is False


def test_llm_verify_from_config(tmp_path, monkeypatch):
    from unittest.mock import patch

    from metacrafter.core import CrafterCmd

    (tmp_path / ".metacrafter").write_text("llm_verify: true\nllm_provider: ollama\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    with patch("metacrafter.scan_service.CrafterCmd._llm_provider", return_value=FakeProvider(keep_all_but(set()))):
        assert CrafterCmd(quiet=True).llm_verifier is not None
        assert CrafterCmd(quiet=True, llm_verify=False).llm_verifier is None
