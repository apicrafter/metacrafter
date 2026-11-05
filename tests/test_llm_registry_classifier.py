"""Registry-driven LLM classification: shortlist, constrained answers, disclosure, cache."""
from pathlib import Path

import pytest

from metacrafter.classify.llm.registry_classifier import (
    RegistryLLMClassifier,
    build_prompt,
    classification_schema,
    mask_value,
    shortlist,
)
from metacrafter.registry.adapter import RegistryAdapter

SNAPSHOT = Path(__file__).parent / "fixtures" / "registry_snapshot"


class FakeProvider:
    def __init__(self, answer, is_local=False):
        self.answer = answer
        self.is_local = is_local
        self.model = "fake"
        self.prompts = []
        self.schemas = []

    def classify(self, prompt, temperature=0.0, max_tokens=500, schema=None):
        self.prompts.append(prompt)
        self.schemas.append(schema)
        return dict(self.answer)


@pytest.fixture(scope="module")
def registry():
    return RegistryAdapter.from_directory(SNAPSHOT)


def test_shortlist_uses_values_and_field_name(registry):
    ids = [r["id"] for r in shortlist(registry, "contact_email", ["a@example.com", "b@example.org"])]
    assert "email" in ids


def test_schema_constrains_to_shortlist():
    schema = classification_schema(["email", "phone"])
    assert schema["properties"]["datatype_id"]["anyOf"][0]["enum"] == ["email", "phone"]


def test_prompt_separates_negative_examples(registry):
    prompt = build_prompt("iban", [], "none", [registry.record("iban")])
    assert "NOT this type" in prompt
    assert "GB82WEST12345698765433" in prompt.split("NOT this type")[1]
    assert "not shared" in prompt


def test_invented_id_is_rejected(registry):
    provider = FakeProvider({"datatype_id": "made_up", "confidence": 0.9, "reason": "x"})
    clf = RegistryLLMClassifier(registry, provider)
    result = clf.classify_field("contact_email", ["a@example.com"])
    assert result["datatype_id"] is None
    assert result["evidence"]["rejected"]["datatype_id"] == "made_up"


def test_alias_answer_resolves(registry):
    provider = FakeProvider({"datatype_id": "guid", "confidence": 0.9, "reason": "x"})
    clf = RegistryLLMClassifier(registry, provider)
    result = clf.classify_field("record_uuid", ["3f2504e0-4f89-11d3-9a0c-0305e82c3301"])
    if "uuid" in result["evidence"]["shortlist"]:
        assert result["datatype_id"] == "uuid"


def test_pii_candidates_are_masked_for_remote_providers(registry):
    provider = FakeProvider({"datatype_id": "email", "confidence": 0.9, "reason": "x"}, is_local=False)
    clf = RegistryLLMClassifier(registry, provider, sample_policy="raw")
    result = clf.classify_field("contact_email", ["alice@example.com"])
    assert result["evidence"]["disclosure"] == "masked"
    assert "alice@example.com" not in provider.prompts[0]
    assert "aaaaa@aaaaaaa.aaa" in provider.prompts[0]
    assert result["is_pii"] is True


def test_local_provider_or_explicit_opt_in_sends_raw(registry):
    local = FakeProvider({"datatype_id": "email", "confidence": 0.9, "reason": "x"}, is_local=True)
    RegistryLLMClassifier(registry, local, sample_policy="raw").classify_field("contact_email", ["alice@example.com"])
    assert "alice@example.com" in local.prompts[0]
    remote = FakeProvider({"datatype_id": "email", "confidence": 0.9, "reason": "x"})
    RegistryLLMClassifier(registry, remote, sample_policy="raw", send_pii_values=True).classify_field("contact_email", ["alice@example.com"])
    assert "alice@example.com" in remote.prompts[0]


def test_names_only_policy_shares_no_values(registry):
    provider = FakeProvider({"datatype_id": "email", "confidence": 0.9, "reason": "x"})
    result = RegistryLLMClassifier(registry, provider).classify_field("contact_email", ["alice@example.com"])
    assert result["evidence"]["disclosure"] == "none"
    assert "alice" not in provider.prompts[0]


def test_results_are_cached(registry):
    provider = FakeProvider({"datatype_id": "email", "confidence": 0.9, "reason": "x"})
    clf = RegistryLLMClassifier(registry, provider)
    clf.classify_field("contact_email", ["a@example.com"])
    second = clf.classify_field("contact_email", ["a@example.com"])
    assert second["cached"] is True
    assert len(provider.prompts) == 1


def test_mask_value():
    assert mask_value("Ab-12") == "Aa-99"


def test_crafter_cmd_maps_llm_confidence_and_enriches():
    from metacrafter.core import CrafterCmd

    cmd = CrafterCmd(quiet=True)
    cmd.llm_classifier = RegistryLLMClassifier(
        cmd.registry, FakeProvider({"datatype_id": "email", "confidence": 1.0, "reason": "x"})
    )
    report = cmd.scan_data([{"contact": "a@example.com"}], classification_mode="llm", confidence=50, parse_dates=False)
    match = report["data"][0]["matches"][0]
    assert match["ruletype"] == "llm"
    assert match["confidence"] == 80.0
    assert match["datatype"] == "email"
    assert match["pii_source"] == "registry"
    assert match["evidence"]["scoring_model"] == "v1-llm"


def test_cache_key_changes_with_registry_version(registry):
    from dataclasses import replace

    from metacrafter.registry.adapter import RegistryAdapter

    provider = FakeProvider({"datatype_id": "email", "confidence": 0.9, "reason": "x"})
    clf = RegistryLLMClassifier(registry, provider)
    key_before = clf._cache_key("f", ["v"])
    other = RegistryAdapter(registry.registry, replace(registry.provenance, version="2.0.0"))
    assert RegistryLLMClassifier(other, provider)._cache_key("f", ["v"]) != key_before
