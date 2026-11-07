"""Universal rules under --langs/--country-codes, language suffixes, parent name evidence, LLM budget."""

import pytest

from metacrafter.classify.processor import RULE_TYPE_DATA, RulesProcessor
from metacrafter.classify.utils import strip_language_suffix
from metacrafter.registry.adapter import RegistryAdapter, SnapshotRegistry


def write(tmp_path, name, body, lang="common", country=None):
    head = f"format_version: 2\nname: {name}\ndescription: t\ncontext: common\nlang: {lang}\n"
    if country:
        head += f"country_code: {country}\n"
    path = tmp_path / f"{name}.yaml"
    path.write_text(head + "rules:\n" + body, encoding="utf-8")
    return str(path)


def field_rule(rid, key, names):
    return f"  {rid}:\n    key: {key}\n    match: text\n    type: field\n    rule: {names}\n    priority: 1\n"


# --- universal rules ------------------------------------------------------------

def load(tmp_path, **kwargs):
    processor = RulesProcessor(**kwargs)
    processor.import_rules(write(tmp_path, "universal", field_rule("emailfield", "email", "email"), country="xx"))
    processor.import_rules(write(tmp_path, "nocountry", field_rule("urlfield", "url", "url")))
    processor.import_rules(write(tmp_path, "us", field_rule("ssnfield", "usssn", "ssn"), lang="en", country="us"))
    processor.import_rules(write(tmp_path, "ru", field_rule("innfield", "inn", "inn"), lang="ru", country="ru"))
    return {r["key"] for r in processor.field_rules}


def test_country_filter_keeps_universal_rules(tmp_path):
    assert load(tmp_path, countries=["us"]) == {"email", "url", "usssn"}


def test_language_filter_keeps_language_independent_rules(tmp_path):
    assert load(tmp_path, langs=["ru"]) == {"email", "url", "inn"}


def test_no_filters_load_everything(tmp_path):
    assert load(tmp_path) == {"email", "url", "usssn", "inn"}


def test_match_time_language_filter_keeps_common_rules(tmp_path):
    processor = RulesProcessor()
    processor.import_rules(write(tmp_path, "c", "  anyvalue:\n    key: uuid\n    match: regex\n    type: data\n    rule: 'x+'\n"))
    processor.import_rules(write(tmp_path, "e", "  envalue:\n    key: word\n    match: regex\n    type: data\n    rule: 'y+'\n", lang="en"))
    processor.import_rules(write(tmp_path, "r", "  ruvalue:\n    key: slovo\n    match: regex\n    type: data\n    rule: 'z+'\n", lang="ru"))
    keys = {r["key"] for r in processor.get_filtered_rules(RULE_TYPE_DATA, langs=["en"])}
    assert keys == {"uuid", "word"}


# --- language suffixes ----------------------------------------------------------

@pytest.mark.parametrize("name,base", [
    ("title_en", "title"), ("abstract_EN", "abstract"), ("name-ru", "name"), ("description.hy", "description"),
    ("item.title.ru", "item.title"), ("meta.id", "meta.id"),
    ("user_id", "user_id"), ("is_no", "is_no"), ("time_ms", "time_ms"), ("title", "title"), ("en", "en"),
])
def test_strip_language_suffix(name, base):
    assert strip_language_suffix(name) == base


def test_field_rule_matches_name_without_language_suffix(tmp_path):
    processor = RulesProcessor()
    processor.import_rules(write(tmp_path, "t", field_rule("descriptionfield", "description", "abstract,description")))
    result = processor.match_dict([{"abstract_en": "Text", "category": {"description_en": "Text"}, "user_id": "1"}],
                                  confidence=5, parse_dates=False)
    found = {r.field: {m.dataclass for m in r.matches} for r in result.results}
    assert found["abstract_en"] == {"description"}
    assert found["category.description_en"] == {"description"}
    assert found["user_id"] == set()


def test_field_rule_matches_parent_of_language_key(tmp_path):
    processor = RulesProcessor()
    processor.import_rules(write(tmp_path, "t", field_rule("descriptionfield", "description", "abstract,description")))
    rows = [{"description": {"ru": "Текст", "en": "Text"}, "item": {"abstract": {"de": "Text"}},
             "meta": {"id": "1"}, "en": "Text"}]
    result = processor.match_dict(rows, confidence=5, parse_dates=False)
    found = {r.field: {m.dataclass for m in r.matches} for r in result.results}
    assert found["description.ru"] == {"description"}
    assert found["description.en"] == {"description"}
    assert found["item.abstract.de"] == {"description"}
    assert found["meta.id"] == set()
    assert found["en"] == set()


# --- parent name evidence -------------------------------------------------------

RECORDS = [
    {"id": "language", "status": "stable", "kind": "datatype", "name": "Language"},
    {"id": "iso639_2", "status": "stable", "kind": "datatype", "name": "ISO 639-2 code",
     "regexp": "^(?:[a-z]{3}|[A-Z]{3})$", "parent": "language"},
    {"id": "driver_license", "status": "stable", "kind": "datatype", "name": "Driver license"},
    {"id": "xx_driver_license", "status": "stable", "kind": "datatype", "name": "X driver license",
     "regexp": "^X\\d{8}$", "parent": "driver_license", "country": ["XX"]},
]


@pytest.fixture
def processor(tmp_path):
    registry = SnapshotRegistry(datatypes=RECORDS, aliases={}, manifest={"version": "t"}, source="t")
    processor = RulesProcessor()
    processor.import_rules(write(tmp_path, "p", field_rule("placeholder", "email", "email")))
    processor.bind_registry(RegistryAdapter._wrap(registry, None, "t"), detectors="stable")
    return processor


def classes(processor, rows):
    result = processor.match_dict(rows, confidence=5, parse_dates=False)
    return {r.field: {m.dataclass for m in r.matches} for r in result.results}


def test_parent_name_counts_for_generic_pattern(processor):
    found = classes(processor, [{"language": "eng", "workspace": "geo"}, {"language": "rus", "workspace": "dev"}])
    assert "iso639_2" in found["language"]
    assert "iso639_2" not in found["workspace"]


def test_parent_name_of_language_key_counts_for_generic_pattern(processor):
    found = classes(processor, [{"language": {"en": "eng"}, "workspace": {"en": "geo"}},
                                {"language": {"en": "rus"}, "workspace": {"en": "dev"}}])
    assert "iso639_2" in found["language.en"]
    assert "iso639_2" not in found["workspace.en"]


def test_parent_name_scores_half_bonus(processor):
    result = processor.match_dict([{"language": "eng"}], confidence=5, parse_dates=False)
    match = next(m for m in result.results[0].matches if m.dataclass == "iso639_2")
    assert match.evidence["name_score"] == 0.5


def test_parent_name_does_not_lift_country_gate(processor):
    rows = [{"driver_license": "X12345678"}, {"driver_license": "X87654321"}]
    assert "xx_driver_license" not in classes(processor, rows)["driver_license"]
    own = [{"xx_driver_license": "X12345678"}]
    assert "xx_driver_license" in classes(processor, own)["xx_driver_license"]


# --- LLM request budget ---------------------------------------------------------

class CountingProvider:
    is_local = False
    model = "fake"

    def __init__(self):
        self.calls = 0

    def classify(self, prompt, temperature=0.0, max_tokens=500, schema=None):
        self.calls += 1
        return {"datatype_id": None, "confidence": 0.0, "reason": "x"}


def llm_cmd(max_requests, fallback="error"):
    from metacrafter.classify.llm.registry_classifier import RegistryLLMClassifier
    from metacrafter.core import CrafterCmd

    cmd = CrafterCmd(quiet=True, llm_max_requests=max_requests, llm_fallback=fallback)
    cmd.llm_classifier = RegistryLLMClassifier(cmd.registry, CountingProvider(), max_requests=cmd.llm_max_requests)
    return cmd


ROWS = [{"contact_email": "a@example.com", "uuid": "3f2504e0-4f89-11d3-9a0c-0305e82c3301", "website": "https://example.com/"}]


def test_llm_budget_error_names_the_setting():
    cmd = llm_cmd(1)
    with pytest.raises(RuntimeError, match="budget of 1 requests exceeded; raise llm_max_requests"):
        cmd.scan_data(ROWS, classification_mode="llm", parse_dates=False)
    assert cmd.llm_errors[0]["code"] == "LLMBudgetExceeded"


def test_llm_budget_fallback_uses_rules():
    cmd = llm_cmd(1, fallback="rules")
    report = cmd.scan_data(ROWS, classification_mode="llm", parse_dates=False)
    assert report["results"]
    assert cmd.llm_classifier.provider.calls == 1


def test_llm_max_requests_from_config(tmp_path, monkeypatch):
    from metacrafter.core import CrafterCmd

    (tmp_path / ".metacrafter").write_text("llm_max_requests: 250\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    assert CrafterCmd(quiet=True).llm_max_requests == 250
    assert CrafterCmd(quiet=True, llm_max_requests=7).llm_max_requests == 7


def test_llm_max_requests_default():
    from metacrafter.core import CrafterCmd

    assert CrafterCmd(quiet=True).llm_max_requests == 100
    with pytest.raises(ValueError):
        CrafterCmd(quiet=True, llm_max_requests=0)
