"""Run from outside the checkout after installing a wheel."""

from metacrafter import CrafterCmd
from metacrafter.config import ConfigLoader, DEFAULT_RULEPATH
from metacrafter.product_tools import lint_rules

ConfigLoader.load_config = staticmethod(lambda: None)
assert DEFAULT_RULEPATH, "metacrafter-rules default pack not found"
processor, diagnostics = lint_rules(DEFAULT_RULEPATH[0])
compile_errors = [d for d in diagnostics if d["error"] == "Pattern compilation failed"]
assert not compile_errors, compile_errors
assert len(processor.field_rules) + len(processor.data_rules) >= 1000
report = CrafterCmd(quiet=True).scan_data(
    [{"email": "test@example.com", "x": 1}], parse_dates=False
)
assert any(
    m["dataclass"] == "email" for field in report["data"] for m in field["matches"]
)
assert "x" in report["stats"]
print("Installed default rules, SDK imports, short fields and classification verified")
