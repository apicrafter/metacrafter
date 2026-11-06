"""Rules processor module for classifying and matching data types."""
import glob
import os
import importlib
import ast
import re
import logging
import warnings
from functools import lru_cache
from pathlib import Path
from typing import List, Dict, Any, Optional, Union, Set, Callable

import yaml

from metacrafter.engine.registry_detectors import detector_name_evidence, detector_names_country_type
from metacrafter.engine.scoring import data_score, field_score, priority_weight
from pyparsing import (
    LineEnd,
    LineStart,
    Word,
    oneOf,
    Literal,
    CaselessLiteral,
    alphas,
    alphanums,
    hexnums,
    Optional as PyParsingOptional,
    ZeroOrMore,
    printables,
    nums,
    ParseException,
    lineEnd,
    lineStart,
    ParserElement,
)

from .utils import headers, dict_to_columns, strip_language_suffix


def _normalize_country_codes(
    country_value: Optional[Union[str, List[str], tuple, set]]
) -> Optional[List[str]]:
    """Return normalized list of lower-cased country codes.

    Args:
        country_value: Comma-separated string, single string, list, tuple, or set
            of country codes. Can be None.

    Returns:
        List of normalized (lowercase) country codes, or None if input is empty/None.
    """
    if not country_value:
        return None
    if isinstance(country_value, str):
        parts = [country_value]
    elif isinstance(country_value, (list, tuple, set)):
        parts = list(country_value)
    else:
        return None
    normalized = []
    for token in parts:
        if token is None:
            continue
        for piece in str(token).replace(";", ",").split(","):
            code = piece.strip().lower()
            if code:
                normalized.append(code)
    return normalized or None


def _create_safe_namespace() -> Dict[str, Any]:
    """Create a safe namespace for evaluating PyParsing rules.
    
    This restricts eval() to only allow PyParsing objects and basic operations,
    preventing arbitrary code execution.
    
    Returns:
        Dictionary containing safe namespace with PyParsing classes and
        restricted built-in functions.
    """
    # Create a restricted namespace with only allowed PyParsing objects
    safe_dict = {
        # PyParsing classes
        'Word': Word,
        'Literal': Literal,
        'CaselessLiteral': CaselessLiteral,
        'Optional': PyParsingOptional,
        'ZeroOrMore': ZeroOrMore,
        'oneOf': oneOf,
        'LineStart': LineStart,
        'LineEnd': LineEnd,
        'lineStart': lineStart,
        'lineEnd': lineEnd,
        # Character sets
        'alphas': alphas,
        'alphanums': alphanums,
        'hexnums': hexnums,
        'nums': nums,
        'printables': printables,
        # Basic operations
        '__builtins__': {
            'len': len,
            'str': str,
            'int': int,
            'float': float,
            'min': min,
            'max': max,
            'abs': abs,
        },
    }
    return safe_dict


def _compile_rule_with_warning_capture(
    rule_string: str, rule_id: str, filename: str, rule_label: str
) -> ParserElement:
    """Compile rule while capturing SyntaxWarnings for debugging.
    
    Args:
        rule_string: PyParsing rule expression as string
        rule_id: Unique identifier for the rule
        filename: Source filename where rule is defined
        rule_label: Label for the rule type (e.g., "data", "field")
    
    Returns:
        Compiled PyParsing parser element
        
    Raises:
        ValueError: If rule compilation fails
        SyntaxError: If rule syntax is invalid
    """
    with warnings.catch_warnings(record=True) as caught_warnings:
        warnings.simplefilter("always", SyntaxWarning)
        compiled_rule = _safe_eval_pyparsing_rule(
            rule_string,
            f"{filename}:{rule_id}:{rule_label}",
        )
    for warn in caught_warnings:
        logging.warning(
            "SyntaxWarning when compiling %s rule '%s' in %s: %s",
            rule_label,
            rule_id,
            filename,
            warn.message,
        )
    return compiled_rule

_YAML_LOADER = getattr(yaml, "CSafeLoader", yaml.SafeLoader)


@lru_cache(maxsize=1024)
def _parse_rule_file(filename: str, mtime_ns: int, size: int) -> Any:
    """Parse a rule file once per (path, mtime, size); callers get a deep copy."""
    with open(filename, "r", encoding="utf8") as f:
        return yaml.load(f, Loader=_YAML_LOADER)  # noqa: S506 - safe loader


def _load_rule_document(filename: str) -> Any:
    import copy

    stat = os.stat(filename)
    return copy.deepcopy(_parse_rule_file(str(filename), stat.st_mtime_ns, stat.st_size))


_MAX_REPETITION = 1000


def _is_bounded_repetition(node: ast.AST) -> bool:
    """Return True for a repetition count PyParsing accepts with a safe upper bound.

    Accepted forms: an integer ``0..1000``, or a 2-tuple of such integers where the
    upper bound may also be ``None`` or ``...`` (open-ended, as in ``ZeroOrMore``).
    """
    def bounded_int(item: ast.AST) -> bool:
        return (
            isinstance(item, ast.Constant)
            and isinstance(item.value, int)
            and not isinstance(item.value, bool)
            and 0 <= item.value <= _MAX_REPETITION
        )

    if bounded_int(node):
        return True
    if isinstance(node, ast.Tuple) and len(node.elts) == 2:
        low, high = node.elts
        open_high = isinstance(high, ast.Constant) and (high.value is None or high.value is Ellipsis)
        return bounded_int(low) and (bounded_int(high) or open_high)
    return False


@lru_cache(maxsize=4096)
def _safe_eval_pyparsing_rule(
    rule_string: str, rule_source: str = "<string>"
) -> ParserElement:
    """Safely evaluate a PyParsing rule string.
    
    Cached to avoid recompiling the same rules multiple times.
    
    Args:
        rule_string: String containing PyParsing expression
        rule_source: Source identifier for error messages (filename or description)
        
    Returns:
        Compiled PyParsing parser element
        
    Raises:
        ValueError: If rule contains unsafe code or compilation fails
        SyntaxError: If rule syntax is invalid
    """
    if not isinstance(rule_string, str) or len(rule_string) > 16000:
        raise ValueError("Pattern must be a string of at most 16000 characters")
    safe_dict = _create_safe_namespace()
    safe_dict.pop("__builtins__", None)
    tree = ast.parse(rule_string, mode="eval")
    nodes = list(ast.walk(tree))
    if len(nodes) > 2000:
        raise ValueError("Pattern expression is too complex")
    allowed = (ast.Expression, ast.Constant, ast.Name, ast.Load, ast.Call,
               ast.keyword, ast.BinOp, ast.Add, ast.Sub, ast.BitOr, ast.BitAnd, ast.BitXor,
               ast.Mult, ast.UnaryOp, ast.USub, ast.List, ast.Tuple, ast.Attribute,
               ast.Subscript)
    methods = {"suppress", "setResultsName", "set_results_name", "leaveWhitespace",
               "leave_whitespace", "copy", "combine", "setName", "set_name"}
    for node in nodes:
        if not isinstance(node, allowed):
            raise ValueError("Unsupported pattern syntax: " + type(node).__name__)
        if isinstance(node, ast.Name) and node.id not in safe_dict:
            raise ValueError("Unknown pattern name: " + node.id)
        if isinstance(node, ast.Attribute) and node.attr not in methods:
            raise ValueError("Unsupported parser method: " + node.attr)
        if isinstance(node, ast.Call):
            if not isinstance(node.func, (ast.Name, ast.Attribute)):
                raise ValueError("Unsupported pattern call")
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Mult):
            # PyParsing repetition: ``expr * 3``, ``3 * expr`` or ``expr * (0, 6)``.
            if not (_is_bounded_repetition(node.right) or _is_bounded_repetition(node.left)):
                raise ValueError("Parser repetition must be a bounded integer")
        if isinstance(node, ast.Subscript):
            # PyParsing indexed repetition: ``expr[2]``, ``expr[0, ...]``.
            if not _is_bounded_repetition(node.slice):
                raise ValueError("Parser repetition must be a bounded integer")
    rule_filename = rule_source or "<string>"

    try:
        # Compile the rule in restricted namespace
        code_obj = compile(rule_string, "<pyparsing-rule>", "eval")
        compiled_rule = eval(code_obj, {"__builtins__": {}}, safe_dict)
        if not isinstance(compiled_rule, ParserElement):
            raise ValueError("Expression must produce a PyParsing parser")
        return compiled_rule
    except (SyntaxError, NameError, TypeError) as e:
        # These are expected errors for invalid rule syntax
        raise ValueError(
            f"Failed to compile PyParsing rule: {rule_string}. "
            f"Error: {str(e)}"
        ) from e
    except Exception as e:
        # Catch-all for any other unexpected errors
        logging.error(f"Unexpected error compiling rule '{rule_string}': {e}", exc_info=True)
        raise ValueError(
            f"Failed to compile PyParsing rule: {rule_string}. "
            f"Unexpected error: {str(e)}"
        ) from e

DEFAULT_MAX_LEN = 100
DEFAULT_MIN_LEN = 3
DEFAULT_PRIORITY = 3
REGEX_MAX_LEN = 256
MATCH_TYPES = ("text", "ppr", "func", "regex", "registry")
# Rule files with this country code (or none) apply in every country.
UNIVERSAL_COUNTRY = "xx"
# Rule files with this language are language-independent.
COMMON_LANG = "common"

RULE_TYPE_FIELD = 1
RULE_TYPE_DATA = 2

BASE_URL = "https://registry.apicrafter.io/datatype/{dataclass}"


class TableScanResult:
    """Results of table scan classification.
    
    Aggregates classification results for all columns in a table.
    """

    def __init__(self) -> None:
        """Initialize empty table scan result."""
        self.results: List['ColumnMatchResult'] = []

    def add(self, result: 'ColumnMatchResult') -> None:
        """Add a column match result to the table scan.
        
        Args:
            result: ColumnMatchResult instance to add
        """
        self.results.append(result)

    def is_empty(self) -> bool:
        """Check if scan result is empty.
        
        Returns:
            True if no column results have been added, False otherwise
        """
        return len(self.results) == 0

    def asdict(self) -> List[Dict[str, Any]]:
        """Convert scan results to dictionary format.
        
        Returns:
            List of dictionaries, each representing a column match result
        """
        res = []
        for m in self.results:
            res.append(m.asdict())
        return res


class ColumnMatchResult:
    """Results of column classification.
    
    Contains all rule matches for a single column/field.
    """

    def __init__(self, field: str, matches: Optional[List['RuleResult']] = None) -> None:
        """Initialize column match result.
        
        Args:
            field: Field/column name
            matches: Optional list of rule matches (defaults to empty list)
        """
        self.field: str = field
        self.matches: List['RuleResult'] = matches if matches is not None else []
        self.rejected = None

    def add(self, match: 'RuleResult') -> None:
        """Add a rule match result.
        
        Args:
            match: RuleResult instance to add
        """
        self.matches.append(match)

    def is_empty(self) -> bool:
        """Check if no matches found.
        
        Returns:
            True if no rule matches, False otherwise
        """
        return len(self.matches) == 0

    def asdict(self) -> Dict[str, Any]:
        """Convert column match result to dictionary format.
        
        Returns:
            Dictionary with 'field' and 'matches' keys
        """
        matches = []
        for m in self.matches:
            matches.append(m.asdict())
        result = {"field": self.field, "matches": matches}
        if self.rejected is not None:
            result["rejected_candidates"] = self.rejected
        return result


def rule_pii_source(rule: Dict[str, Any]) -> Optional[str]:
    """Return why a rule marks PII: its own ``is_pii`` flag or its ``pii`` context."""
    if rule.get("is_pii") is True:
        return "rule"
    if "pii" in rule.get("context", []):
        return "context"
    return None


def rule_is_pii(rule: Dict[str, Any]) -> bool:
    return rule_pii_source(rule) is not None


class RuleResult:
    """Result of a single rule match.
    
    Represents one classification rule that matched a field or data value.
    """

    def __init__(
        self,
        ruleid: str,
        dataclass: str,
        confidence: float,
        ruletype: str,
        is_pii: bool = False,
        format: Optional[str] = None,
        evidence=None,
        pii_source: Optional[str] = None,
    ) -> None:
        """Initialize rule match result.
        
        Args:
            ruleid: Unique identifier of the matching rule
            dataclass: Data class/type that was matched
            confidence: Confidence score (0-100)
            ruletype: Type of rule that matched ("field", "data", "fieldtype")
            is_pii: Whether this match represents PII data
            format: Optional format string (e.g., date format pattern)
        """
        self.ruleid: str = ruleid
        self.dataclass: str = dataclass
        self.confidence: float = confidence
        self.ruletype: str = ruletype
        self.is_pii: bool = is_pii
        self.format: Optional[str] = format
        self.evidence = evidence
        self.pii_source: Optional[str] = pii_source if is_pii else None
        self.pattern: Optional[str] = None

    def class_url(self) -> str:
        """Get URL to data class registry entry.
        
        Returns:
            URL string pointing to the data class definition
        """
        from metacrafter.registry.client import default_registry_url
        return (default_registry_url() + "/datatype/{dataclass}").format(dataclass=self.dataclass)

    def asdict(self) -> Dict[str, Any]:
        """Convert rule result to dictionary format.
        
        Returns:
            Dictionary containing all rule result fields
        """
        return {
            "evidence": self.evidence,
            "reason": getattr(self, "reason", None),
            "ruleid": self.ruleid,
            "dataclass": self.dataclass,
            "confidence": self.confidence,
            "ruletype": self.ruletype,
            "format": self.format,
            "classurl": self.class_url(),
            "is_pii": self.is_pii,
            "pii_source": self.pii_source,
            "pattern": self.pattern,
        }


class RulesProcessor:
    """Classification rules processor class.
    
    Loads, compiles, and applies classification rules to identify data types
    in structured data (CSV, JSON, databases, etc.).
    """

    def __init__(
        self,
        langs: Optional[List[str]] = None,
        contexts: Optional[List[str]] = None,
        countries: Optional[List[str]] = None,
    ) -> None:
        """Initialize rules processor.
        
        Args:
            langs: Optional list of language codes to filter rules (e.g., ['en', 'ru'])
            contexts: Optional list of context filters (e.g., ['pii', 'common'])
            countries: Optional list of ISO country codes to filter rules
        """
        self.preset_langs: Optional[List[str]] = langs
        self.preset_contexts: Optional[List[str]] = contexts
        self.preset_countries: Optional[List[str]] = (
            [c.lower() for c in countries] if countries else None
        )
        self.registry = None
        self.reset_rules()

    def reset_rules(self) -> None:
        """Clear all imported rules and reset statistics.
        
        Removes all field rules, data rules, and resets language/context/country
        statistics. Useful for reinitializing the processor.
        """
        self.diagnostics = []
        self.data_rules: List[Dict[str, Any]] = []
        self.field_rules: List[Dict[str, Any]] = []
        self.__rule_keys: List[str] = []
        self.langs: Dict[str, int] = {}
        self.contexts: Dict[str, int] = {}
        self.countries: Dict[Optional[str], int] = {}

    def import_rules(self, filename: str) -> None:
        """Import classification rules from a YAML file.
        
        Loads rules from a YAML file, compiles PyParsing expressions, and
        filters rules based on preset language, context, and country filters.
        
        Args:
            filename: Path to YAML file containing rule definitions
            
        Raises:
            IOError: If file cannot be read
            yaml.YAMLError: If YAML file is malformed
            ValueError: If rule compilation fails
        """
        logging.debug("Loading rules file %s", filename)
        ruledata = _load_rule_document(filename)

        from metacrafter.rules.validation import validate_rule_file

        if not isinstance(ruledata, dict):
            self.diagnostics.append({"ruleid": None, "source": filename, "error": "Rule file must be a mapping"})
            logging.error("Skipping rule file %s: not a mapping", filename)
            return
        file_errors, rule_errors = validate_rule_file(ruledata)
        if file_errors:
            for message in file_errors:
                self.diagnostics.append({"ruleid": None, "source": filename, "error": "Schema: " + message})
            logging.error("Skipping invalid rule file %s: %s", filename, "; ".join(file_errors))
            return

        # Language and country filters select language- and country-specific rule files;
        # language-independent (lang: common) and universal (no country or country_code: xx)
        # files always load.
        if self.preset_langs and ruledata["lang"] not in self.preset_langs and ruledata["lang"] != COMMON_LANG:
            return
        if self.preset_contexts and ruledata["context"] not in self.preset_contexts:
            return
        rule_countries = _normalize_country_codes(ruledata.get("country_code"))
        if self.preset_countries and rule_countries and UNIVERSAL_COUNTRY not in rule_countries:
            if not any(code in self.preset_countries for code in rule_countries):
                return

        for rulekey in ruledata["rules"].keys():
            rule = ruledata["rules"][rulekey]
            if isinstance(rule, dict) and rule.get("disabled") is True:
                continue
            if rulekey in rule_errors:
                for message in rule_errors[rulekey]:
                    self.diagnostics.append({"ruleid": rulekey, "source": filename, "error": "Schema: " + message})
                logging.warning("Skipping rule '%s' in %s: %s", rulekey, filename, "; ".join(rule_errors[rulekey]))
                continue
            if not isinstance(rule, dict) or rule.get("match") not in MATCH_TYPES or rule.get("type") not in ("field", "data") or not isinstance(rule.get("key"), str) or not isinstance(rule.get("rule"), str) or not rule.get("rule"):
                self.diagnostics.append({"ruleid": rulekey, "source": filename, "error": "Invalid rule structure"})
                continue
            rule["deprecated"] = rule.get("deprecated") is True
            declared_minlen = "minlen" in rule
            declared_maxlen = "maxlen" in rule
            try:
                rule["priority"] = int(rule.get("priority", DEFAULT_PRIORITY))
                rule["minlen"] = int(rule.get("minlen", DEFAULT_MIN_LEN))
                rule["maxlen"] = int(rule.get("maxlen", DEFAULT_MAX_LEN))
                if not 0 <= rule["minlen"] <= rule["maxlen"] <= 100000:
                    raise ValueError("Invalid length range")
            except (ValueError, TypeError):
                self.diagnostics.append({"ruleid": rulekey, "source": filename, "error": "Invalid priority or length"})
                continue
            if "fieldrule" in rule and rule.get("fieldrulematch") not in ("ppr", "text"):
                self.diagnostics.append({"ruleid": rulekey, "source": filename, "error": "Invalid field guard"})
                continue
            rule["is_pii"] = rule.get("is_pii") in (True, "True", "true", 1)
            rule["source"] = filename

            rule["imprecise"] = (
                (int(rule["imprecise"]) != 0) if "imprecise" in rule.keys() else False
            )
            #            print(rulekey, rule['imprecise'])
            if rule["match"] == "ppr":
                try:
                    compiled_rule = _compile_rule_with_warning_capture(
                        rule["rule"],
                        rulekey,
                        filename,
                        "data",
                    )
                    rule["compiled"] = lineStart + compiled_rule + lineEnd
                except (ValueError, SyntaxError) as e:
                    logging.error(
                        f"Failed to compile PyParsing rule '{rulekey}': {e}. "
                        "Skipping this rule."
                    )
                    self.diagnostics.append({"ruleid": rulekey, "source": filename, "error": "Pattern compilation failed"})
                    continue
            elif rule["match"] == "func":
                try:
                    module, funcname = rule["rule"].rsplit(".", 1)
                    match_func = getattr(importlib.import_module(module), funcname)
                    rule["compiled"] = match_func
                except (ImportError, AttributeError, ValueError) as e:
                    logging.warning(
                        f"Failed to import function '{rule['rule']}' for rule '{rulekey}': {e}. "
                        "Skipping this rule."
                    )
                    self.diagnostics.append({"ruleid": rulekey, "source": filename, "error": "Function import failed"})
                    continue
            elif rule["match"] == "regex":
                try:
                    rule["compiled"] = re.compile(rule["rule"])
                except re.error as e:
                    logging.error(f"Failed to compile regex rule '{rulekey}': {e}. Skipping this rule.")
                    self.diagnostics.append({"ruleid": rulekey, "source": filename, "error": "Pattern compilation failed"})
                    continue
            elif rule["match"] == "registry":
                # Bound to the registry record's regexp and checksum in bind_registry().
                rule["registry_id"] = rule["rule"].strip()
                rule["compiled"] = None
            elif rule["match"] == "text":
                keywords = rule["rule"].split(",")
                # Text rules match by set membership; no parser is built (building
                # pyparsing oneOf expressions dominated rule loading time).
                rule["compiled"] = None
                rule["keywords"] = set(map(str.lower, keywords))
            if rule["match"] == "text":
                if not declared_maxlen:
                    rule["maxlen"] = len(max(keywords, key=len))
                if not declared_minlen:
                    rule["minlen"] = len(min(keywords, key=len))
            elif rule["match"] in ("regex", "registry"):
                rule["maxlen"] = int(rule["maxlen"]) if declared_maxlen else REGEX_MAX_LEN
                rule["minlen"] = int(rule["minlen"]) if declared_minlen else 1
            else:
                rule["maxlen"] = (
                    int(rule["maxlen"]) if "maxlen" in rule.keys() else DEFAULT_MAX_LEN
                )
                rule["minlen"] = (
                    int(rule["minlen"]) if "minlen" in rule.keys() else DEFAULT_MIN_LEN
                )
            if "validator" in rule.keys() and str(rule["validator"]).startswith("checksum:"):
                from metacrafter.engine.checksums import checksum_validator

                vfunc = checksum_validator(rule["validator"])
                if vfunc is None:
                    logging.warning(
                        "Checksum validator '%s' for rule '%s' is unavailable "
                        "(unknown algorithm or metacrafter-registry not installed). Skipping this rule.",
                        rule["validator"], rulekey,
                    )
                    self.diagnostics.append({"ruleid": rulekey, "source": filename, "error": "Required validator unavailable"})
                    continue
                rule["vfunc"] = vfunc
            elif "validator" in rule.keys():
                try:
                    module, funcname = rule["validator"].rsplit(".", 1)
                    match_func = getattr(importlib.import_module(module), funcname)
                    rule["vfunc"] = match_func
                except (ImportError, AttributeError, ValueError) as e:
                    logging.warning(
                        f"Failed to import validator '{rule['validator']}' for rule '{rulekey}': {e}. "
                        "Skipping validator."
                    )
                    self.diagnostics.append({"ruleid": rulekey, "source": filename, "error": "Required validator unavailable"})
                    continue
            if "fieldrule" in rule.keys() and "fieldrulematch" in rule.keys():
                if rule["fieldrulematch"] == "ppr":
                    try:
                        compiled_field_rule = _compile_rule_with_warning_capture(
                            rule["fieldrule"],
                            rulekey,
                            filename,
                            "field",
                        )
                        rule["f_compiled"] = lineStart + compiled_field_rule + lineEnd
                    except (ValueError, SyntaxError) as e:
                        logging.error(
                            f"Failed to compile field PyParsing rule '{rulekey}': {e}. "
                            "Skipping field rule."
                        )
                        self.diagnostics.append({"ruleid": rulekey, "source": filename, "error": "Required field guard unavailable"})
                        continue
                elif rule["fieldrulematch"] == "text":
                    keywords = rule["fieldrule"].split(",")
                    # Convert to set for O(1) membership testing instead of O(n) list lookup
                    rule["fieldkeywords"] = set(map(str.lower, keywords))
                    rule["f_compiled"] = None  # text guards match by set membership
            if rulekey in self.__rule_keys:
                # Later rulepath entries override earlier ones for the same rule id
                # (default pack first, then .metacrafter rulepath, then --rulepath).
                previous = self._unregister_rule(rulekey)
                logging.info(
                    "Rule '%s' from %s overrides the definition from %s",
                    rulekey,
                    filename,
                    previous.get("source") if previous else "an earlier file",
                )
            self.__rule_keys.append(rulekey)
            rule["id"] = rulekey
            for key in ["context", "lang"]:
                rule[key] = ruledata[key]
            rule["group"] = ruledata["name"]
            rule["group_desc"] = ruledata["description"]
            rule["country_code"] = rule_countries
            if rule["type"] == "field":
                self.field_rules.append(rule)
            elif rule["type"] == "data":
                self.data_rules.append(rule)

            v = self.langs.get(rule["lang"], 0)
            self.langs[rule["lang"]] = v + 1

            contexts = rule["context"].split(".")
            rule["context"] = contexts

            # Add more than one context
            if (
                "is_pii" in rule.keys()
                and rule["is_pii"]
                and "pii" not in rule["context"]
            ):
                rule["context"].append("pii")

            for context in contexts:
                v = self.contexts.get(context, 0)
                self.contexts[context] = v + 1
            if rule_countries:
                for code in rule_countries:
                    v = self.countries.get(code, 0)
                    self.countries[code] = v + 1
            else:
                v = self.countries.get(None, 0)
                self.countries[None] = v + 1

        # priority 1 is the strongest (metacrafter-rules convention); stable sort keeps file order
        self.field_rules.sort(key=lambda r: r["priority"])
        self.data_rules.sort(key=lambda r: r["priority"])
        logging.debug("Loaded rules from %s", filename)

    def register_rule(self, rule: Dict[str, Any]) -> None:
        """Add a pre-built rule (for example a registry-derived detector)."""
        if rule["id"] in self.__rule_keys:
            self._unregister_rule(rule["id"])
        self.__rule_keys.append(rule["id"])
        (self.field_rules if rule["type"] == "field" else self.data_rules).append(rule)
        self.langs[rule["lang"]] = self.langs.get(rule["lang"], 0) + 1
        for context in rule.get("context", []):
            self.contexts[context] = self.contexts.get(context, 0) + 1
        self.countries[None] = self.countries.get(None, 0) + 1

    def bind_registry(self, adapter, detectors: str = "stable") -> int:
        """Attach registry data: compile ``match: registry`` rules and add derived detectors.

        Returns the number of registry-derived detectors added.
        """
        from metacrafter.engine.registry_detectors import add_registry_detectors, bind_registry_rules

        self.registry = adapter
        bind_registry_rules(self, adapter)
        return add_registry_detectors(self, adapter, detectors)

    def _unregister_rule(self, rulekey: str) -> Optional[Dict[str, Any]]:
        """Remove a loaded rule by id and roll back its statistics counters."""
        previous = None
        for rules in (self.field_rules, self.data_rules):
            for index, existing in enumerate(rules):
                if existing.get("id") == rulekey:
                    previous = rules.pop(index)
                    break
            if previous is not None:
                break
        self.__rule_keys.remove(rulekey)
        if previous is None:
            return None
        lang = previous.get("lang")
        if lang in self.langs:
            self.langs[lang] -= 1
        for context in previous.get("context", []):
            if context in self.contexts:
                self.contexts[context] -= 1
        for code in previous.get("country_code") or [None]:
            if code in self.countries:
                self.countries[code] -= 1
        return previous

    def import_rules_path(self, pathname: str, recursive: bool = True) -> None:
        """Import rules from a directory path.
        
        Scans a directory (optionally recursively) for YAML rule files
        and imports all found rules.
        
        Args:
            pathname: Directory path containing rule files
            recursive: If True, search subdirectories recursively
            
        Raises:
            IOError: If path cannot be accessed
            yaml.YAMLError: If any YAML file is malformed
        """
        if not recursive:
            filenames = sorted(glob.glob(pathname + "/*.yaml"))
            for filename in filenames:
                self.import_rules(filename)
        else:
            for path in sorted(Path(pathname).rglob("*.yaml")):
                self.import_rules(str(path))

    def dumpStats(self) -> None:
        """Print statistics about loaded rules.
        
        Displays counts of field rules, data rules, languages, contexts,
        and country codes. Also shows count of date/time patterns from qddate.
        """
        import qddate

        print("Rule types:")
        print(f"- field based rules {len(self.field_rules)}")
        print(f"- data based rules {len(self.data_rules)}")
        print("Context:")
        for key in sorted(self.contexts.keys()):
            print(f"- {key} {self.contexts[key]}")
        print("Language:")
        for key in sorted(self.langs.keys()):
            print(f"- {key} {self.langs[key]}")
        if self.countries:
            print("Country codes:")
            for key in sorted(self.countries.keys(), key=lambda value: value or ""):
                print(f"- {key or 'unknown'} {self.countries[key]}")
        dparser = qddate.DateParser(
            patterns=qddate.patterns.PATTERNS_EN + qddate.patterns.PATTERNS_RU
        )
        print(f"Data/time patterns (qddate): {len(dparser.patterns)}")

    def get_filtered_rules(
        self,
        ruletype: int,
        contexts: Optional[List[str]] = None,
        langs: Optional[List[str]] = None,
        ignore_imprecise: bool = False,
        include_deprecated: bool = False,
    ) -> List[Dict[str, Any]]:
        """Filter rules by type, context, language, precision and deprecation.
        
        Filters the loaded rules based on the specified criteria. This helps
        reduce the number of rules to evaluate during data matching.
        
        Args:
            ruletype: Rule type constant (RULE_TYPE_FIELD or RULE_TYPE_DATA)
            contexts: Optional list of context filters (e.g., ['pii', 'common'])
            langs: Optional list of language codes to filter (e.g., ['en', 'ru'])
            ignore_imprecise: If True, exclude rules marked as imprecise
            
        Returns:
            List of filtered rule dictionaries matching the criteria
        """
        rules = self.field_rules if ruletype == RULE_TYPE_FIELD else self.data_rules
        if not include_deprecated:
            rules = [rule for rule in rules if not rule.get("deprecated")]
        filtered = []
        if not contexts and not langs and not ignore_imprecise:
            return rules
        for rule in rules:
            in_context = False
            in_lang = False
            in_imprecise = False
            if not contexts:
                in_context = True
            else:
                for rule_context in rule["context"]:
                    if rule_context in contexts:
                        in_context = True
                        break
            if not langs:
                in_lang = True
            elif rule["lang"] in langs or rule["lang"] == COMMON_LANG:
                in_lang = True
            if ignore_imprecise and rule["imprecise"]:
                in_imprecise = True
            if in_lang and in_context and not in_imprecise:
                filtered.append(rule)
        #            else:
        #                print('Rule %s removed' % (rule['key']))
        #            print(rule['key'], in_lang, in_context, in_imprecise)
        return filtered

    def match_dict(
        self,
        data: List[Dict[str, Any]],
        fields: Optional[List[str]] = None,
        datastats: Optional[Dict[str, Dict[str, Any]]] = None,
        confidence: float = 95.0,
        stop_on_match: bool = False,
        parse_dates: bool = True,
        dateparser: Optional[Any] = None,
        limit: int = 1000,
        filter_contexts: Optional[List[str]] = None,
        filter_langs: Optional[List[str]] = None,
        except_empty: bool = True,
        ignore_imprecise: bool = True,
        empty_values=None,
        explain=False,
        legacy_scoring: bool = False,
        include_deprecated: bool = False,
    ) -> TableScanResult:
        """Match classification rules against a list of dictionaries.
        
        Processes an array of dictionaries (e.g., from JSON lines or BSON)
        and applies classification rules to identify data types for each field.
        
        Args:
            data: List of dictionaries to classify
            fields: Optional list of specific field names to process.
                If None, processes all fields found in data
            datastats: Optional dictionary of field statistics from Analyzer.
                Keys are field names, values are statistics dictionaries
            confidence: Minimum confidence threshold (0-100) for rule matches.
                Defaults to 95.0
            stop_on_match: If True, stop after first rule match per field.
                Defaults to False
            parse_dates: If True, attempt date/time pattern matching using
                dateparser. Defaults to True
            dateparser: Optional qddate.DateParser instance for date matching.
                Required if parse_dates is True
            limit: Maximum number of records to process per field. Defaults to 1000
            filter_contexts: Optional list of context filters (e.g., ['pii'])
            filter_langs: Optional list of language codes to filter (e.g., ['en'])
            except_empty: If True, exclude empty values from confidence calculations.
                Defaults to True
            ignore_imprecise: If True, ignore rules marked as imprecise.
                Defaults to True
                
        Returns:
            TableScanResult containing classification results for all fields
            
        Raises:
            ValueError: If dateparser is required but not provided
        """
        from metacrafter.reports import validate_options
        validate_options(limit, confidence)
        from metacrafter.classify.stats import DEFAULT_EMPTY_VALUES
        empty_values = DEFAULT_EMPTY_VALUES if empty_values is None else empty_values
        results = TableScanResult()
        data = data[:limit]
        if not fields:
            fields = headers(data)
        field_rules = self.get_filtered_rules(
            RULE_TYPE_FIELD, filter_contexts, filter_langs, ignore_imprecise, include_deprecated
        )
        data_rules = self.get_filtered_rules(
            RULE_TYPE_DATA, filter_contexts, filter_langs, ignore_imprecise, include_deprecated
        )

        data_columns = dict_to_columns(data)
        missing = set(fields) - set(data_columns)
        if missing:
            raise ValueError("Unknown selected fields: " + ", ".join(sorted(missing)))
        nonstr = []
        if datastats:
            for field in datastats.keys():
                if datastats[field]["ftype"] != "str":
                    nonstr.append(field)
        for field in fields:
            m_result = ColumnMatchResult(field=field, matches=[])
            if explain:
                m_result.rejected = []
            shortfield = field.rsplit(".", 1)[-1].strip()
            # Match field name first; a name with a language suffix (title_en) falls back to
            # its base name (title) when the full name matches no field rule.
            field_found: List[Dict[str, Any]] = []
            name_variants = [(shortfield, field)]
            base = strip_language_suffix(shortfield)
            if base != shortfield:
                name_variants.append((base, field[: len(field) - len(shortfield)] + base))
            for short_name, full_name in name_variants:
                for rule in field_rules:
                    if not self._field_rule_matches(rule, short_name, full_name):
                        continue
                    field_found.append(rule)
                    if stop_on_match:
                        break
                if field_found:
                    break
            field_found = self._drop_country_name_variants(field_found, m_result, explain)
            field_hits: Dict[str, float] = {}
            for rule in field_found:
                field_hits[rule["key"]] = max(
                    field_hits.get(rule["key"], 0.0), priority_weight(rule.get("priority"))
                )
                m_result.add(
                    RuleResult(
                        ruleid=rule["id"],
                        confidence=field_score(rule.get("priority"), legacy=legacy_scoring),
                        dataclass=rule["key"],
                        ruletype="field",
                        is_pii=rule_is_pii(rule),
                        pii_source=rule_pii_source(rule),
                        evidence={
                            "scoring_model": "legacy" if legacy_scoring else "v1",
                            "name_score": priority_weight(rule.get("priority")),
                            "rule_source": rule.get("source"),
                        },
                    )
                )
            data = data_columns[field]
            slice = data[0:limit]
            min_len = 0
            max_len = 0
            if datastats:
                # Handle special field types: boolean fields are matched directly,
                # float fields are skipped from further rule matching
                if field in datastats.keys():
                    if datastats[field]["ftype"] == "bool":
                        m_result.add(
                            RuleResult(
                                ruleid="_int_fieldtype_boolean",
                                confidence=100,
                                dataclass="boolean",
                                ruletype="fieldtype",
                            )
                        )
                        results.add(m_result)
                        continue
                    elif datastats[field]["ftype"] == "float":
                        # Float fields are skipped from pattern matching
                        # as they don't benefit from rule-based classification
                        results.add(m_result)
                        continue
                    elif datastats[field]["ftype"] == "datetime":
                        m_result.add(
                            RuleResult(
                                ruleid="_int_fieldtype_datetime",
                                confidence=100,
                                dataclass="datetime",
                                ruletype="fieldtype",
                            )
                        )
                        results.add(m_result)
                        continue
                    elif datastats[field]["ftype"] == "date":
                        m_result.add(
                            RuleResult(
                                ruleid="_int_fieldtype_date",
                                confidence=100,
                                dataclass="date",
                                ruletype="fieldtype",
                            )
                        )
                        results.add(m_result)
                        continue
                    min_len = datastats[field]["minlen"]
                    max_len = datastats[field]["maxlen"]
            if min_len == 0:
                try:
                    min_len = len(min(data, key=len))
                    max_len = len(max(data, key=len))
                except (TypeError, ValueError):
                    min_len = 4
                    max_len = 4

            #            if field not in nonstr:

            if field:
                # Filtering rules by field max and min length of the data
                rules = []
                for rule in data_rules:
                    #                    print('Field %s %d %d, rule %s %d %d' % (field, min_len, max_len, rule['key'], rule['minlen'], rule['maxlen']))
                    if (rule["minlen"] <= min_len <= rule["maxlen"]) or (
                        min_len <= rule["minlen"] <= max_len
                    ):
                        if "f_compiled" in rule.keys():
                            if rule["fieldrulematch"] == "ppr":
                                try:
                                    res = rule["f_compiled"].parseString(shortfield)
                                    rules.append(rule)
                                except ParseException as e:
                                    pass
                            elif rule["fieldrulematch"] == "text":
                                # Cache lowercase conversion
                                if shortfield.lower() in rule["fieldkeywords"]:
                                    rules.append(rule)
                        else:
                            rules.append(rule)
                #                print(field)
                #                for rule in rules:
                #                    print('- %s' %(rule['key']))
                data_found = []
                for rule in rules:
                    name_weight = field_hits.get(rule["key"], 0.0)
                    if rule.get("derived"):
                        detector_weight = detector_name_evidence(rule, base) or detector_name_evidence(rule, shortfield)
                        names_type = bool(name_weight) or detector_names_country_type(rule, shortfield)
                        name_weight = name_weight or detector_weight
                        gate = self._detector_gate(rule, name_weight, names_type)
                        if gate:
                            if explain:
                                m_result.rejected.append({"ruleid": rule["id"], "dataclass": rule["key"], "confidence": 0.0, "matched_count": 0, "considered_count": 0, "reason": gate})
                            continue
                    success = 0
                    pattern_hits = 0
                    empty = 0
                    total = len(slice)
                    for value in slice:
                        if value in empty_values:
                            if except_empty:
                                empty += 1
                            continue
                        text = str(value)
                        slen = len(text)
                        if slen == 0:
                            if except_empty:
                                empty += 1
                            continue
                        if slen < rule["minlen"] or slen > rule["maxlen"]:
                            continue
                        hit, valid = self._value_check(rule, text)
                        if hit:
                            pattern_hits += 1
                        if valid:
                            success += 1
                    considered = total - empty if except_empty else total
                    scored = data_score(
                        matched=success,
                        pattern_hits=pattern_hits,
                        considered=considered,
                        specificity=rule.get("specificity", 1.0),
                        name_weight=name_weight,
                        country_match=self._country_match(rule),
                        draft=rule.get("registry_status") == "draft",
                        legacy=legacy_scoring,
                        has_validator="vfunc" in rule,
                        empty=empty,
                        sampled=total,
                    )
                    result = scored["confidence"]
                    evidence = scored["evidence"]
                    evidence["rule_source"] = rule.get("source")
                    if success and result >= confidence:
                        match = RuleResult(
                            ruleid=rule["id"],
                            confidence=result,
                            dataclass=rule["key"],
                            ruletype=rule.get("ruletype", "data"),
                            evidence=evidence,
                            is_pii=rule_is_pii(rule),
                            pii_source=rule_pii_source(rule),
                        )
                        if rule.get("pattern_id"):
                            match.pattern = rule["pattern_id"]
                        m_result.add(match)
                        data_found.append((rule, match, name_weight))
                        if stop_on_match:
                            break

                    elif explain:
                        m_result.rejected.append({"ruleid": rule["id"], "dataclass": rule["key"], "confidence": result, "matched_count": success, "considered_count": considered, "reason": "No matching values" if not success else "Below confidence threshold"})
                self._drop_shadowed_country_matches(data_found, m_result, explain)

            if m_result.is_empty() and parse_dates and field not in nonstr and dateparser is not None:
                total = len(slice)
                success = 0
                empty = 0
                date_format = None
                for value in slice:
                    if value in empty_values:
                        if except_empty:
                            empty += 1
                        continue
                    if len(str(value)) == 0:
                        if except_empty:
                            empty += 1
                        continue
                    if not isinstance(value, str):
                        continue
                    if not hasattr(dateparser, 'match'):
                        break
                    res = dateparser.match(value, noyear=False)
                    if res:
                        success += 1
                        date_format = res["pattern"]["key"]

                if except_empty:
                    subtotal = total - empty
                    if subtotal == 0:
                        result = 0
                    else:
                        result = success * 100.0 / (total - empty)
                else:
                    result = success * 100.0 / total if total else 0
                if success and result >= confidence:
                    date_match = RuleResult(
                        ruleid="qddate",
                        confidence=result,
                        dataclass="datetime",
                        ruletype="data",
                        format=date_format,
                        evidence={
                            "scoring_model": "legacy" if legacy_scoring else "v1",
                            "matched_count": success,
                            "considered_count": total - empty if except_empty else total,
                            "empty_count": empty,
                        },
                    )
                    date_match.pattern = self.qddate_pattern(date_format)
                    m_result.add(date_match)
            results.add(m_result)
        return results

    # --- matching helpers ---------------------------------------------------

    @staticmethod
    def _field_rule_matches(rule: Dict[str, Any], shortfield: str, field: str) -> bool:
        """Return True when a field rule matches the field name (and its field guard)."""
        match = rule["match"]
        names = (shortfield, field)
        hit = False
        if match == "func":
            hit = any(bool(rule["compiled"](name)) for name in names)
        elif match == "ppr":
            for name in names:
                try:
                    rule["compiled"].parseString(name)
                    hit = True
                    break
                except ParseException:
                    continue
        elif match == "text":
            hit = shortfield.lower() in rule["keywords"] or field.lower() in rule["keywords"]
        elif match in ("regex", "registry"):
            pattern = rule.get("compiled")
            hit = pattern is not None and any(
                pattern.fullmatch(name) or pattern.fullmatch(name.lower()) for name in names
            )
        if not hit:
            return False
        if "f_compiled" in rule:
            if rule.get("fieldrulematch") == "text":
                return shortfield.lower() in rule.get("fieldkeywords", set())
            try:
                rule["f_compiled"].parseString(shortfield)
            except ParseException:
                return False
        return True

    @staticmethod
    def _value_check(rule: Dict[str, Any], text: str):
        """Return ``(pattern_hit, valid)`` for one value: valid = hit and validator passes."""
        match = rule["match"]
        hit = False
        if match == "func":
            try:
                hit = bool(rule["compiled"](text))
            except Exception:  # noqa: BLE001 - a failing plugin does not count as a hit
                hit = False
        elif match == "ppr":
            try:
                rule["compiled"].parseString(text)
                hit = True
            except ParseException:
                hit = False
        elif match in ("regex", "registry"):
            pattern = rule.get("compiled")
            hit = pattern is not None and pattern.fullmatch(text) is not None
        elif match == "text":
            hit = text.lower() in rule["keywords"]
        if not hit:
            return False, False
        vfunc = rule.get("vfunc")
        if vfunc is None:
            return True, True
        try:
            return True, bool(vfunc(text))
        except Exception:  # noqa: BLE001 - validator errors fail the value
            return True, False

    def _country_match(self, rule: Dict[str, Any]) -> Optional[bool]:
        """Country evidence for registry-derived detectors: True, False or None (unknown)."""
        countries = rule.get("registry_country")
        if not countries or not self.preset_countries:
            return None
        wanted = {c.upper() for c in self.preset_countries}
        return bool(wanted & {c.upper() for c in countries})

    @staticmethod
    def _rule_countries(rule: Dict[str, Any]) -> Set[str]:
        """Countries a rule is specific to; empty for universal rules (``country_code: xx``)."""
        codes = rule.get("country_code") or rule.get("registry_country") or []
        codes = {str(code).lower() for code in codes if code}
        return set() if UNIVERSAL_COUNTRY in codes else codes

    def _detector_gate(self, rule: Dict[str, Any], name_weight: float, names_type: bool = False) -> Optional[str]:
        """Reason a registry-derived detector may not report a match on this field, or None.

        A detector with a generic regexp (it accepts a large share of ordinary tokens) needs
        field-name evidence for its datatype or its parent (``name_weight``). A detector for
        a country-specific datatype needs the field name to name the datatype itself
        (``names_type``), unless the scan country matches, or is unknown and the record has
        a checksum.
        """
        if rule.get("needs_name_evidence") and not name_weight:
            return "Generic pattern without field-name evidence"
        if rule.get("registry_country") and not names_type:
            country = self._country_match(rule)
            if country is False:
                return "Country-specific type outside the scan countries"
            if country is None and "vfunc" not in rule:
                return "Country-specific type without country or field-name evidence"
        return None

    def _drop_country_name_variants(
        self, found: List[Dict[str, Any]], m_result: ColumnMatchResult, explain: bool
    ) -> List[Dict[str, Any]]:
        """Drop country-specific field-name matches that other name matches already cover.

        Applies when no scan country is set. A country-specific rule (its rule file has a
        country other than ``xx``) is dropped when a universal rule matched the field with
        at least its priority weight, or when any rule matched with a higher weight: a field
        ``city`` matched by a universal rule does not also report ``egcity`` or ``trcity``.
        """
        if self.preset_countries or len(found) < 2:
            return found
        best_any = max(priority_weight(r.get("priority")) for r in found)
        universal = [priority_weight(r.get("priority")) for r in found if not self._rule_countries(r)]
        best_universal = max(universal) if universal else None
        kept = []
        for rule in found:
            weight = priority_weight(rule.get("priority"))
            if self._rule_countries(rule) and (
                weight < best_any or (best_universal is not None and weight <= best_universal)
            ):
                if explain:
                    m_result.rejected.append({"ruleid": rule["id"], "dataclass": rule["key"], "confidence": 0.0, "matched_count": 0, "considered_count": 0, "reason": "Country-specific field name covered by a universal rule"})
                continue
            kept.append(rule)
        return kept

    def _drop_shadowed_country_matches(self, found, m_result: ColumnMatchResult, explain: bool) -> None:
        """Drop country-specific value matches that only repeat a universal match.

        Applies when no scan country is set, to curated rules without a validator and
        without field-name evidence: ``rufias`` (a GUID pattern) is dropped when ``uuid``
        matched the same values with at least the same confidence.
        """
        if self.preset_countries or not found:
            return
        universal = [match.confidence for rule, match, _ in found if not self._rule_countries(rule)]
        if not universal:
            return
        best = max(universal)
        for rule, match, name_weight in found:
            if (
                rule.get("derived")
                or name_weight
                or not self._rule_countries(rule)
                or rule.get("match") == "func"
                or "vfunc" in rule
                or match.confidence > best
            ):
                continue
            m_result.matches.remove(match)
            if explain:
                m_result.rejected.append({"ruleid": rule["id"], "dataclass": rule["key"], "confidence": match.confidence, "matched_count": match.evidence.get("matched_count", 0) if match.evidence else 0, "considered_count": match.evidence.get("considered_count", 0) if match.evidence else 0, "reason": "Country-specific pattern repeats a universal match"})

    def qddate_pattern(self, date_format: Optional[str]) -> Optional[str]:
        """Registry pattern record id for a qddate format key, via the registry crosswalk."""
        if not date_format or self.registry is None:
            return None
        return self.registry.qddate_pattern(date_format)
