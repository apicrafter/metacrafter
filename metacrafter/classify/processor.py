"""Rules processor module for classifying and matching data types."""
import glob
import itertools
import os
import importlib
import ast
import re
import logging
import math
import warnings
from collections import Counter
from functools import lru_cache
from pathlib import Path
from typing import List, Dict, Any, Iterable, Optional, Union, Set, Callable, Tuple

import yaml

from metacrafter.engine.background import check_broadness
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

from .utils import (
    dict_to_columns,
    headers,
    resolve_field_names,
    sparse_map_parents,
    strip_language_suffix,
    unescape_field_path,
    whole_number_texts,
)


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

    def rank(self) -> None:
        """Order matches by confidence, then field-name evidence, then rule order.

        The first match is the field's primary datatype (``datatype_url``). Confidence is
        clamped at 100, so a match whose datatype the field name names can tie with a
        value-only match: ``okpd`` (field ``okpd2``) and ``ipv4`` both reach 100 on OKPD2
        codes, and the name decides.
        """
        self.matches.sort(key=match_rank_key)

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


def match_name_score(match: "RuleResult") -> float:
    """Field-name evidence of a match (``evidence.name_score``), 0 when it has none."""
    evidence = getattr(match, "evidence", None) or {}
    try:
        return float(evidence.get("name_score") or 0.0)
    except (TypeError, ValueError):
        return 0.0


def match_rank_key(match: "RuleResult") -> Tuple[float, float]:
    """Sort key: higher confidence first, then higher field-name evidence; stable otherwise."""
    return (-float(match.confidence or 0.0), -match_name_score(match))


#: A time of day after a date: "2016-01-01 10:00", "2024-01-01T10:00:00" (not "(бывш. 25.01.2008)").
_TIME_AFTER_DATE = re.compile(r"^[T\s,]*\d{1,2}:\d{2}")

#: A character class with a range of letters ([A-Z0-9], [0-9B-DF-HJ-NP-Z]) in a regexp.
_LETTER_CLASS = re.compile(r"\[[^\]]*[A-Za-z]-[A-Za-z][^\]]*\]")

#: Distinct values of a field that the value-fit checks of a field-name match look at.
FIT_SAMPLE = 200
#: Datatypes of field-name matches that say only that a field holds text for people: the
#: object a field belongs to decides what the text names (see _drop_generic_name_matches).
GENERIC_TEXT_TYPES = frozenset({"name", "description", "title"})
#: A short lower-case code (``wp``, ``en``, ``x1``), not text for people.
_SHORT_CODE = re.compile(r"[a-z][a-z0-9_-]{0,3}")
#: Values of a yes/no column that are no answer, besides the empty values of a scan.
NO_ANSWER_VALUES = frozenset({"-", "\u2013", "\u2014", "?", "n/a", "na", "n.a.", "nd", "n.d.", "null", "none", "nan", "nil"})
#: GeoJSON geometry types whose coordinates are positions of single points.
POINT_GEOMETRIES = frozenset({"point", "multipoint"})
#: GeoJSON geometry types whose coordinates are the vertices of a line or a polygon.
SHAPE_GEOMETRIES = frozenset({"linestring", "multilinestring", "polygon", "multipolygon"})


def geometry_kinds(field: str, columns: Dict[str, List[Any]]) -> Optional[Tuple[int, int]]:
    """``(lines and polygons, points)``: counts of the GeoJSON geometry types in the ``type``
    field beside a ``coordinates`` field; None for other fields or without such types."""
    parent, last = split_field_path(field)
    if unescape_field_path(last) != "coordinates":
        return None
    kinds = Counter(v.strip().lower() for v in columns.get(f"{parent}.type" if parent else "type", [])
                    if isinstance(v, str))
    shapes = sum(n for kind, n in kinds.items() if kind in SHAPE_GEOMETRIES)
    points = sum(n for kind, n in kinds.items() if kind in POINT_GEOMETRIES)
    return (shapes, points) if shapes + points else None


def fit_texts(values: List[Any], empty_values: Any) -> Counter:
    """Counts of the texts of non-empty values, as value rules see them (whole floats as
    integers: ``2023.0`` is ``2023``)."""
    texts: Counter = Counter()
    for value in values:
        if value is None or isinstance(value, bool) or value in empty_values:
            continue
        if isinstance(value, float):
            if math.isnan(value):
                continue
            text = str(int(value)) if value.is_integer() else str(value)
        else:
            text = str(value).strip()
        if text:
            texts[text] += 1
    return texts


def split_field_path(field: str) -> Tuple[str, str]:
    """``(parent path, last name)`` of an escaped field path (``a.b\\.c`` -> ``("a", "b\\.c")``)."""
    last, i = -1, 0
    while i < len(field):
        if field[i] == "\\":
            i += 2
            continue
        if field[i] == ".":
            last = i
        i += 1
    return (field[:last], field[last + 1:]) if last >= 0 else ("", field)


def date_pattern_match(dateparser: Any, value: str) -> Optional[Tuple[str, bool]]:
    """``(qddate pattern key, has_time)`` of the date that starts ``value``, or None.

    qddate reads a date at the start of a text and ignores what follows (its
    ``:t_right`` patterns), so ``18-2-023909`` read as 18 February 0239 with ``09``
    left over. A date must end the value or be followed by a character that is
    neither a letter nor a digit, and its values must form a date. ``has_time`` is
    True when a time of day follows the date (``2016-01-01 00:00:00``): the value is a
    date and time, not a date. A note after the date (``30.09.2016 (бывш. 25.01.2008)``)
    is not a time. qddate reports a time it read as a ``:time_N`` suffix of the key.
    """
    match_typed = getattr(dateparser, "match_typed", None)
    if match_typed is None:
        res = dateparser.match(value, noyear=False)
        if not res:
            return None
        return res["pattern"]["key"], bool(re.search(r"\d{1,2}:\d{2}", value))
    found = match_typed(value, allow_no_year=False)
    if found is None:
        return None
    end = found.span[1]
    if end < len(value) and value[end].isalnum():
        return None
    # match_typed reads a time after the date too (dt:date:date_9:time_1).
    return found.pattern_key, ":time" in found.pattern_key or bool(_TIME_AFTER_DATE.match(value[end:]))


def date_pattern_key(dateparser: Any, value: str) -> Optional[str]:
    """qddate pattern key of the date that starts ``value``, or None (see date_pattern_match)."""
    found = date_pattern_match(dateparser, value)
    return found[0] if found else None


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
        # Value-fit checks of field-name matches: broadness by check id, registry checks by datatype.
        self._fit_broadness: Dict[str, float] = {}
        self._registry_fit: Optional[Dict[str, List[Dict[str, Any]]]] = None
        self._registry_patterns: Dict[str, List[Dict[str, Any]]] = {}
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
            # A field rule that needs non-empty values (see _drop_unfit_field_rules); metacrafter
            # 2.1 and earlier ignore the attribute.
            rule["needs_values"] = rule.get("needs_values") is True
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
            if "fieldregex" in rule.keys():
                # A field-name guard by regular expression, searched in the short field name
                # without regard to case. Metacrafter versions before it ignore the attribute.
                try:
                    if not isinstance(rule["fieldregex"], str) or not rule["fieldregex"]:
                        raise re.error("empty pattern")
                    rule["f_regex"] = re.compile(rule["fieldregex"], re.IGNORECASE)
                except re.error as e:
                    logging.error(f"Failed to compile field regex of rule '{rulekey}': {e}. Skipping this rule.")
                    self.diagnostics.append({"ruleid": rulekey, "source": filename, "error": "Required field guard unavailable"})
                    continue
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

    def bind_registry(self, adapter, detectors: str = "checksum") -> int:
        """Attach registry data: compile ``match: registry`` rules and add derived detectors.

        Returns the number of registry-derived detectors added.
        """
        from metacrafter.engine.registry_detectors import add_registry_detectors, bind_registry_rules

        self.registry = adapter
        self._registry_fit = None
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
        lookahead: Optional[List[Dict[str, Any]]] = None,
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
            lookahead: Records of the source read after ``data`` (the head of a file). They
                are not classified; a ``boolean`` match looks at their values for a third
                answer (see ``_drop_contradicted_matches``), and field rules with
                ``needs_values: true`` look at them when the sample has no values
                (``_drop_unfit_field_rules``).

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
        fields = resolve_field_names(fields, data_columns)
        # Nested objects whose keys are data (the words of an inverted index): their keys are
        # not field names, so field-name rules and field guards do not look at them.
        map_parents = sparse_map_parents(data)
        lookahead_columns: Dict[str, List[Any]] = {}

        def lookahead_values(name: str) -> List[Any]:
            """Values of a field in the records read after the sample."""
            if not lookahead:
                return []
            if not lookahead_columns:
                lookahead_columns.update(dict_to_columns(lookahead))
            return lookahead_columns.get(name, [])
        # Value rules by datatype, imprecise ones included: with a field name for their type
        # they check whether the values fit it (see field_score).
        fit_rules: Dict[str, List[Dict[str, Any]]] = {}
        if not legacy_scoring:
            for rule in self.get_filtered_rules(RULE_TYPE_DATA, filter_contexts, filter_langs, False, include_deprecated):
                fit_rules.setdefault(rule["key"], []).append(rule)
        nonstr = []
        if datastats:
            for field in datastats.keys():
                if datastats[field]["ftype"] != "str":
                    nonstr.append(field)
        for field in fields:
            m_result = ColumnMatchResult(field=field, matches=[])
            if explain:
                m_result.rejected = []
            # Names are compared without the escapes of nested paths: a CSV header
            # "Имеет привязку к физ. лицам" is the column "…физ\. лицам". The short name is
            # the part after the last dot, escaped or not, so a flattened header
            # "info.okoguCode" still matches a rule for "okoguCode".
            name = unescape_field_path(field)
            shortfield = name.rsplit(".", 1)[-1].strip()
            # Match field name first; a name with a language suffix (title_en) or a language
            # key of a nested object (title.en) falls back to its base name (title) when the
            # full name matches no field rule.
            field_found: List[Dict[str, Any]] = []
            name_variants = [(shortfield, name)]
            base = shortfield
            base_field = strip_language_suffix(name.strip())
            base_name = base_field.rsplit(".", 1)[-1].strip()
            if base_field != name.strip() and base_name:
                base = base_name
                name_variants.append((base, base_field))
            name_is_data = bool(map_parents) and split_field_path(field)[0] in map_parents
            if name_is_data:
                name_variants = []
            for short_name, full_name in name_variants:
                for rule in field_rules:
                    if not self._field_rule_matches(rule, short_name, full_name):
                        continue
                    field_found.append(rule)
                    if stop_on_match:
                        break
                if field_found:
                    break
            data = data_columns[field]
            slice = data[0:limit]
            # Name rules whose values do not fit go first, so that they do not cover the
            # country-specific rules (provincia of codes: it_province_code, not province).
            field_found = self._drop_unfit_field_rules(field_found, slice, empty_values, m_result, explain,
                                                       more_values=lambda: lookahead_values(field))
            field_found = self._drop_country_name_variants(field_found, m_result, explain)
            field_hits: Dict[str, float] = {}
            texts = fit_texts(slice, empty_values) if field_found and not legacy_scoring else None
            for rule in field_found:
                field_hits[rule["key"]] = max(
                    field_hits.get(rule["key"], 0.0), priority_weight(rule.get("priority"))
                )
                fit = self._value_fit(rule, texts, fit_rules) if texts else None
                evidence = {
                    "scoring_model": "legacy" if legacy_scoring else "v1",
                    "name_score": priority_weight(rule.get("priority")),
                    "rule_source": rule.get("source"),
                }
                if fit:
                    evidence["value_fit"] = fit
                m_result.add(
                    RuleResult(
                        ruleid=rule["id"],
                        confidence=field_score(rule.get("priority"), legacy=legacy_scoring, fit=fit),
                        dataclass=rule["key"],
                        ruletype="field",
                        is_pii=rule_is_pii(rule),
                        pii_source=rule_pii_source(rule),
                        evidence=evidence,
                    )
                )
            # Before the float columns leave the loop: coordinates are floats.
            self._drop_unfit_geopoints(field, data_columns, slice, empty_values, m_result, explain)
            self._add_geometry_match(field, data_columns, m_result)
            min_len = 0
            max_len = 0
            if datastats:
                # Handle special field types: boolean fields are matched directly,
                # float fields are skipped from further rule matching
                if field in datastats.keys():
                    if datastats[field]["ftype"] == "bool":
                        # A column of JSON booleans holds yes/no flags: a field name such as
                        # status or type does not make it a category (#60 of metacrafter-rules).
                        for match in [m for m in m_result.matches if m.ruletype == "field" and m.dataclass != "boolean"]:
                            m_result.matches.remove(match)
                            if explain:
                                m_result.rejected.append({"ruleid": match.ruleid, "dataclass": match.dataclass, "confidence": match.confidence, "matched_count": 0, "considered_count": 0, "reason": "Boolean values"})
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
                        # Float fields are skipped from pattern matching as they don't
                        # benefit from rule-based classification, unless every value is a
                        # whole number: identifiers read as floats (.xls, pandas with NaN)
                        # are matched as their integer text.
                        whole = whole_number_texts(slice, empty_values)
                        if whole is None:
                            results.add(m_result)
                            continue
                        slice = data = whole
                        lengths = [len(text) for text in whole if text]
                        min_len, max_len = min(lengths), max(lengths)
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
                    if not min_len:
                        min_len = datastats[field]["minlen"]
                        max_len = datastats[field]["maxlen"]
            if min_len == 0:
                # Lengths of the values rules will see: a column with empty values (a
                # record without the nested object, an empty array) must not narrow the
                # length prefilter, or every rule whose length range excludes the
                # placeholder is dropped.
                lengths = [len(str(value)) for value in data if value not in empty_values]
                lengths = [length for length in lengths if length]
                if lengths:
                    min_len, max_len = min(lengths), max(lengths)

            #            if field not in nonstr:

            if field:
                # Filtering rules by field max and min length of the data
                rules = []
                for rule in data_rules:
                    #                    print('Field %s %d %d, rule %s %d %d' % (field, min_len, max_len, rule['key'], rule['minlen'], rule['maxlen']))
                    if (rule["minlen"] <= min_len <= rule["maxlen"]) or (
                        min_len <= rule["minlen"] <= max_len
                    ):
                        if not self._has_field_guard(rule) or (
                            not name_is_data and self._field_guard_passes(rule, shortfield)
                        ):
                            rules.append(rule)
                #                print(field)
                #                for rule in rules:
                #                    print('- %s' %(rule['key']))
                data_found = []
                for rule in rules:
                    name_weight = field_hits.get(rule["key"], 0.0)
                    if rule.get("derived"):
                        detector_weight = 0.0 if name_is_data else (
                            detector_name_evidence(rule, base) or detector_name_evidence(rule, shortfield))
                        names_type = bool(name_weight) or (
                            not name_is_data and detector_names_country_type(rule, shortfield))
                        name_weight = name_weight or detector_weight
                        gate = self._detector_gate(rule, name_weight, names_type)
                        if gate:
                            if explain:
                                m_result.rejected.append({"ruleid": rule["id"], "dataclass": rule["key"], "confidence": 0.0, "matched_count": 0, "considered_count": 0, "reason": gate})
                            continue
                    success = 0
                    pattern_hits = 0
                    letter_hits = 0
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
                            if not text.isdigit():
                                letter_hits += 1
                    if success and not letter_hits and self._digits_only_for_letters(rule, name_weight):
                        if explain:
                            m_result.rejected.append({"ruleid": rule["id"], "dataclass": rule["key"], "confidence": 0.0, "matched_count": success, "considered_count": total, "reason": "Digits only for a country-specific format with letters"})
                        continue
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
                self._drop_unnamed_detectors(data_found, m_result, explain)

            # Date patterns run when no value rule matched. A field-name match alone
            # (``created``, ``birth_date``) does not skip them: the values then add evidence.
            only_name_matches = all(match.ruletype == "field" for match in m_result.matches)
            if only_name_matches and parse_dates and field not in nonstr and dateparser is not None:
                total = len(slice)
                success = 0
                empty = 0
                date_format = None
                with_time = 0
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
                    found_date = date_pattern_match(dateparser, value)
                    if found_date:
                        success += 1
                        date_format = found_date[0]
                        with_time += found_date[1]

                if except_empty:
                    subtotal = total - empty
                    if subtotal == 0:
                        result = 0
                    else:
                        result = success * 100.0 / (total - empty)
                else:
                    result = success * 100.0 / total if total else 0
                if success and result >= confidence:
                    # qddate patterns are dates: the field is a datetime only when the values
                    # carry a time after the date (#55 of metacrafter-rules).
                    date_match = RuleResult(
                        ruleid="qddate",
                        confidence=result,
                        dataclass="datetime" if with_time else "date",
                        ruletype="data",
                        format=date_format,
                        evidence={
                            "scoring_model": "legacy" if legacy_scoring else "v1",
                            "matched_count": success,
                            "considered_count": total - empty if except_empty else total,
                            "empty_count": empty,
                        },
                    )
                    if not with_time:
                        date_match.pattern = self.qddate_pattern(date_format)
                    m_result.add(date_match)
            more_values: List[Any] = []
            if lookahead and any(m.dataclass == "boolean" and m.ruletype == "data" for m in m_result.matches):
                more_values = lookahead_values(field)
            self._drop_contradicted_matches(m_result, slice, empty_values, explain, more_values)
            self._drop_generic_name_matches(m_result, slice, empty_values, explain)
            self._drop_unfit_geopoints(field, data_columns, slice, empty_values, m_result, explain)
            results.add(m_result)
        return results

    #: Values that a yes/no column may hold besides empty ones: two answers and a placeholder.
    MAX_BOOLEAN_VALUES = 3

    def _drop_contradicted_matches(
        self, m_result: ColumnMatchResult, values: List[Any], empty_values: Any, explain: bool,
        more_values: Iterable[Any] = (),
    ) -> None:
        """Drop matches that the values of the field contradict.

        - A ``date`` match by field name when a value rule found dates with a time of day
          (``datetime``): a WordPress ``date`` holds ``2024-01-01T10:00:00``.
        - A ``boolean`` match by values when the field has more distinct answers than a
          yes/no column (``да``/``не``/``кран``/``бутон`` is a category), or three answers that
          each occur more than once, one of which no matched boolean rule accepts
          (``Да``/``Нет``/``Частично``). Placeholders (``-``, ``n/a``, ``?``) are no answer.
          ``more_values`` are values of the field read after the sample (see
          ``match_dict(lookahead=...)``): a third answer may first appear below the head of a
          file. Two answers stay a yes/no column even when the matched rule knows one of them
          only (``SI``/``NO`` by an English yes/no rule, ``да``/``не``).
        - A ``name`` match by field name when every value is a number: a KML Placemark
          ``name`` that holds district numbers (17, 18).
        """
        drop = []
        if any(m.ruletype == "field" and m.dataclass == "name" for m in m_result.matches):
            texts = [str(v).strip() for v in values if v not in empty_values and str(v).strip()]
            if texts and all(t.replace(".", "", 1).isdigit() for t in texts):
                drop += [m for m in m_result.matches if m.ruletype == "field" and m.dataclass == "name"]
        value_types = {m.dataclass for m in m_result.matches if m.ruletype != "field"}
        if "datetime" in value_types:
            drop += [m for m in m_result.matches if m.ruletype == "field" and m.dataclass == "date"]
        booleans = [m for m in m_result.matches if m.dataclass == "boolean" and m.ruletype == "data"]
        if booleans:
            answers: Counter = Counter()
            texts: Dict[str, str] = {}
            for value in itertools.chain(values, more_values):
                if value is None or value in empty_values:
                    continue
                text = str(value).strip()
                key = text.lower()
                if key and key not in NO_ANSWER_VALUES:
                    answers[key] += 1
                    texts.setdefault(key, text)
            if len(answers) > self.MAX_BOOLEAN_VALUES:
                drop += booleans
            else:
                # Three answers, each given more than once (a footnoted ``False [1]`` is not one),
                # one of which the matched yes/no rules do not accept.
                frequent = [key for key, count in answers.items() if count > 1]
                if len(frequent) >= 3:
                    ids = {m.ruleid for m in booleans}
                    rules = [rule for rule in self.data_rules if rule.get("id") in ids]
                    if any(self._third_answer(texts[key], rules) for key in frequent):
                        drop += booleans
        for match in drop:
            if match in m_result.matches:
                m_result.matches.remove(match)
                if explain:
                    m_result.rejected.append({"ruleid": match.ruleid, "dataclass": match.dataclass, "confidence": match.confidence, "matched_count": 0, "considered_count": 0, "reason": "Contradicted by the values of the field"})

    def _drop_generic_name_matches(
        self, m_result: ColumnMatchResult, values: List[Any], empty_values: Any, explain: bool
    ) -> None:
        """Drop field-name matches of generic text types (``name``, ``description``, ``title``)
        that the field's values contradict.

        A ``name`` key holds the name of whatever its object is: the ``name`` of a GBIF
        ``usage`` is a scientific name, of a HAL ``curies`` link a CURIE prefix. The generic
        match is dropped when:

        - a value rule of another type matched with at least its confidence (``species`` 100
          on ``classifications.*.usage.name``, ``encountryname`` on ``gadm.level0.name``);
        - every non-empty value is a short lower-case code (``wp``, ``en``): text for people
          is not written so.
        """
        generic = [m for m in m_result.matches if m.ruletype == "field" and m.dataclass in GENERIC_TEXT_TYPES]
        if not generic:
            return
        best = max(float(m.confidence or 0.0) for m in generic)
        reason = None
        if any(m.ruletype != "field" and m.dataclass not in GENERIC_TEXT_TYPES and float(m.confidence or 0.0) >= best
               for m in m_result.matches):
            reason = "A value rule of a more specific type matched the field"
        else:
            texts = [str(v).strip() for v in values if v is not None and v not in empty_values and str(v).strip()]
            if texts and all(_SHORT_CODE.fullmatch(t) for t in texts):
                reason = "Every value is a short lower-case code"
        if reason is None:
            return
        for match in generic:
            m_result.matches.remove(match)
            if explain:
                m_result.rejected.append({"ruleid": match.ruleid, "dataclass": match.dataclass, "confidence": match.confidence, "matched_count": 0, "considered_count": 0, "reason": reason})

    @staticmethod
    def _third_answer(text: str, rules: List[Dict[str, Any]]) -> bool:
        """True when ``text`` is an answer that none of the boolean ``rules`` accepts: it has a
        letter or a digit and fails each rule's length range, pattern and validator."""
        if not any(ch.isalnum() for ch in text):
            return False
        for rule in rules:
            if rule["minlen"] <= len(text) <= rule["maxlen"] and RulesProcessor._value_check(rule, text)[1]:
                return False
        return bool(rules)

    def _drop_unfit_geopoints(
        self, field: str, columns: Dict[str, List[Any]], values: List[Any], empty_values: Any,
        m_result: ColumnMatchResult, explain: bool,
    ) -> None:
        """Drop ``geopoint`` matches on coordinates that are not a longitude / latitude point.

        - The ``coordinates`` of GeoJSON geometries whose ``type`` beside them is mostly a line
          or a polygon are the vertices of shapes (``geometry.coordinates`` of district
          boundaries and bicycle paths), not points. A layer of points with a few building
          outlines (OpenStreetMap exports) keeps its label.
        - Coordinates of which most numbers lie outside -180..180 are projected (metres of a
          national grid, Web Mercator): a point, but not in degrees.
        """
        found = [m for m in m_result.matches if m.dataclass == "geopoint"]
        if not found:
            return
        reason = None
        kinds = geometry_kinds(field, columns)
        if kinds is not None and kinds[0] * 2 > sum(kinds):
            reason = "Coordinates of lines or polygons, not of points"
        if reason is None:
            numbers = []
            for value in values:
                if value is None or isinstance(value, bool) or value in empty_values:
                    continue
                try:
                    number = float(value)
                except (TypeError, ValueError):
                    continue
                if not math.isnan(number):
                    numbers.append(number)
            if numbers and sum(abs(n) > 180 for n in numbers) * 2 > len(numbers):
                reason = "Projected coordinates, outside the longitude / latitude range"
        if reason is None:
            return
        for match in found:
            m_result.matches.remove(match)
            if explain:
                m_result.rejected.append({"ruleid": match.ruleid, "dataclass": match.dataclass, "confidence": match.confidence, "matched_count": 0, "considered_count": 0, "reason": reason})

    @staticmethod
    def _add_geometry_match(field: str, columns: Dict[str, List[Any]], m_result: ColumnMatchResult) -> None:
        """Label the ``coordinates`` of GeoJSON lines and polygons ``geojson``.

        The ``coordinates`` of a geometry whose ``type`` beside it is mostly a line or a
        polygon (``geometry.coordinates`` of district boundaries, bicycle paths; in degrees or
        projected) are the vertices of the geometry, read as one list of numbers: the field
        is the geometry, not a point (``_drop_unfit_geopoints`` drops ``geopoint`` there).
        Points keep their ``geopoint`` label; projected points get none (their numbers are an
        easting and a northing together). The confidence is the share of lines and polygons
        among the geometries.
        """
        kinds = geometry_kinds(field, columns)
        if kinds is None or kinds[0] * 2 <= sum(kinds):
            return
        shapes, points = kinds
        m_result.add(
            RuleResult(
                ruleid="_int_geometry_coordinates",
                confidence=round(100.0 * shapes / (shapes + points), 2),
                dataclass="geojson",
                ruletype="fieldtype",
                evidence={"matched_count": shapes, "considered_count": shapes + points},
            )
        )

    # --- value fit of field-name matches ------------------------------------

    def _fit_check(self, check_id: str, test: Callable[[str], bool], dictionary: bool) -> Dict[str, Any]:
        """A value check with its background broadness (computed once per check id)."""
        broadness = self._fit_broadness.get(check_id)
        if broadness is None:
            broadness = check_broadness(test)[0]
            self._fit_broadness[check_id] = broadness
        return {"id": check_id, "test": test, "dictionary": dictionary, "broadness": broadness}

    def _rule_fit_check(self, rule: Dict[str, Any]) -> Dict[str, Any]:
        minlen, maxlen = rule["minlen"], rule["maxlen"]

        def test(text: str) -> bool:
            return minlen <= len(text) <= maxlen and self._value_check(rule, text)[1]

        return self._fit_check(rule["id"], test, dictionary=rule["match"] == "text")

    def _registry_fit_checks(self, key: str) -> List[Dict[str, Any]]:
        """Checks of datatype ``key`` from the registry: the regexps of its record and of the
        pattern records of the datatype (``semantic_type``), each with its checksum."""
        adapter = self.registry
        if adapter is None or not getattr(adapter, "available", False):
            return []
        if self._registry_fit is None:
            self._registry_fit = {}
            patterns: Dict[str, List[Dict[str, Any]]] = {}
            for record in adapter.registry:
                if (record.get("kind") == "pattern" and record.get("semantic_type") and record.get("regexp")
                        and record.get("status") != "deprecated"):
                    target = adapter.resolve(record["semantic_type"]).canonical_id or record["semantic_type"]
                    patterns.setdefault(target, []).append(record)
            self._registry_patterns = patterns
        canonical = adapter.resolve(key).canonical_id or key
        if canonical in self._registry_fit:
            return self._registry_fit[canonical]
        from metacrafter.engine.checksums import checksum_validator

        record = adapter.record(canonical)
        records = [record] if record and record.get("regexp") and record.get("status") != "deprecated" else []
        checks = []
        for record in records + self._registry_patterns.get(canonical, []):
            try:
                compiled = re.compile(record["regexp"])
            except re.error:
                continue
            vfunc = checksum_validator("checksum:" + record["checksum"]) if record.get("checksum") else None

            def test(text: str, compiled=compiled, vfunc=vfunc) -> bool:
                return compiled.fullmatch(text) is not None and (vfunc is None or bool(vfunc(text)))

            checks.append(self._fit_check(f"registry:{record['id']}", test, dictionary=False))
        self._registry_fit[canonical] = checks
        return checks

    def _value_fit(self, rule: Dict[str, Any], texts: Counter, fit_rules: Dict[str, List[Dict[str, Any]]]
                   ) -> Optional[Dict[str, Any]]:
        """How well the values fit the datatype of a field-name match (see ``field_score``).

        The checks are the value rules of the rule's key, its registry regexps and the
        validator of the field rule. None when the datatype has no checks.
        """
        checks = [self._rule_fit_check(r) for r in fit_rules.get(rule["key"], ())]
        checks += self._registry_fit_checks(rule["key"])
        if "vfunc" in rule:
            vfunc = rule["vfunc"]
            checks.append(self._fit_check(f"{rule['id']}:validator", lambda text: bool(vfunc(text)), dictionary=False))
        if not checks or not texts:
            return None
        sample = list(texts.items())[:FIT_SAMPLE]
        considered = sum(n for _, n in sample)
        passed_shape = [False] * len(sample)
        best = (-1.0, None, 0.0)
        shape_broadness = None
        for check in checks:
            hits = 0
            for i, (text, n) in enumerate(sample):
                try:
                    ok = check["test"](text)
                except Exception:  # noqa: BLE001 - a failing check rejects the value
                    ok = False
                if ok:
                    hits += n
                    if not check["dictionary"]:
                        passed_shape[i] = True
            rate = hits / considered
            support = rate * (1.0 - check["broadness"])
            if support > best[0]:
                best = (support, check, rate)
            if not check["dictionary"]:
                shape_broadness = max(shape_broadness or 0.0, check["broadness"])
        fit = {
            "considered_count": considered,
            "checks": len(checks),
            "support": round(best[0], 4),
            "misfit": 0.0,
            "check": best[1]["id"],
            "rate": round(best[2], 4),
            "broadness": best[1]["broadness"],
        }
        if shape_broadness is not None:
            shape_rate = sum(n for (_, n), ok in zip(sample, passed_shape) if ok) / considered
            fit.update(misfit=round((1.0 - shape_rate) * shape_broadness, 4), shape_rate=round(shape_rate, 4),
                       shape_broadness=shape_broadness)
        return fit

    def _drop_unfit_field_rules(
        self, found: List[Dict[str, Any]], values: List[Any], empty_values: Any, m_result: ColumnMatchResult,
        explain: bool, more_values: Optional[Callable[[], List[Any]]] = None,
    ) -> List[Dict[str, Any]]:
        """Field-name rules whose values fit them: a field rule's ``validator`` checks the values.

        A field rule may name a validator that its values must pass: an organization name or
        a city has letters (``ORGANIZATION`` that holds the codes 1, 2, 5 is not one, nor a
        ``city`` of numeric city ids), and a license that is a URL is not a software license
        (metacrafter-rules #60). The match is dropped when fewer than half of the non-empty
        sampled values pass. A field without non-empty sampled values keeps its name matches
        (an empty ``Rechtsvorm`` column is still ``legal_form``), except those of rules with
        ``needs_values: true``: the name alone does not say what the values are (an empty
        ``created_at`` column is not labelled by a rule that asks for Unix milliseconds). Such
        a rule looks at the records read after the sample (``more_values``): it is kept when
        they have values and at least half pass its validator.
        Rules without a validator keep their match, except country-specific rules of the same
        type as a universal rule that the values failed: the values do not fit that type in
        any country. Country rules of other types stay (a ``provincia`` of codes is
        ``it_province_code``, not ``province``).
        """
        if not any("vfunc" in rule or rule.get("needs_values") for rule in found):
            return found
        texts = [str(v).strip() for v in values if v not in empty_values and v is not None]
        texts = [t for t in texts if t]
        if not texts:
            # No values to veto by. Rules that need values look at the records after the sample.
            if not any(rule.get("needs_values") for rule in found):
                return found
            later = [str(v).strip() for v in (more_values() if more_values is not None else ())
                     if v not in empty_values and v is not None]
            later = [t for t in later if t]
            kept = []
            for rule in found:
                if rule.get("needs_values") and not (later and self._validator_passes(rule, later)):
                    if explain:
                        reason = ("The values do not fit the field-name rule" if later
                                  else "The field-name rule needs values and the field has none")
                        m_result.rejected.append({"ruleid": rule["id"], "dataclass": rule["key"], "confidence": 0.0, "matched_count": 0, "considered_count": len(later), "reason": reason})
                    continue
                kept.append(rule)
            return kept
        kept, unfit = [], set()
        for rule in found:
            if "vfunc" in rule:
                passed = 0
                for text in texts:
                    try:
                        passed += bool(rule["vfunc"](text))
                    except Exception:  # noqa: BLE001 - a failing validator rejects the value
                        pass
                if passed * 2 < len(texts):
                    if not self._rule_countries(rule):
                        unfit.add(rule["key"])
                    if explain:
                        m_result.rejected.append({"ruleid": rule["id"], "dataclass": rule["key"], "confidence": 0.0, "matched_count": passed, "considered_count": len(texts), "reason": "Values do not fit the field-name rule"})
                    continue
            kept.append(rule)
        out = []
        for rule in kept:
            if rule["key"] in unfit and "vfunc" not in rule and self._rule_countries(rule):
                if explain:
                    m_result.rejected.append({"ruleid": rule["id"], "dataclass": rule["key"], "confidence": 0.0, "matched_count": 0, "considered_count": len(texts), "reason": "Values do not fit the field-name rule of this type"})
                continue
            out.append(rule)
        return out

    @staticmethod
    def _validator_passes(rule: Dict[str, Any], texts: List[str]) -> bool:
        """True when the rule has no validator or at least half of ``texts`` pass it."""
        vfunc = rule.get("vfunc")
        if vfunc is None:
            return True
        passed = 0
        for text in texts:
            try:
                passed += bool(vfunc(text))
            except Exception:  # noqa: BLE001 - a failing validator rejects the value
                pass
        return passed * 2 >= len(texts)

    def _drop_unnamed_detectors(self, found, m_result: ColumnMatchResult, explain: bool) -> None:
        """Drop value matches without field-name evidence when the field name names another type
        that its values also match.

        A value rule has field-name evidence when a field rule of its type matched the name or
        when its field guard (``fieldrule``, ``fieldregex``) let it run on this field.

        - A registry detector without such evidence is dropped: RÚIAN address point codes pass
          the IČO check digit (the same modulo 11), so the ``cz_ico`` detector reports
          ``AdresniBod``; a value rule for the RÚIAN code whose field rule names the field is the
          better evidence (metacrafter-rules #61, #63).
        - A value rule without such evidence is dropped when a value rule with a field guard
          matched at least as many of the values. A guarded rule runs only on fields named for
          it, because its values alone are ambiguous: NTEE codes (B71Z, J40) of an ``NTEE_CD``
          column also have the shape of ICD-10 codes, and the guarded NTEE rule matches all of
          them (metacrafter-rules #66). A value rule that matched more values is kept, and so are
          the matches beside a value rule that a field-name rule backs: ``uuid`` stays with
          ``rufias`` on ``aoguid``, ``ipv4`` with ``okpd`` on ``okpd2``.

        Applies when no scan country is set.
        """
        if self.preset_countries or not found:
            return
        named = set()
        guarded = []
        for rule, match, name_weight in found:
            if rule.get("derived") or match not in m_result.matches:
                continue
            if name_weight or self._has_field_guard(rule):
                named.add(rule["key"])
            if self._has_field_guard(rule):
                guarded.append(match.confidence)
        if not named:
            return
        best = max(guarded) if guarded else None
        for rule, match, name_weight in found:
            if name_weight or rule["key"] in named or match not in m_result.matches:
                continue
            if rule.get("derived"):
                reason = "The field name names another type that the values match"
            elif best is None or self._has_field_guard(rule) or match.confidence > best:
                continue
            else:
                reason = "The field name names another type whose value rule matched at least as many values"
            m_result.matches.remove(match)
            if explain:
                m_result.rejected.append({"ruleid": rule["id"], "dataclass": rule["key"], "confidence": match.confidence, "matched_count": match.evidence.get("matched_count", 0) if match.evidence else 0, "considered_count": match.evidence.get("considered_count", 0) if match.evidence else 0, "reason": reason})

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
        if RulesProcessor._has_field_guard(rule):
            return RulesProcessor._field_guard_passes(rule, shortfield)
        return True

    @staticmethod
    def _has_field_guard(rule: Dict[str, Any]) -> bool:
        """True when a rule has a field-name guard: ``fieldrule`` or ``fieldregex``."""
        return "f_compiled" in rule or "f_regex" in rule

    @staticmethod
    def _field_guard_passes(rule: Dict[str, Any], shortfield: str) -> bool:
        """True when the short field name passes the rule's field-name guard.

        A rule with both guards runs when either passes: a rule pack can keep a ``fieldrule``
        keyword list for metacrafter versions that ignore ``fieldregex`` and add the regular
        expression for the names the list does not hold.
        """
        pattern = rule.get("f_regex")
        if pattern is not None and pattern.search(shortfield):
            return True
        if "f_compiled" not in rule:
            return False
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

    def _digits_only_for_letters(self, rule: Dict[str, Any], name_weight: float) -> bool:
        """True when a country-specific registry detector should not report digits alone.

        The detector has a checksum and runs without field-name or country evidence, and the
        format of its datatype has letters (``[A-Z0-9]``). Digit strings are the usual shape
        of codes in every country, and a short check digit accepts many of them: most
        Italian ISTAT municipality codes (``047016``) pass the AIC check, and the Kyiv budget
        code ``26000000000`` passes the German driving licence check (metacrafter-rules #56).
        Detectors of all-digit formats (Russian INN) keep reporting by checksum.
        """
        if not rule.get("derived") or name_weight or not rule.get("registry_country"):
            return False
        if self._country_match(rule) is not None:
            return False
        pattern = rule.get("compiled")
        source = getattr(pattern, "pattern", "") or ""
        return bool(_LETTER_CLASS.search(source))

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
        a checksum. A checksum detector without field-name or country evidence must also see
        letters when its format has them (see ``_digits_only_for_letters``).
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

    @staticmethod
    def _validator_id(rule: Dict[str, Any]) -> Any:
        """What identifies a rule's validator: its declared name, else the function itself."""
        return rule.get("validator") or rule.get("vfunc")

    def _drop_shadowed_country_matches(self, found, m_result: ColumnMatchResult, explain: bool) -> None:
        """Drop country-specific value matches that only repeat a universal match.

        Applies when no scan country is set, to curated rules without field-name evidence.
        A rule without a validator is dropped when a universal rule matched with at least its
        confidence: ``rufias`` (a GUID pattern) repeats ``uuid``. A rule with a validator is
        kept, unless a matched universal rule uses the same validator and has a higher
        confidence: the shared check proves nothing beyond the universal match, and the
        country pattern missed values the universal rule accepted. ``de_iban``
        (``validate_iban``) is dropped on a column of mixed-country IBANs and kept on a
        column of German IBANs.
        """
        if self.preset_countries or not found:
            return
        universal = [(rule, match) for rule, match, _ in found if not self._rule_countries(rule)]
        if not universal:
            return
        best = max(match.confidence for _, match in universal)
        shared = {self._validator_id(rule) for rule, _ in universal if "vfunc" in rule}
        for rule, match, name_weight in found:
            if (
                rule.get("derived")
                or name_weight
                or not self._rule_countries(rule)
                or rule.get("match") == "func"
            ):
                continue
            if "vfunc" in rule:
                if self._validator_id(rule) not in shared or match.confidence >= best:
                    continue
                reason = "Country-specific pattern covers fewer values than a universal match with the same validator"
            elif match.confidence > best:
                continue
            else:
                reason = "Country-specific pattern repeats a universal match"
            m_result.matches.remove(match)
            if explain:
                m_result.rejected.append({"ruleid": rule["id"], "dataclass": rule["key"], "confidence": match.confidence, "matched_count": match.evidence.get("matched_count", 0) if match.evidence else 0, "considered_count": match.evidence.get("considered_count", 0) if match.evidence else 0, "reason": reason})

    def qddate_pattern(self, date_format: Optional[str]) -> Optional[str]:
        """Registry pattern record id for a qddate format key, via the registry crosswalk."""
        if not date_format or self.registry is None:
            return None
        return self.registry.qddate_pattern(date_format)
