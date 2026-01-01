"""API module for Metacrafter server endpoints."""
import logging
import json as std_json
from typing import Optional
import qddate
import yaml
from flask import (
    Flask,
    json,
    jsonify,
    request,
)

from metacrafter.classify.stats import Analyzer
from metacrafter.config import ConfigLoader, DEFAULT_RULEPATH
from metacrafter.classify.processor import RulesProcessor, BASE_URL

MANAGE_PREFIX = ""
DEFAULT_LIMIT = 1000


class MetacrafterApp:
    """Application factory for Metacrafter API server.
    
    Manages dependencies (rules processor and date parser) and provides
    dependency injection for API endpoints.
    """
    
    def __init__(
        self,
        rules_processor: Optional[RulesProcessor] = None,
        date_parser: Optional[qddate.DateParser] = None,
    ):
        """Initialize MetacrafterApp with optional dependencies.
        
        Args:
            rules_processor: Optional pre-initialized rules processor.
                If None, will be created from configuration.
            date_parser: Optional pre-initialized date parser.
                If None, will be created with default patterns.
        """
        self.rules_processor = rules_processor
        self.date_parser = date_parser
        self.app = Flask("Metacrafter")
        self._setup_routes()
    
    def _create_processor(self) -> RulesProcessor:
        """Create and initialize rules processor from configuration."""
        processor = RulesProcessor()
        try:
            rulepath = ConfigLoader.get_rulepath()
            for rp in rulepath:
                processor.import_rules_path(rp, recursive=True)
        except (yaml.YAMLError, IOError) as e:
            logging.error("Error loading configuration: %s", e)
            # Fall back to default rulepath
            rulepath = DEFAULT_RULEPATH
            for rp in rulepath:
                processor.import_rules_path(rp, recursive=True)
        return processor
    
    def _create_date_parser(self) -> qddate.DateParser:
        """Create date parser with default patterns."""
        return qddate.DateParser(
            patterns=qddate.patterns.PATTERNS_EN + qddate.patterns.PATTERNS_RU
        )
    
    def _setup_routes(self):
        """Set up Flask routes with dependency injection."""
        self.app.add_url_rule(
            MANAGE_PREFIX + "/api/v1/scan_data",
            "scan_data",
            lambda: scan_data(self.rules_processor or self._create_processor(),
                            self.date_parser or self._create_date_parser()),
            methods=["POST"],
        )
    
    def initialize_rules(self):
        """Initialize rules processor and date parser from configuration.
        
        This method is provided for backward compatibility.
        Dependencies are now lazily initialized when needed.
        """
        if self.rules_processor is None:
            self.rules_processor = self._create_processor()
        if self.date_parser is None:
            self.date_parser = self._create_date_parser()


def scan_data(rules_processor: RulesProcessor, date_parser: qddate.DateParser):
    """API endpoint to scan data items and return classification results.

    Accepts JSON array of items and returns classification results.

    Args:
        rules_processor: Initialized rules processor instance
        date_parser: Initialized date parser instance

    Query Parameters:
        format: Output format (short/full), default: short
        langs: Comma-separated language filters
        contexts: Comma-separated context filters
        limit: Maximum records to process per field, default: 1000

    Returns:
        JSON response with classification results

    Raises:
        ValueError: If request data is invalid
        KeyError: If required data is missing
    """
    output_format = request.args.get("format", default="short", type=str)
    langs = request.args.get("langs", default=None, type=str)
    contexts = request.args.get("contexts", default=None, type=str)
    scan_limit = request.args.get("limit", default=DEFAULT_LIMIT, type=int)
    langs = langs.split(".") if langs is not None else None
    contexts = contexts.split(".") if contexts is not None else None

    try:
        if not request.data:
            raise ValueError("Request data is empty")
        items = std_json.loads(request.data)
        analyzer = Analyzer()
        datastats = analyzer.analyze(
            fromfile=None,
            itemlist=items,
            options={"delimiter": ",", "format_in": None, "zipfile": None},
        )
        if datastats is None:
            raise ValueError("No data to analyze")
        headers = [
            "key",
            "ftype",
            "is_dictkey",
            "is_uniq",
            "n_uniq",
            "share_uniq",
            "minlen",
            "maxlen",
            "avglen",
            "tags",
            "has_digit",
            "has_alphas",
            "has_special",
            "minval",
            "maxval",
            "has_any_digit",
            "has_any_alphas",
            "has_any_special",
            "dictvalues",
        ]
        datastats_dict = {}
        for row in datastats:
            datastats_dict[row[0]] = {}
        for row in datastats:
            for n, header in enumerate(headers):
                if n < len(row):
                    datastats_dict[row[0]][header] = row[n]

        # Use minimum confidence threshold (5%) for API requests
        MIN_CONFIDENCE_FOR_MATCH = 5.0
        results = rules_processor.match_dict(
            items,
            datastats=datastats_dict,
            confidence=MIN_CONFIDENCE_FOR_MATCH,
            dateparser=date_parser,
            parse_dates=True,
            limit=scan_limit,
            filter_contexts=contexts,
            filter_langs=langs,
        )


        output = []
        outdata = []
        report = {}
        for res in results.results:
            matches = []
            for match in res.matches:
                s = f"{match.dataclass} {match.confidence:0.2f}"
                if match.format is not None:
                    s += f" ({match.format})"
                matches.append(s)
            if res.field not in datastats_dict:
                continue
            output.append(
                [
                    res.field,
                    datastats_dict[res.field]["ftype"],
                    ",".join(datastats_dict[res.field]["tags"]),
                    ",".join(matches),
                    BASE_URL.format(dataclass=res.matches[0].dataclass)
                    if res.matches
                    else "",
                ]
            )
            record = res.asdict()
            record["tags"] = datastats_dict[res.field]["tags"]
            record["ftype"] = datastats_dict[res.field]["ftype"]
            record["datatype_url"] = (
                BASE_URL.format(dataclass=res.matches[0].dataclass)
                if res.matches
                else ""
            )
            record["stats"] = datastats_dict[res.field]

            outdata.append(record)
        report = {'results': output, 'data': outdata}
    except std_json.JSONDecodeError as ex:
        logging.error("JSON decode error: %s", ex)
        report = {"error": "Invalid JSON", "message": str(ex)}
        return jsonify(report), 400
    except ValueError as ex:
        # Check if it's an empty data error that should be "Invalid JSON"
        if "empty" in str(ex).lower() or not request.data:
            logging.error("Invalid request data: %s", ex)
            report = {"error": "Invalid JSON", "message": str(ex)}
        else:
            logging.error("Invalid request data: %s", ex)
            report = {"error": "Invalid request data", "message": str(ex)}
        return jsonify(report), 400
    except KeyError as ex:
        logging.error("Invalid request data: %s", ex)
        report = {"error": "Invalid request data", "message": str(ex)}
        return jsonify(report), 400
    except Exception as ex:
        logging.error("Unexpected error in scan_data: %s", ex, exc_info=True)
        report = {"error": "Internal server error", "message": str(ex)}
        return jsonify(report), 500
    return jsonify(report)


def add_api_rules(app: Flask, prefix: str = MANAGE_PREFIX):
    """Add API routes to Flask application (backward compatibility).
    
    Note: This function is kept for backward compatibility.
    For new code, use MetacrafterApp class directly.
    
    Args:
        app: Flask application instance
        prefix: URL prefix for API routes
    """
    # Create a temporary app instance to get dependencies
    # This maintains backward compatibility but is not ideal
    # Prefer using MetacrafterApp directly
    app_instance = MetacrafterApp()
    app.add_url_rule(
        prefix + "/api/v1/scan_data",
        "classify",
        lambda: scan_data(
            app_instance.rules_processor or app_instance._create_processor(),
            app_instance.date_parser or app_instance._create_date_parser()
        ),
        methods=["POST"],
    )


# Backward compatibility function
def initialize_rules():
    """Initialize rules (backward compatibility function).
    
    Note: This function is kept for backward compatibility.
    Dependencies are now managed by MetacrafterApp.
    """
    pass
