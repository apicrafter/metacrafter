"""Validated HTTP adapter for the shared scanning service."""

import hmac
import json
import logging
import os
from threading import Lock
from flask import Flask, jsonify, request
from werkzeug.exceptions import BadRequest, RequestEntityTooLarge

MANAGE_PREFIX = ""
DEFAULT_LIMIT = 1000


class MetacrafterApp:
    def __init__(self, rules_processor=None, date_parser=None, auth_token=None):
        self.rules_processor = rules_processor
        self.date_parser = date_parser
        self.auth_token = auth_token or os.environ.get("METACRAFTER_API_TOKEN")
        self.service = None
        self._lock = Lock()
        self.app = Flask("Metacrafter")
        self.app.config.update(
            MAX_CONTENT_LENGTH=10 * 1024 * 1024, MAX_RECORDS=10000, MAX_DEPTH=32
        )
        self.app.add_url_rule(
            "/api/v1/scan_data", "scan_data", self._scan, methods=["POST"]
        )
        self.app.add_url_rule("/health", "health", lambda: jsonify(status="ok"))
        self.app.add_url_rule("/ready", "ready", self._ready)
        self.app.register_error_handler(
            RequestEntityTooLarge,
            lambda error: (jsonify(error="Payload too large"), 413),
        )

    def _create_processor(self):
        from metacrafter.classify.processor import RulesProcessor
        from metacrafter.config import ConfigLoader

        processor = RulesProcessor()
        for path in ConfigLoader.get_rulepath():
            processor.import_rules_path(path)
        return processor

    def _create_date_parser(self):
        import qddate

        return qddate.DateParser(
            patterns=qddate.patterns.PATTERNS_EN + qddate.patterns.PATTERNS_RU
        )

    def initialize_rules(self):
        with self._lock:
            if self.service is None:
                from metacrafter.core import CrafterCmd

                self.rules_processor = self.rules_processor or self._create_processor()
                self.date_parser = self.date_parser or self._create_date_parser()
                self.service = CrafterCmd(remote="injected", quiet=True)
                self.service.processor = self.rules_processor
                self.service.dparser = self.date_parser
                self.service.llm_classifier = None
                self.service.classification_mode = "rules"

    def _ready(self):
        try:
            self.initialize_rules()
            return jsonify(status="ready")
        except Exception:
            return jsonify(status="unavailable"), 503

    def _scan(self):
        if self.auth_token:
            supplied = request.headers.get("Authorization", "")
            if not hmac.compare_digest(supplied, "Bearer " + self.auth_token):
                return jsonify(error="Unauthorized"), 401
        try:
            self.initialize_rules()
            return _scan_service(self.service)
        except Exception:
            logging.exception("Scan service initialization failed")
            return jsonify(error="Internal server error"), 500


def _depth(value):
    if isinstance(value, dict):
        return 1 + max((_depth(v) for v in value.values()), default=0)
    if isinstance(value, list):
        return 1 + max((_depth(v) for v in value), default=0)
    return 0


def _scan_service(service):
    try:
        if not request.data:
            return jsonify(error="Invalid JSON", message="Request data is empty"), 400
        items = json.loads(request.data)
        if not isinstance(items, list) or any(
            not isinstance(item, dict) for item in items
        ):
            raise ValueError("Payload must be an array of objects")
        from flask import current_app

        if len(items) > current_app.config.get("MAX_RECORDS", 10000) or _depth(
            items
        ) > current_app.config.get("MAX_DEPTH", 32):
            raise ValueError("Payload exceeds record or nesting limit")
        options = {}
        for key in ("limit",):
            if key in request.args:
                options[key] = int(request.args[key])
        for key in ("confidence", "dict_share", "llm_min_confidence"):
            if key in request.args:
                options[key] = float(request.args[key])
        for key in ("contexts", "langs", "fields"):
            if key in request.args:
                options[key] = request.args[key]
        for key in (
            "stop_on_match",
            "parse_dates",
            "ignore_imprecise",
            "except_empty",
            "stats_only",
            "explain",
        ):
            if key in request.args:
                value = request.args[key].lower()
                if value not in ("true", "false", "1", "0"):
                    raise ValueError(key + " must be true or false")
                options[key] = value in ("true", "1")
        if "empty_values" in request.args:
            options["empty_values"] = json.loads(request.args["empty_values"])
            if not isinstance(options["empty_values"], list):
                raise ValueError("empty_values must be an array")
        report = service.scan_data(items, **options)
        return jsonify(report)
    except (json.JSONDecodeError, BadRequest):
        return jsonify(error="Invalid JSON"), 400
    except (ValueError, TypeError, RecursionError) as error:
        return (
            jsonify(
                error="Invalid request data",
                message=(
                    str(error)
                    if not isinstance(error, RecursionError)
                    else "Payload nesting limit exceeded"
                ),
            ),
            400,
        )
    except Exception:
        logging.exception("Scan request failed")
        return jsonify(error="Internal server error"), 500


def scan_data(rules_processor, date_parser):
    from metacrafter.core import CrafterCmd

    service = CrafterCmd(remote="injected", quiet=True)
    service.processor, service.dparser = rules_processor, date_parser
    service.llm_classifier, service.classification_mode = None, "rules"
    return _scan_service(service)


def add_api_rules(app, prefix=MANAGE_PREFIX):
    instance = MetacrafterApp()
    app.add_url_rule(
        prefix + "/api/v1/scan_data", "classify", instance._scan, methods=["POST"]
    )


def initialize_rules():
    """Compatibility hook; application instances own initialized dependencies."""


def create_app():
    return MetacrafterApp().app
