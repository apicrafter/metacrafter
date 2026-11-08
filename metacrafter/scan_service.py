#!/usr/bin/env python
# -*- coding: utf8 -*-
import io
import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import List, Optional, Dict, Any, Union

import csv
import requests
import typer
import yaml
from tabulate import tabulate

import qddate
import qddate.patterns

from iterable.helpers.detect import open_iterable

from metacrafter.classify.processor import RulesProcessor, BASE_URL, match_rank_key
from metacrafter.classify.stats import Analyzer, DEFAULT_DICT_SHARE, DEFAULT_EMPTY_VALUES

from metacrafter.config import ConfigLoader, default_rulepath
from metacrafter.engine.pii import demote_outranked_pii
from metacrafter.report import finalize_report, redact_samples, render_markdown, to_legacy
from metacrafter.reports import statistics_dict, validate_options, normalize_report, aggregate_reports, rules_fingerprint
from metacrafter.classify.utils import dict_to_columns, strip_values
from metacrafter import output as cli_output

try:  # pragma: no cover - tqdm is optional at import time
    from tqdm import tqdm
except ImportError:  # pragma: no cover
    tqdm = None

# Constants
# Supported file formats via iterabledata
SUPPORTED_FILE_TYPES = [
    "csv",      # Comma-separated values
    "tsv",      # Tab-separated values
    "json",     # JSON (array of objects)
    "jsonl",    # JSON Lines
    "ndjson",   # Newline-delimited JSON (alias for jsonl)
    "bson",     # Binary JSON
    "parquet",  # Apache Parquet
    "avro",     # Apache Avro
    "orc",      # Apache ORC
    "xls",      # Excel 97-2003
    "xlsx",     # Excel 2007+
    "xml",      # XML
    "pickle",   # Python pickle
    "pkl",      # Python pickle (alternative extension)
]
CODECS = ["lz4", 'gz', 'xz', 'bz2', 'zst', 'br', 'snappy', 'zip']
BINARY_DATA_FORMATS = ["bson", "parquet", "avro", "orc", "pickle", "pkl", "xls", "xlsx"]

# Configuration constants
DEFAULT_CONFIDENCE_THRESHOLD = 95.0  # Default confidence threshold for rule matching
DEFAULT_SCAN_LIMIT = 1000  # Default number of records to scan per field
# Minimum confidence of a reported match unless a scan sets ``confidence``. 50 drops
# chance checksum collisions and partial value matches, and field-name-only matches of
# priority 2 and 3 rules (48 and 36); ``confidence=5`` gives the 1.0 behaviour.
MIN_CONFIDENCE_FOR_MATCH = 50.0
# Registry-derived detectors unless ``registry_detectors`` is set (argument or .metacrafter).
DEFAULT_REGISTRY_DETECTORS = "checksum"
DEFAULT_BATCH_SIZE = 1000  # Default batch size for database queries

RESULT_HEADERS = ["key", "ftype", "tags", "matches", "datatype_url"]
from metacrafter.reports import STATS_HEADERS

LLM_CONFIDENCE_CAP = 80.0
LLM_CONFIDENCE_WEIGHT = 0.8
DEFAULT_LLM_MAX_REQUESTS = 100  # LLM requests per scan unless llm_max_requests is set


DEFAULT_JSON_INDENT = 4
DEFAULT_TABLE_FORMAT = cli_output.DEFAULT_TABLE_FORMAT
DEFAULT_REQUEST_TIMEOUT = 30
DEFAULT_RETRY_DELAY = 1.0


# Pure helpers now live in metacrafter/cmd/helpers.py (first slice of the core
# refactor). Re-exported here under their original names for backward compat.
from metacrafter.cmd.helpers import (  # noqa: E402
    split_option_list as _split_option_list,
    resolve_output_target as _resolve_output_target,
    strip_bom_from_keys as _strip_bom_from_keys,
)


def _open_data_file(filename, iterableargs):
    """Open a data file with iterabledata.

    Delimiter detection reads the first line and rewinds the stream. A stream that
    cannot rewind (zstandard) raises ``io.UnsupportedOperation``; such a file is
    opened again without detection, so its CSV delimiter is a comma.
    """
    try:
        return open_iterable(filename, iterableargs=iterableargs)
    except io.UnsupportedOperation:
        if not iterableargs.get("autodetect"):
            raise
    logging.debug(f"Delimiter detection is not possible for {filename}; using a comma")
    fallback = {key: value for key, value in iterableargs.items() if key != "autodetect"}
    return open_iterable(filename, iterableargs=fallback)


app = typer.Typer(
    help="Metacrafter CLI for scanning data sources and managing labeling rules."
)
rules_app = typer.Typer(help="Commands for inspecting and managing rules.")
app.add_typer(rules_app, name="rules", help="Inspect, list, and summarize rules.")

scan_app = typer.Typer(help="Commands that scan files, databases, or directories.")
app.add_typer(scan_app, name="scan", help="Scan files, SQL databases, MongoDB, or folders.")

server_app = typer.Typer(help="Commands for running the Metacrafter API server.")
app.add_typer(server_app, name="server", help="Run the API server and web interface.")

export_app = typer.Typer(help="Commands that export scan results to external systems.")
app.add_typer(export_app, name="export", help="Export scan results to external metadata catalogs.")


class CrafterCmd(object):
    """Main command class for Metacrafter operations.
    
    Handles file scanning, database scanning, and rule management.
    """
    
    def __init__(
        self,
        remote: Optional[str] = None,
        debug: bool = False,
        rulepath: Optional[List[str]] = None,
        country_codes: Optional[List[str]] = None,
        verbose: bool = False,
        quiet: bool = False,
        progress: bool = False,
        table_format: str = DEFAULT_TABLE_FORMAT,
        json_indent: Optional[int] = DEFAULT_JSON_INDENT,
        timeout: Optional[float] = None,
        retries: int = 0,
        retry_delay: float = DEFAULT_RETRY_DELAY,
        use_llm: bool = False,
        llm_only: bool = False,
        llm_provider: Optional[str] = None,
        llm_registry_path: Optional[str] = None,
        llm_index_path: Optional[str] = None,
        llm_model: Optional[str] = None,
        llm_api_key: Optional[str] = None,
        llm_base_url: Optional[str] = None,
        sample_policy: str = "names-only",
        llm_fallback: str = "error",
        embedding_provider: Optional[str] = None,
        embedding_model: Optional[str] = None,
        embedding_base_url: Optional[str] = None,
        llm_max_requests: Optional[int] = None,
        registry_path: Optional[str] = None,
        registry_offline: Optional[bool] = None,
        categories: Optional[Union[str, List[str]]] = None,
        pii_only: bool = False,
        status: Optional[Union[str, List[str]]] = None,
        registry_detectors: Optional[str] = None,
        legacy_scoring: bool = False,
        include_deprecated: bool = False,
        include_samples: bool = False,
        strip: bool = False,
        llm_verify: Optional[bool] = None,
    ):
        """Initialize CrafterCmd instance.
        
        Args:
            remote: Optional remote server URL for API calls
            debug: Enable debug logging if True
            rulepath: Optional list of custom rule paths to override config
        """
        log_level = None
        if debug:
            log_level = logging.DEBUG
        elif verbose:
            log_level = logging.INFO
        elif quiet:
            log_level = logging.ERROR

        if log_level is not None:
            logging.getLogger("metacrafter").setLevel(log_level)
        self.registry_url = ConfigLoader.get_registry_url().rstrip("/")
        self._registry_settings = ConfigLoader.get_registry_settings()
        if registry_path:
            self._registry_settings["registry_path"] = registry_path
        if registry_offline is not None:
            self._registry_settings["offline"] = registry_offline
        self._registry = None
        self.registry_filters = {
            "categories": _split_option_list(categories) if isinstance(categories, str) else (list(categories) if categories else None),
            "pii_only": bool(pii_only),
            "status": _split_option_list(status) if isinstance(status, str) else (list(status) if status else None),
        }
        self.remote = remote
        self.custom_rulepath = rulepath
        self.country_codes = (
            [code.lower() for code in country_codes] if country_codes else None
        )
        self.verbose = verbose and not quiet
        self.quiet = quiet
        self.progress_enabled = progress and not quiet
        if self.progress_enabled and tqdm is None:
            logging.warning(
                "tqdm is not available, disabling progress output. "
                "Install the optional dependency to enable progress bars."
            )
            self.progress_enabled = False
        self.table_format = table_format or DEFAULT_TABLE_FORMAT
        if json_indent is None or json_indent <= 0:
            self.json_indent = None
        else:
            self.json_indent = json_indent
        if timeout is None:
            self.request_timeout = DEFAULT_REQUEST_TIMEOUT
        elif timeout <= 0:
            self.request_timeout = None
        else:
            self.request_timeout = timeout
        self.request_retries = max(0, retries or 0)
        if retry_delay is None or retry_delay < 0:
            self.retry_delay = DEFAULT_RETRY_DELAY
        else:
            self.retry_delay = retry_delay

        self.include_samples = bool(include_samples)
        self.strip = bool(strip)
        self.legacy_scoring = bool(legacy_scoring)
        self.include_deprecated = bool(include_deprecated)
        self.registry_detectors = registry_detectors or self._registry_settings.get("detectors") or DEFAULT_REGISTRY_DETECTORS
        self.registry_detector_count = 0
        self.processor = None
        if remote is None:
            self.processor = RulesProcessor(countries=self.country_codes)
            self.prepare()
            self.registry_detector_count = self.processor.bind_registry(
                self.registry, detectors=self.registry_detectors
            )
        
        if llm_fallback not in ("error", "rules"):
            raise ValueError("llm_fallback must be error or rules")
        from metacrafter.privacy import disclose_samples
        disclose_samples([], sample_policy)
        self.sample_policy = sample_policy
        self.llm_fallback = llm_fallback
        self.llm_errors = []
        self.llm_min_confidence = 50.0
        # Load LLM config from file if not provided
        llm_config = ConfigLoader.get_llm_config()
        if llm_config:
            logging.debug(f"LLM config loaded from file: classification_mode={llm_config.get('classification_mode')}, "
                        f"llm_provider={llm_config.get('llm_provider')}, "
                        f"llm_registry_path={llm_config.get('llm_registry_path')}")
            
            # Use config values if not explicitly provided
            if llm_registry_path is None:
                llm_registry_path = llm_config.get("llm_registry_path")
                if llm_registry_path:
                    logging.debug(f"Using llm_registry_path from config: {llm_registry_path}")
            if llm_index_path is None:
                llm_index_path = llm_config.get("llm_index_path")
                if llm_index_path:
                    logging.debug(f"Using llm_index_path from config: {llm_index_path}")
            if llm_provider is None and llm_config.get("llm_provider"):
                llm_provider = llm_config.get("llm_provider")
                logging.debug(f"Using llm_provider from config: {llm_provider}")
            if llm_model is None:
                llm_model = llm_config.get("llm_model")
                if llm_model:
                    logging.debug(f"Using llm_model from config: {llm_model}")
            if llm_api_key is None:
                llm_api_key = llm_config.get("llm_api_key")
                if llm_api_key:
                    logging.debug("Using llm_api_key from config (key present)")
            if llm_base_url is None:
                llm_base_url = llm_config.get("llm_base_url")
                if llm_base_url:
                    logging.debug("Using configured LLM endpoint")
            
            # Check if config enables LLM (if not explicitly set via parameters)
            if not use_llm and not llm_only:
                config_mode = llm_config.get("classification_mode", "rules")
                logging.debug(f"Config classification_mode: {config_mode}")
                if config_mode in ("llm", "hybrid"):
                    use_llm = True
                    if config_mode == "llm":
                        llm_only = True
                    logging.debug(f"LLM enabled from config: mode={config_mode}, use_llm={use_llm}, llm_only={llm_only}")
        else:
            logging.debug("No LLM config found in .metacrafter file")
        
        llm_provider = llm_provider or "openai"
        if llm_max_requests is None:
            llm_max_requests = (llm_config or {}).get("llm_max_requests") or DEFAULT_LLM_MAX_REQUESTS
        if isinstance(llm_max_requests, bool) or not isinstance(llm_max_requests, int) or llm_max_requests < 1:
            raise ValueError("llm_max_requests must be a positive integer")
        self.llm_max_requests = llm_max_requests
        self.llm_min_confidence = (llm_config or {}).get("llm_min_confidence", 50.0)
        if self.llm_min_confidence is None:
            self.llm_min_confidence = 50.0
        # Initialize LLM classifier if requested
        self.llm_classifier = None
        self.classification_mode = "rules"  # Default: rules-only
        
        if use_llm or llm_only:
            if llm_only:
                self.classification_mode = "llm"
            else:
                self.classification_mode = "hybrid"
            
            logging.debug(f"Initializing LLM classifier: mode={self.classification_mode}, "
                        f"provider={llm_provider}, registry_path={llm_registry_path}, "
                        f"index_path={llm_index_path}")
            
            try:
                if not llm_registry_path:
                    self.llm_classifier = self._registry_llm_classifier(
                        llm_provider=llm_provider,
                        llm_model=llm_model,
                        llm_api_key=llm_api_key,
                        llm_base_url=llm_base_url,
                        sample_policy=sample_policy,
                        llm_max_requests=llm_max_requests,
                        llm_config=llm_config or {},
                    )
                else:
                    import warnings

                    warnings.warn(
                        "llm_registry_path is deprecated: LLM classification now uses the registry "
                        "adapter (registry_path / registry_url). The JSONL + vector index path will be removed.",
                        DeprecationWarning,
                        stacklevel=2,
                    )
                    from metacrafter.classify.llm import LLMClassifier
                
                    # Determine embedding API key (always uses OpenAI for embeddings)
                    embedding_api_key = llm_api_key if llm_provider == "openai" else os.getenv("OPENAI_API_KEY")
                    if not embedding_api_key:
                        logging.debug("No embedding API key found, checking OPENAI_API_KEY env var")
                        embedding_api_key = os.getenv("OPENAI_API_KEY")
                
                    # Resolve registry path if it's a relative path
                    resolved_registry_path = None
                    if llm_registry_path:
                        registry_path_obj = Path(llm_registry_path)
                        if not registry_path_obj.is_absolute():
                            # Determine config file location to resolve relative paths correctly
                            config_file_path_str = ConfigLoader.get_config_file_path()
                            if config_file_path_str:
                                config_file_path = Path(config_file_path_str).parent
                                logging.debug(f"Found config file at: {config_file_path_str}, using parent dir: {config_file_path}")
                            
                                # Try resolving relative to config file location first
                                resolved_registry_path = config_file_path / registry_path_obj
                                logging.debug(f"Trying registry_path relative to config dir: {resolved_registry_path} (exists: {resolved_registry_path.exists()})")
                            
                                if not resolved_registry_path.exists():
                                    # Fall back to current directory
                                    resolved_registry_path = Path.cwd() / registry_path_obj
                                    logging.debug(f"Trying registry_path relative to CWD: {resolved_registry_path} (exists: {resolved_registry_path.exists()})")
                            else:
                                # No config file found, try current directory
                                resolved_registry_path = Path.cwd() / registry_path_obj
                                logging.debug(f"No config file found, trying registry_path relative to CWD: {resolved_registry_path} (exists: {resolved_registry_path.exists()})")
                        else:
                            resolved_registry_path = registry_path_obj
                            logging.debug(f"Using absolute registry_path: {resolved_registry_path} (exists: {resolved_registry_path.exists()})")
                    else:
                        logging.debug("No registry_path provided, LLMClassifier will try to find default")
                
                    self.llm_classifier = LLMClassifier(
                        registry_path=str(resolved_registry_path) if resolved_registry_path else None,
                        index_path=llm_index_path,
                        embedding_api_key=embedding_api_key,
                        llm_provider=llm_provider,
                        llm_model=llm_model,
                        llm_api_key=llm_api_key,
                        llm_base_url=llm_base_url,
                        sample_policy=sample_policy,
                        embedding_provider=embedding_provider,
                        embedding_model=embedding_model or "text-embedding-3-small",
                        embedding_base_url=embedding_base_url,
                        max_requests=llm_max_requests,
                        timeout=self.request_timeout or 30.0,
                    )
                    if self.verbose:
                        logging.info(f"LLM classifier initialized with provider: {llm_provider}")
                    logging.debug("LLM classifier initialized successfully")
            except Exception as error:
                if self.llm_fallback != "rules":
                    raise RuntimeError("Requested LLM mode could not initialize; configure llm_fallback='rules' to allow fallback") from error
                self.llm_classifier = None
                self.llm_errors.append({"code": "llm_initialization", "message": "Requested LLM unavailable; using rules"})

        if llm_verify is None:
            llm_verify = bool((llm_config or {}).get("llm_verify"))
        self.llm_verifier = None
        if llm_verify:
            try:
                from metacrafter.classify.llm.verifier import LLMVerifier

                self.llm_verifier = LLMVerifier(
                    registry=self.registry,
                    provider=self._llm_provider(llm_provider, llm_model, llm_api_key, llm_base_url),
                    sample_policy=sample_policy,
                    send_pii_values=bool((llm_config or {}).get("llm_send_pii_values", False)),
                    max_requests=llm_max_requests,
                )
            except Exception as error:
                if self.llm_fallback != "rules":
                    raise RuntimeError("LLM verifier could not initialize; configure llm_fallback='rules' to allow fallback") from error
                self.llm_errors.append({"code": "llm_initialization", "message": "LLM verifier unavailable; rule matches are not verified"})


    def prepare(self) -> None:
        """Prepare the processor by loading rules and initializing date parser.
        
        Loads configuration and imports rules from configured paths.
        Uses custom rulepath if provided, otherwise uses config file or defaults.
        
        Raises:
            yaml.YAMLError: If configuration file is malformed
            IOError: If rule files cannot be read
        """
        try:
            rulepath = ConfigLoader.get_rulepath(extra_paths=self.custom_rulepath)
            for rp in rulepath:
                self.processor.import_rules_path(rp, recursive=True)
            self.dparser = qddate.DateParser(
                patterns=qddate.patterns.PATTERNS_EN + qddate.patterns.PATTERNS_RU
            )
        except (yaml.YAMLError, IOError) as e:
            logging.error(f"Error loading configuration: {e}")
            # Fall back to default rulepath
            rulepath = default_rulepath()
            for rp in rulepath:
                self.processor.import_rules_path(rp, recursive=True)
            self.dparser = qddate.DateParser(
                patterns=qddate.patterns.PATTERNS_EN + qddate.patterns.PATTERNS_RU
            )

    def _iter_with_progress(
        self,
        iterable: Any,
        desc: str,
        unit: str = "records",
        total: Optional[int] = None,
    ) -> tuple:
        """Wrap iterable with tqdm if progress reporting is enabled.
        
        Args:
            iterable: Iterable to wrap with progress bar
            desc: Description text for progress bar
            unit: Unit label (e.g., "records", "files")
            total: Total number of items (for progress calculation)
            
        Returns:
            Tuple of (wrapped_iterable, progress_bar). If progress is disabled,
            returns (original_iterable, None).
        """
        if not self.progress_enabled or tqdm is None:
            return iterable, None
        progress_iter = tqdm(
            iterable,
            desc=desc,
            unit=unit,
            total=total,
            leave=False,
        )
        return progress_iter, progress_iter

    def _create_progress_bar(
        self,
        total: Optional[int] = None,
        desc: Optional[str] = None,
        unit: str = "records",
    ) -> Optional[Any]:
        """Create a tqdm progress bar that the caller will update manually.
        
        Args:
            total: Total number of items to process
            desc: Description text for progress bar
            unit: Unit label (e.g., "records", "files")
            
        Returns:
            tqdm progress bar instance, or None if progress is disabled
        """
        if not self.progress_enabled or tqdm is None:
            return None
        return tqdm(
            total=total,
            desc=desc,
            unit=unit,
            leave=False,
        )

    def _format_country_codes(self, country_codes: Optional[List[str]]) -> str:
        """Format country codes for display.
        
        Args:
            country_codes: List of country codes or None
            
        Returns:
            Formatted string of country codes, or 'any' if None/empty
        """
        if country_codes and isinstance(country_codes, list) and len(country_codes) > 0:
            return ','.join(sorted(country_codes)).upper()
        return 'any'
    
    def _format_contexts(self, contexts: List[str]) -> str:
        """Format contexts for display with better visual separation.
        
        Args:
            contexts: List of context strings
            
        Returns:
            Formatted string with pipe separators
        """
        if not contexts:
            return ''
        # Sort for consistency
        sorted_contexts = sorted(contexts)
        return ' | '.join(sorted_contexts)
    
    def _format_lang(self, lang: str) -> str:
        """Format language code for display.
        
        Args:
            lang: Language code string
            
        Returns:
            Formatted language string (currently just returns as-is, can be enhanced with flags)
        """
        return lang

    def _print_scan_header(self, filename, filetype, record_count, contexts, langs):
        """Print a concise, readable header before scan results."""
        if self.quiet:
            return
        # Status/diagnostic output goes to stderr so that stdout carries only
        # the result payload (keeps `... --output-format json | jq` working).
        if cli_output.should_use_rich(self.table_format, sys.stderr):
            from rich.text import Text

            console = cli_output.get_console(sys.stderr)
            body = Text()
            body.append("File      ", style="cyan")
            body.append(f"{filename}\n", style="bold")
            body.append("Format    ", style="cyan")
            body.append(f"{filetype}\n")
            body.append("Records   ", style="cyan")
            body.append(f"{record_count}")
            if self.verbose:
                body.append("\nContexts  ", style="cyan")
                body.append(f"{contexts if contexts else 'all'}")
                body.append("\nLanguages ", style="cyan")
                body.append(f"{langs if langs else 'all'}")
            from rich.panel import Panel

            console.print(Panel(body, title="metacrafter scan", title_align="left",
                                border_style="cyan", expand=False))
            return
        print(f"Processing file {filename}", file=sys.stderr)
        print(f"Filetype identified as {filetype}", file=sys.stderr)
        print(f"Processing {record_count} records", file=sys.stderr)
        if self.verbose:
            print(f"Contexts filter: {contexts}, Languages filter: {langs}", file=sys.stderr)
    
    def rules_list(
        self, 
        output_format: str = "table", 
        output: Optional[str] = None
    ) -> None:
        """List all loaded classification rules.
        
        Displays a table of all field rules, data rules, and date/time patterns
        with their metadata (ID, name, type, match method, group, language, country, context).
        
        Args:
            output_format: Output format - 'table', 'json', 'yaml', or 'csv'
            output: Optional output file path
        """
        if not self.processor:
            print("Local rules are unavailable when a remote API endpoint is configured.")
            return
        
        # Collect all rules data
        all_rules_data = []
        
        # Process field rules
        for item in self.processor.field_rules:
            rule_dict = {
                'id': item.get('id', ''),
                'name': item.get('name', ''),
                'type': item.get('type', ''),
                'match': item.get('match', ''),
                'group': item.get('group', ''),
                'group_desc': item.get('group_desc', ''),
                'lang': item.get('lang', ''),
                'country': item.get('country_code', None),
                'contexts': item.get('context', []),
                'is_pii': item.get('is_pii', False),
                'priority': item.get('priority', 0),
                'minlen': item.get('minlen', None),
                'maxlen': item.get('maxlen', None),
            }
            all_rules_data.append(rule_dict)
        
        # Process data rules
        for item in self.processor.data_rules:
            rule_dict = {
                'id': item.get('id', ''),
                'name': item.get('name', ''),
                'type': item.get('type', ''),
                'match': item.get('match', ''),
                'group': item.get('group', ''),
                'group_desc': item.get('group_desc', ''),
                'lang': item.get('lang', ''),
                'country': item.get('country_code', None),
                'contexts': item.get('context', []),
                'is_pii': item.get('is_pii', False),
                'priority': item.get('priority', 0),
                'minlen': item.get('minlen', None),
                'maxlen': item.get('maxlen', None),
            }
            all_rules_data.append(rule_dict)
        
        # Process date/time patterns
        for pat in self.dparser.patterns:
            rule_dict = {
                'id': pat.get('key', ''),
                'name': pat.get('name', ''),
                'type': 'data',
                'match': 'ppr',
                'group': 'datetime',
                'group_desc': 'qddate datetime patterns',
                'lang': 'common',
                'country': None,
                'contexts': ['datetime'],
                'is_pii': False,
                'priority': 0,
                'minlen': None,
                'maxlen': None,
            }
            all_rules_data.append(rule_dict)
        
        # Output based on format
        output_format_lower = (output_format or "table").lower()
        
        if output_format_lower == "table":
            # Render with rich directly to the console when possible.
            if not output and cli_output.should_use_rich(self.table_format):
                console = cli_output.get_console()
                rich_rules = []
                for rule in all_rules_data:
                    rich_rules.append({
                        'id': rule['id'],
                        'name': rule['name'],
                        'type': rule['type'],
                        'match': rule['match'],
                        'lang_display': self._format_lang(rule['lang']),
                        'country_display': self._format_country_codes(rule['country']),
                        'contexts': rule['contexts'],
                    })
                cli_output.render_rules_rich(console, rich_rules, title="Metacrafter rules")
                return
            # Format for table display (plain fallback / file output)
            headers = ['id', 'name', 'type', 'match', 'lang', 'country', 'contexts']
            table_data = []
            for rule in all_rules_data:
                table_data.append([
                    rule['id'],
                    rule['name'],
                    rule['type'],
                    rule['match'],
                    self._format_lang(rule['lang']),
                    self._format_country_codes(rule['country']),
                    self._format_contexts(rule['contexts']),
                ])
            output_text = tabulate(
                table_data,
                headers=headers,
                tablefmt=cli_output.tabulate_format(self.table_format),
            )
            
        elif output_format_lower in ("json", "yaml"):
            # Prepare structured data
            output_dict = {
                'total_rules': len(all_rules_data),
                'rules': all_rules_data
            }
            
            if output_format_lower == "json":
                output_text = json.dumps(
                    output_dict, 
                    indent=self.json_indent or 2, 
                    ensure_ascii=False,
                    sort_keys=True
                )
            else:  # yaml
                output_text = yaml.safe_dump(
                    output_dict, 
                    default_flow_style=False, 
                    allow_unicode=True,
                    sort_keys=False
                )
                
        elif output_format_lower == "csv":
            # Write CSV format
            from io import StringIO
            output_buffer = StringIO()
            writer = csv.DictWriter(
                output_buffer, 
                fieldnames=[
                    'id', 'name', 'type', 'match', 'group', 'lang', 
                    'country', 'contexts', 'is_pii', 'priority', 'minlen', 'maxlen'
                ]
            )
            writer.writeheader()
            for rule in all_rules_data:
                writer.writerow({
                    'id': rule['id'],
                    'name': rule['name'],
                    'type': rule['type'],
                    'match': rule['match'],
                    'group': rule['group'],
                    'lang': rule['lang'],
                    'country': self._format_country_codes(rule['country']),
                    'contexts': '|'.join(rule['contexts']),
                    'is_pii': str(rule['is_pii']).lower(),
                    'priority': rule['priority'],
                    'minlen': rule['minlen'] if rule['minlen'] is not None else '',
                    'maxlen': rule['maxlen'] if rule['maxlen'] is not None else '',
                })
            output_text = output_buffer.getvalue()
        else:
            raise ValueError(f"Unsupported output format: {output_format}. Choose from: table, json, yaml, csv")
        
        # Write output
        if output:
            with open(output, 'w', encoding='utf-8') as f:
                f.write(output_text)
        else:
            print(output_text)            

    def rules_dumpstats(self) -> None:
        """Print statistics about loaded rules.
        
        Displays counts of field rules, data rules, languages, contexts,
        country codes, and date/time patterns.
        """
        if not self.processor:
            print("Local rules are unavailable when a remote API endpoint is configured.")
            return
        print("Rule types:")
        print("- field based rules %d" % (len(self.processor.field_rules)))
        print("- data based rules %d" % (len(self.processor.data_rules)))
        print("Context:")
        for key in sorted(self.processor.contexts.keys()):
            print("- %s %d" % (key, self.processor.contexts[key]))
        print("Language:")
        for key in sorted(self.processor.langs.keys()):
            print("- %s %d" % (key, self.processor.langs[key]))
        if self.processor.countries:
            print("Country code:")
            for key in sorted(self.processor.countries.keys(), key=lambda value: value or ""):
                print("- %s %d" % (key or "unknown", self.processor.countries[key]))
        print("Data/time patterns (qddate): %d" % (len(self.dparser.patterns)))

    def _filter_results_for_display(self, prepared, dformat):
        if not prepared:
            return []
        if dformat == "short":
            return [r for r in prepared if len(r) > 3 and len(r[3]) > 0]
        if dformat in ["full", "long"]:
            return prepared
        logging.warning("Unknown output detail format %s, defaulting to 'full'", dformat)
        return prepared

    def _stringify_row(self, row):
        safe_row = []
        for value in row:
            if isinstance(value, list):
                safe_row.append(",".join(map(str, value)))
            elif isinstance(value, tuple):
                safe_row.append(",".join(map(str, value)))
            else:
                safe_row.append(value)
        return safe_row

    def _write_serialized_output(self, payload, output, fmt="json", line_delimited=False):
        if fmt == "json":
            if line_delimited:
                data_str = json.dumps(payload, ensure_ascii=False)
            else:
                data_str = json.dumps(
                    payload,
                    indent=self.json_indent,
                    sort_keys=True,
                    ensure_ascii=False,
                )
        elif fmt == "yaml":
            data_str = yaml.safe_dump(
                payload, sort_keys=False, allow_unicode=True, default_flow_style=False
            )
        else:
            raise ValueError(f"Unsupported format {fmt}")

        if output:
            if isinstance(output, str):
                with open(output, "w", encoding="utf8") as f:
                    f.write(data_str)
            else:
                output.write(data_str)
                if line_delimited:
                    output.write("\n")
        else:
            print(data_str)

    def _write_text_output(self, text, output):
        if output is None:
            print(text, end="")
            return
        if isinstance(output, str):
            with open(output, "w", encoding="utf-8") as handle:
                handle.write(text)
            if not self.quiet:
                print(f"Output written to {output}", file=sys.stderr)
            return
        output.write(text)

    def _write_csv_output(self, rows, headers, output):
        safe_rows = [self._stringify_row(row) for row in rows]
        should_close = False
        if output:
            if isinstance(output, str):
                fobj = open(output, "w", newline="", encoding="utf8")
                should_close = True
            else:
                fobj = output
        else:
            fobj = sys.stdout
        writer = csv.writer(fobj)
        if headers:
            writer.writerow(headers)
        writer.writerows(safe_rows)
        if should_close:
            fobj.close()

    def _write_stats_output(self, filename, stats_table, stats_dict, output, output_format):
        output_format = (output_format or "table").lower()
        if output_format == "table":
            if output:
                if isinstance(output, str) and output.lower().endswith(".csv"):
                    self._write_csv_output(stats_table, STATS_HEADERS, output)
                else:
                    payload = {
                        "table": filename,
                        "stats": stats_dict,
                        "stats_table": stats_table,
                    }
                    self._write_serialized_output(
                        payload,
                        output,
                        "json",
                        line_delimited=not isinstance(output, str),
                    )
                if isinstance(output, str) and not self.quiet:
                    print(f"Output written to {output}", file=sys.stderr)
            else:
                if cli_output.should_use_rich(self.table_format) and stats_table:
                    console = cli_output.get_console()
                    cli_output.render_stats_rich(
                        console, stats_table, STATS_HEADERS
                    )
                elif stats_table:
                    print(
                        tabulate(
                            stats_table,
                            headers=STATS_HEADERS,
                            tablefmt=cli_output.tabulate_format(self.table_format),
                        )
                    )
                else:
                    print("No statistics available")
            return

        if output_format in ("json", "yaml"):
            payload = {
                "table": filename,
                "stats": stats_dict,
                "stats_table": stats_table,
            }
            fmt = output_format
            self._write_serialized_output(payload, output, fmt)
        elif output_format == "csv":
            self._write_csv_output(stats_table, STATS_HEADERS, output)
        else:
            print(f"Unknown output format {output_format}")
            return

        if isinstance(output, str) and not self.quiet:
            print(f"Output written to {output}", file=sys.stderr)

    def _write_results(
        self,
        report,
        filename,
        dformat,
        output,
        output_format="table",
        stats_only=False,
    ):
        prepared = report.get("results") or []
        detailed = report.get("data") or []
        stats_table = report.get("stats_table") or []
        stats_dict = report.get("stats") or {}

        if stats_only and output_format in ("json", "yaml"):
            self._write_serialized_output(normalize_report(dict(report, source=filename, table=filename)), output, output_format)
            return
        if stats_only:
            self._write_stats_output(
                filename, stats_table, stats_dict, output, output_format
            )
            return

        output_format = (output_format or "table").lower()
        if output_format == "table":
            filtered = self._filter_results_for_display(prepared, dformat)
            if output:
                if isinstance(output, str) and output.lower().endswith(".csv"):
                    self._write_csv_output(filtered, RESULT_HEADERS, output)
                else:
                    payload = normalize_report(dict(report, table=filename, source=filename, data=detailed, results=filtered))
                    self._write_serialized_output(
                        payload,
                        output,
                        "json",
                        line_delimited=not isinstance(output, str),
                    )
                if isinstance(output, str) and not self.quiet:
                    print(f"Output written to {output}", file=sys.stderr)
            else:
                if cli_output.should_use_rich(self.table_format) and detailed:
                    console = cli_output.get_console()
                    cli_output.render_scan_results_rich(
                        console, detailed, dformat
                    )
                elif filtered:
                    print(
                        cli_output.render_scan_results_plain(
                            filtered, RESULT_HEADERS, self.table_format
                        )
                    )
                else:
                    print("No results")
            return

        if output_format in ("json", "yaml", "legacy", "md"):
            payload = normalize_report(dict(report, table=filename, source=filename, data=detailed, results=prepared))
            if output_format == "legacy":
                self._write_serialized_output(to_legacy(payload), output, "json")
            else:
                if not self.include_samples:
                    payload = redact_samples(payload)
                if output_format == "md":
                    self._write_text_output(render_markdown(payload, self.include_samples), output)
                else:
                    self._write_serialized_output(payload, output, output_format)
        elif output_format == "csv":
            filtered = self._filter_results_for_display(prepared, dformat)
            self._write_csv_output(filtered, RESULT_HEADERS, output)
        else:
            print(f"Unknown output format {output_format}")
            return

        if isinstance(output, str) and not self.quiet:
            print(f"Output written to {output}", file=sys.stderr)

    def _write_db_results(
        self, db_results, dformat, output, output_format="table", stats_only=False, errors=None
    ):
        if output_format in ("json", "yaml", "legacy", "md"):
            reports = [dict(report, source=table, table=table) for table, report in db_results.items()]
            payload = aggregate_reports(reports, errors)
            if output_format == "legacy":
                self._write_serialized_output(to_legacy(payload), output, "json")
                return
            if not self.include_samples:
                payload = redact_samples(payload)
            if output_format == "md":
                self._write_text_output(render_markdown(payload, self.include_samples), output)
            else:
                self._write_serialized_output(payload, output, output_format)
            return
        if not db_results and output_format == "table":
            print("No tables processed")
            return

        if not output and output_format == "table":
            use_rich_heading = cli_output.should_use_rich(self.table_format)
            for table, report in db_results.items():
                if use_rich_heading:
                    cli_output.get_console().rule(f"[bold]Table: {table}[/bold]")
                else:
                    print(f"Table: {table}")
                self._write_results(
                    report,
                    table,
                    dformat,
                    None,
                    output_format=output_format,
                    stats_only=stats_only,
                )
                print()
            return

        if output_format == "csv":
            if stats_only:
                rows = []
                for table, report in db_results.items():
                    for row in report.get("stats_table", []) or []:
                        rows.append([table] + self._stringify_row(row))
                headers = ["table"] + STATS_HEADERS
            else:
                rows = []
                for table, report in db_results.items():
                    filtered = self._filter_results_for_display(
                        report.get("results") or [], dformat
                    )
                    for row in filtered:
                        rows.append([table] + self._stringify_row(row))
                headers = ["table"] + RESULT_HEADERS
            self._write_csv_output(rows, headers, output)
            if isinstance(output, str) and not self.quiet:
                print(f"Output written to {output}", file=sys.stderr)
            return

        # aggregated structured output
        aggregated = []
        for table, report in db_results.items():
            entry = {"table": table}
            if stats_only:
                entry["stats"] = report.get("stats", {})
                entry["stats_table"] = report.get("stats_table", [])
            else:
                entry["results"] = report.get("results", [])
                entry["fields"] = report.get("data", [])
                entry["stats"] = report.get("stats", {})
            aggregated.append(entry)

        fmt = "json"
        if output_format == "yaml":
            fmt = "yaml"
        self._write_serialized_output(aggregated, output, fmt)
        if isinstance(output, str) and not self.quiet:
            print(f"Output written to {output}", file=sys.stderr)


    def scan_data_client(
        self,
        api_root: str,
        items: List[Dict[str, Any]],
        limit: int = 1000,
        contexts: Optional[List[str]] = None,
        langs: Optional[List[str]] = None,
        confidence: Optional[float] = None,
        stop_on_match: Optional[bool] = None,
        parse_dates: Optional[bool] = None,
        ignore_imprecise: Optional[bool] = None,
        except_empty: Optional[bool] = None,
        fields: Optional[List[str]] = None,
        stats_only: Optional[bool] = None,
        dict_share: Optional[float] = None,
        empty_values: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Scan data using remote API client.
        
        Sends data to a remote Metacrafter API server for classification.
        Note: Not all parameters may be supported by the remote API.
        
        Args:
            api_root: Base URL of the remote API server
            items: List of dictionaries to classify
            limit: Maximum records to process per field
            contexts: Optional list of context filters
            langs: Optional list of language filters
            confidence: Optional minimum confidence threshold
            stop_on_match: Optional flag to stop after first match
            parse_dates: Optional flag to enable date parsing
            ignore_imprecise: Optional flag to ignore imprecise rules
            except_empty: Optional flag to exclude empty values
            fields: Optional list of specific fields to process
            stats_only: Optional flag to return only statistics
            dict_share: Optional dictionary share threshold
            empty_values: Optional list of values treated as empty
            
        Returns:
            Dictionary with classification results (same format as scan_data)
            
        Raises:
            requests.RequestException: If API request fails after retries
        """
        params = {
            'langs': ','.join(langs) if langs else None, 
            'contexts': ','.join(contexts) if contexts else None,
            'limit': limit,
        }
        # Add optional parameters if provided
        if confidence is not None:
            params['confidence'] = confidence
        if stop_on_match is not None:
            params['stop_on_match'] = stop_on_match
        if parse_dates is not None:
            params['parse_dates'] = parse_dates
        if ignore_imprecise is not None:
            params['ignore_imprecise'] = ignore_imprecise
        if except_empty is not None:
            params['except_empty'] = except_empty
        if fields is not None:
            params['fields'] = ','.join(fields) if isinstance(fields, list) else fields
        if stats_only is not None:
            params['stats_only'] = stats_only
        if dict_share is not None:
            params['dict_share'] = dict_share
        if empty_values is not None:
            params['empty_values'] = json.dumps(empty_values)

        if self.strip:
            items = strip_values(items)
        url = api_root + '/api/v1/scan_data'

        headers = {
        'Content-Type': 'application/json'
        }
        payload = json.dumps(items, ensure_ascii=False)
        attempts = max(1, self.request_retries + 1)
        last_exc = None
        for attempt in range(attempts):
            try:
                response = requests.request(
                    "POST",
                    url,
                    headers=headers,
                    data=payload,
                    params=params,
                    timeout=self.request_timeout,
                )
                response.raise_for_status()
                return response.json()
            except requests.RequestException as exc:
                last_exc = exc
                if attempt == attempts - 1:
                    logging.error("Remote scan failed: %s", exc)
                    raise
                if self.retry_delay:
                    time.sleep(self.retry_delay)
        raise last_exc

    def _llm_provider(self, llm_provider, llm_model, llm_api_key, llm_base_url):
        """Provider client for the registry classifier and the verifier."""
        from metacrafter.classify.llm.providers import get_provider

        kwargs = {"timeout": self.request_timeout or 30.0, "max_retries": 1}
        if llm_api_key is not None:
            kwargs["api_key"] = llm_api_key
        if llm_base_url is not None:
            kwargs["base_url"] = llm_base_url
        if llm_model is not None:
            kwargs["model"] = llm_model
        return get_provider(llm_provider or "openai", **kwargs)

    def _registry_llm_classifier(self, *, llm_provider, llm_model, llm_api_key, llm_base_url,
                                 sample_policy, llm_max_requests, llm_config):
        """Build the registry-driven LLM classifier (shortlist + constrained answer)."""
        from metacrafter.classify.llm.registry_classifier import RegistryLLMClassifier

        provider = self._llm_provider(llm_provider, llm_model, llm_api_key, llm_base_url)
        return RegistryLLMClassifier(
            registry=self.registry,
            provider=provider,
            sample_policy=sample_policy,
            send_pii_values=bool(llm_config.get("llm_send_pii_values", False)),
            max_requests=llm_max_requests,
            shortlist_size=int(llm_config.get("llm_shortlist_size") or 15),
        )

    def _classify_with_llm_only(
        self,
        items: List[Dict[str, Any]],
        datastats: Dict[str, Dict[str, Any]],
        langs: Optional[List[str]] = None,
        contexts: Optional[List[str]] = None,
    ):
        from metacrafter.classify.processor import TableScanResult, ColumnMatchResult, RuleResult
        from metacrafter.privacy import disclose_samples
        import math
        from metacrafter.exceptions import LLMBudgetExceeded
        results = TableScanResult()
        columns = dict_to_columns(items)
        limit, used = (getattr(self.llm_classifier, name, None) for name in ("max_requests", "request_count"))
        remaining = limit - used if isinstance(limit, int) and isinstance(used, int) else None
        if remaining is not None and len(datastats) > remaining:
            logging.warning(
                "%d fields may need up to %d LLM requests; %d remain under llm_max_requests (%d)",
                len(datastats), len(datastats), max(remaining, 0), self.llm_max_requests,
            )
        for field in datastats:
            column = ColumnMatchResult(field=field)
            results.add(column)
            try:
                raw = getattr(self.llm_classifier, "accepts_raw_values", False) is True
                # The registry classifier needs raw values locally to build its shortlist and
                # applies the disclosure policy itself before anything reaches the provider.
                samples = columns.get(field, []) if raw else disclose_samples(columns.get(field, []), self.sample_policy)
                llm = self.llm_classifier.classify_field(field_name=field,
                    sample_values=samples,
                    langs=langs, categories=contexts, country=self.country_codes)
                confidence = llm.get("confidence", 0)
                if isinstance(confidence, bool) or not isinstance(confidence, (float, int)) or not math.isfinite(confidence) or not 0 <= confidence <= 1:
                    raise ValueError("Invalid LLM confidence")
                # LLM answers map onto the engine scale capped at 80, so strong value evidence
                # from rules or registry patterns outranks an unverified model answer.
                score = min(LLM_CONFIDENCE_CAP, 100.0 * confidence * LLM_CONFIDENCE_WEIGHT)
                if llm.get("datatype_id") and score >= self._scan_llm_threshold:
                    evidence = dict(llm.get("evidence") or {})
                    evidence.update(scoring_model="v1-llm", model_confidence=confidence)
                    match = RuleResult(ruleid="llm_classifier", dataclass=llm["datatype_id"], confidence=score,
                                       ruletype="llm", is_pii=bool(llm.get("is_pii")),
                                       pii_source="registry" if llm.get("is_pii") else None,
                                       evidence=evidence)
                    match.reason = llm.get("reason", "")
                    column.add(match)
            except Exception as error:
                message = "LLM classification failed"
                if isinstance(error, LLMBudgetExceeded):
                    message = (
                        f"LLM request budget of {error.max_requests} requests exceeded; raise llm_max_requests "
                        "(--llm-max-requests) or use --llm-fallback rules"
                    )
                self.llm_errors.append({"field": field, "code": type(error).__name__, "message": message})
                if self.llm_fallback != "rules":
                    raise RuntimeError(message) from error
                fallback = self.processor.match_dict(items, fields=[field], datastats=datastats,
                    **self._scan_rule_options)
                column.matches = fallback.results[0].matches
        return results

    
    def _apply_field_schema(self, results, datastats, field_schema) -> None:
        """Add database key facts: tags, and an ``id`` match for database-generated keys."""
        from metacrafter.classify.processor import RuleResult

        for column in results.results:
            info = field_schema.get(column.field)
            if not info or column.field not in datastats:
                continue
            tags = datastats[column.field].setdefault("tags", [])
            for tag in ("primary_key", "autoincrement", "foreign_key"):
                if info.get(tag) and tag not in tags:
                    tags.append(tag)
            if info.get("primary_key") and info.get("autoincrement"):
                column.add(RuleResult(
                    ruleid="_db_autoincrement_key",
                    dataclass="id",
                    confidence=100.0,
                    ruletype="schema",
                    evidence={
                        "scoring_model": "legacy" if self.legacy_scoring else "v1",
                        "source": "database schema",
                        "primary_key": True,
                        "autoincrement": True,
                    },
                ))

    VERIFIED_RULETYPES = ("data", "registry")

    def _verify_with_llm(self, results, items, datastats, explain: bool = False) -> Dict[str, Any]:
        """Ask the LLM verifier which value matches to keep; remove the others."""
        from metacrafter.exceptions import LLMBudgetExceeded

        columns = dict_to_columns(items)
        table, by_field = [], {}
        for column in results.results:
            if column.field not in datastats:
                continue
            ids = []
            for match in column.matches:
                if match.ruletype in self.VERIFIED_RULETYPES:
                    datatype = self.registry.resolve(match.dataclass).canonical_id or match.dataclass
                    if datatype not in ids:
                        ids.append(datatype)
            if ids:
                stats = datastats[column.field]
                table.append({"field": column.field, "ftype": stats.get("ftype"), "distinct": stats.get("n_uniq"),
                              "values": columns.get(column.field, [])[:20], "candidates": ids})
                by_field[column.field] = column
        summary = {"provider": type(self.llm_verifier.provider).__name__,
                   "model": getattr(self.llm_verifier.provider, "model", None),
                   "columns": len(table), "removed": 0, "cached": False, "disclosure": None}
        if not table:
            return summary
        try:
            answer = self.llm_verifier.verify(list(datastats), table)
        except Exception as error:
            message = "LLM verification failed; rule matches are kept"
            if isinstance(error, LLMBudgetExceeded):
                message = (f"LLM request budget of {error.max_requests} requests exceeded; raise llm_max_requests "
                           "(--llm-max-requests) or use --llm-fallback rules")
            self.llm_errors.append({"code": type(error).__name__, "message": message})
            if self.llm_fallback != "rules":
                raise RuntimeError(message) from error
            summary["error"] = type(error).__name__
            return summary
        summary.update(cached=answer["cached"], disclosure=answer["disclosure"])
        for field, kept in answer["keep"].items():
            column = by_field[field]
            remaining = []
            for match in column.matches:
                datatype = self.registry.resolve(match.dataclass).canonical_id or match.dataclass
                if match.ruletype not in self.VERIFIED_RULETYPES:
                    remaining.append(match)
                elif datatype in kept:
                    match.evidence = dict(match.evidence or {}, llm_verified=True)
                    remaining.append(match)
                else:
                    summary["removed"] += 1
                    if explain and column.rejected is not None:
                        column.rejected.append({"ruleid": match.ruleid, "dataclass": match.dataclass,
                                                "confidence": match.confidence, "reason": "Rejected by the LLM verifier"})
            column.matches = remaining
        return summary

    def _merge_llm_results(
        self,
        rule_results,
        items: List[Dict[str, Any]],
        datastats: Dict[str, Dict[str, Any]],
        langs: Optional[List[str]] = None,
        contexts: Optional[List[str]] = None,
        min_confidence: float = 50.0,
    ):
        selected = {r.field for r in rule_results.results if not r.matches or max(m.confidence for m in r.matches) < min_confidence}
        stats = {field: value for field, value in datastats.items() if field in selected}
        additions = self._classify_with_llm_only(items, stats, langs, contexts)
        indexed = {r.field: r for r in rule_results.results}
        for column in additions.results:
            if column.field in indexed:
                indexed[column.field].matches.extend(column.matches)
            else:
                rule_results.add(column)
        return rule_results


    @property
    def registry(self):
        """Registry adapter, loaded lazily on first use (see metacrafter.registry.adapter)."""
        if self._registry is None:
            from metacrafter.registry.adapter import get_adapter

            settings = self._registry_settings
            self._registry = get_adapter(
                base_url=settings.get("base_url") or self.registry_url,
                registry_path=settings.get("registry_path"),
                offline=bool(settings.get("offline")),
                cache_ttl=float(settings.get("cache_ttl", 86400)),
                fetch_timeout=float(settings.get("fetch_timeout", 30)),
            )
        return self._registry

    def _registry_filter_allows(self, match: Dict[str, Any]) -> bool:
        filters = self.registry_filters
        if filters.get("pii_only") and not match.get("is_pii"):
            return False
        reg = match.get("registry") or {}
        if filters.get("status") and reg.get("status") not in filters["status"]:
            return False
        if filters.get("categories") and not set(filters["categories"]) & set(reg.get("categories") or []):
            return False
        return True

    def _enrich_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Resolve and enrich every match of a field record, then apply registry filters."""
        registry = self.registry
        matches = [registry.enrich_match(m) for m in record.get("matches", [])]
        # A value-only PII match outranked by a non-PII datatype the field name names is
        # secondary: okpd2 codes that also read as IPv4 (apicrafter/metacrafter#62).
        # Before the filters, so --pii-only sees the demoted flag.
        demote_outranked_pii(matches, ancestors=registry.ancestors)
        record["matches"] = [m for m in matches if self._registry_filter_allows(m)]
        if record["matches"]:
            record["datatype_url"] = record["matches"][0].get("classurl", "")
        else:
            record["datatype_url"] = ""
        return record

    def scan_data(
        self,
        items: List[Dict[str, Any]],
        limit: int = 1000,
        contexts: Optional[Union[str, List[str]]] = None,
        langs: Optional[Union[str, List[str]]] = None,
        confidence: Optional[float] = None,
        stop_on_match: bool = False,
        parse_dates: bool = True,
        ignore_imprecise: bool = True,
        except_empty: bool = True,
        fields: Optional[Union[str, List[str]]] = None,
        stats_only: bool = False,
        dict_share: Optional[float] = None,
        empty_values: Optional[List[str]] = None,
        classification_mode: Optional[str] = None,
        llm_min_confidence: Optional[float] = None,
        explain: bool = False,
        strip: Optional[bool] = None,
        field_schema: Optional[Dict[str, Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        """Scan data items and return classification results.
        
        Args:
            items: List of data items (dicts) to scan
            limit: Maximum records to process per field
            contexts: List of context filters (or comma-separated string)
            langs: List of language filters (or comma-separated string)
            confidence: Minimum confidence threshold (default: MIN_CONFIDENCE_FOR_MATCH)
            stop_on_match: Stop after first rule match (default: False)
            parse_dates: Enable date pattern matching (default: True)
            ignore_imprecise: Ignore imprecise rules (default: True)
            except_empty: Exclude empty values from calculations (default: True)
            fields: List of specific fields to process (default: None = all fields)
            stats_only: Return only statistics without performing classification
            dict_share: Override dictionary share percentage
            empty_values: Override list of values treated as empty
            strip: Remove leading and trailing whitespace from string values before
                analysis (default: the ``strip`` setting of this CrafterCmd, False)
            field_schema: Database schema facts per field name (``primary_key``,
                ``autoincrement``, ``foreign_key``), as ``scan sql`` reflects them
            
        Returns:
            Dictionary with keys:
                - 'results': List of classification results (summary format)
                - 'data': List of detailed field information with matches
                - 'stats': Dictionary of field statistics
                - 'stats_table': Table format of statistics
        """
        confidence = MIN_CONFIDENCE_FOR_MATCH if confidence is None else confidence
        validate_options(limit, confidence)
        if not isinstance(items, list) or any(not isinstance(item, dict) for item in items):
            raise ValueError("items must be an array of objects")
        observed_count = len(items)
        items = items[:limit]
        if self.strip if strip is None else strip:
            items = strip_values(items)
        # Parse contexts and langs if they are strings
        if isinstance(contexts, str):
            contexts = [c.strip() for c in contexts.split(',') if c.strip()]
        if isinstance(langs, str):
            langs = [l.strip() for l in langs.split(',') if l.strip()]
        if isinstance(fields, str):
            fields = [f.strip() for f in fields.split(',') if f.strip()]

        available = dict_to_columns(items)
        if fields is not None:
            missing = set(fields) - set(available)
            if missing:
                raise ValueError("Unknown selected fields: " + ", ".join(sorted(missing)))
        analyzer = Analyzer()
        analyzer_options = {"delimiter": ",", "format_in": None, "zipfile": None}
        if dict_share is not None:
            analyzer_options["dictshare"] = dict_share
        if empty_values is not None:
            analyzer_options["empty"] = empty_values
        record_progress = None
        if self.progress_enabled and items:
            record_progress = self._create_progress_bar(
                total=len(items),
                desc="Processing records",
                unit="records",
            )
        try:
            datastats = analyzer.analyze(
                fromfile=None,
                itemlist=items,
                options=analyzer_options,
                progress=record_progress,
            )
        finally:
            if record_progress:
                record_progress.close()
        datastats_dict = statistics_dict(datastats)
        if fields is not None:
            datastats_dict = {field: datastats_dict[field] for field in fields}
            datastats = [row for row in datastats if row[0] in fields]

        if stats_only:
            return {
                "results": [], "data": [], "fields": [], "stats": datastats_dict,
                "stats_table": datastats, "report_version": 2, "source": "memory",
                "complete": True, "errors": [], "requested_mode": "stats", "effective_mode": "stats",
                "sampling": {"method": "head", "retained_count": len(items), "observed_count": observed_count, "statistics_scope": "sample"},
                "rules_version": rules_fingerprint(self.processor),
            }

        self._scan_llm_threshold = max(confidence, llm_min_confidence if llm_min_confidence is not None else self.llm_min_confidence)
        validate_options(limit, self._scan_llm_threshold)
        self._scan_rule_threshold = confidence
        self._scan_rule_options = dict(confidence=confidence, stop_on_match=stop_on_match,
            dateparser=self.dparser, parse_dates=parse_dates, limit=limit,
            filter_langs=langs, filter_contexts=contexts, except_empty=except_empty,
            ignore_imprecise=ignore_imprecise, empty_values=empty_values, explain=explain,
            legacy_scoring=self.legacy_scoring, include_deprecated=self.include_deprecated)
        self.llm_errors = [] if (self.llm_classifier or self.llm_verifier) else list(self.llm_errors)
        if self.llm_classifier is not None:
            self.llm_classifier.request_count = 0
        verifier_summary = None
        if self.llm_verifier is not None:
            self.llm_verifier.request_count = 0
        # Determine classification mode
        mode = classification_mode if classification_mode is not None else self.classification_mode
        if mode not in ("rules", "llm", "hybrid"):
            raise ValueError("classification_mode must be rules, llm or hybrid")
        if mode != "rules" and self.llm_classifier is None and self.llm_fallback != "rules":
            raise ValueError("Requested LLM mode is unavailable")
        
        # Use provided confidence or default
        confidence_threshold = confidence if confidence is not None else MIN_CONFIDENCE_FOR_MATCH

        # Run classification based on mode
        if mode == "llm":
            # LLM-only mode
            if not self.llm_classifier:
                logging.warning("LLM classifier not available, falling back to rule-based classification")
                # Fall through to rule-based classification
                results = self.processor.match_dict(
                    items,
                    fields=fields,
                    datastats=datastats_dict,
                    confidence=confidence_threshold,
                    stop_on_match=stop_on_match,
                    dateparser=self.dparser,
                    parse_dates=parse_dates,
                    limit=limit,
                    filter_contexts=contexts,
                    filter_langs=langs,
                    except_empty=except_empty,
                    empty_values=empty_values,
                    explain=explain,
                    ignore_imprecise=ignore_imprecise,
                    legacy_scoring=self.legacy_scoring,
                    include_deprecated=self.include_deprecated,
                )
            else:
                results = self._classify_with_llm_only(
                    items=items,
                    datastats=datastats_dict,
                    langs=langs,
                    contexts=contexts,
                )
        else:
            # Rule-based mode (default)
            results = self.processor.match_dict(
                items,
                fields=fields,
                datastats=datastats_dict,
                confidence=confidence_threshold,
                stop_on_match=stop_on_match,
                dateparser=self.dparser,
                parse_dates=parse_dates,
                limit=limit,
                filter_contexts=contexts,
                filter_langs=langs,
                except_empty=except_empty,
                empty_values=empty_values,
                explain=explain,
                ignore_imprecise=ignore_imprecise,
                legacy_scoring=self.legacy_scoring,
                include_deprecated=self.include_deprecated,
            )
            
            if self.llm_verifier is not None:
                verifier_summary = self._verify_with_llm(results, items, datastats_dict, explain=explain)
            # Merge LLM results if in hybrid mode
            if mode == "hybrid" and self.llm_classifier:
                results = self._merge_llm_results(
                    rule_results=results,
                    items=items,
                    datastats=datastats_dict,
                    langs=langs,
                    contexts=contexts,
                    min_confidence=self._scan_llm_threshold,
                )
        if field_schema:
            self._apply_field_schema(results, datastats_dict, field_schema)
        output = []
        outdata = []
        for res in results.results:
            # The first match is the primary datatype (datatype_url): highest confidence,
            # then field-name evidence (apicrafter/metacrafter#58).
            res.rank()
            # Collapse duplicate dataclasses (multiple rules can match the same
            # type), keeping the highest-confidence occurrence of each, ordered
            # by confidence descending for a cleaner summary string.
            best_by_class = {}
            order = []
            res.matches = [match for match in res.matches if match.confidence >= confidence]
            for match in res.matches:
                existing = best_by_class.get(match.dataclass)
                if existing is None:
                    order.append(match.dataclass)
                    best_by_class[match.dataclass] = match
                else:
                    pii = existing.is_pii or match.is_pii
                    if match.confidence > existing.confidence:
                        best_by_class[match.dataclass] = match
                    best_by_class[match.dataclass].is_pii = pii
            deduped = sorted((best_by_class[name] for name in order), key=match_rank_key)
            matches = []
            for match in deduped:
                s = "%s %0.2f" % (match.dataclass, match.confidence)
                if match.format is not None:
                    s += " (%s)" % (match.format)
                matches.append(s)
            if res.field not in datastats_dict.keys():
                continue
            record = res.asdict()
            record = self._enrich_record(record)
            kept = {(m["ruleid"], m["dataclass"]) for m in record["matches"]}
            matches = [
                s for s, m in zip(matches, deduped) if (m.ruleid, m.dataclass) in kept
            ]
            output.append(
                [
                    res.field,
                    datastats_dict[res.field]["ftype"],
                    ",".join(datastats_dict[res.field]["tags"]),
                    ",".join(matches),
                    record["datatype_url"],
                ]
            )
            record["tags"] = datastats_dict[res.field]["tags"]
            record["ftype"] = datastats_dict[res.field]["ftype"]
            record["stats"] = datastats_dict[res.field]
            if field_schema and res.field in field_schema:
                record["database"] = dict(field_schema[res.field])

            outdata.append(record)
        report = {
            'results': output,
            'data': outdata,
            'stats': datastats_dict,
            'stats_table': datastats,
            'source': 'memory',
            'complete': not self.llm_errors,
            'errors': list(self.llm_errors),
            'sampling': {'method': 'head', 'retained_count': len(items), 'observed_count': observed_count, 'statistics_scope': 'sample'},
            'rules_version': rules_fingerprint(self.processor),
            'requested_mode': mode,
            'effective_mode': ('hybrid' if any(m.ruletype == 'llm' for r in results.results for m in r.matches) else 'rules') if mode != 'rules' and (self.llm_classifier is None or self.llm_errors) else mode,
            'sample_policy': self.sample_policy,
            'registry': self.registry.provenance.asdict(),
            'scoring_model': 'legacy' if self.legacy_scoring else 'v1',
            'registry_detectors': {'mode': self.registry_detectors, 'count': self.registry_detector_count},
        }
        if verifier_summary is not None:
            report['llm_verifier'] = verifier_summary
        return finalize_report(report)


    def scan_file(
        self,
        filename,
        delimiter=None,
        tagname=None,
        limit=1000,
        encoding=None,
        contexts=None,
        langs=None,
        dformat="short",
        output=None,
        confidence=None,
        stop_on_match=False,
        parse_dates=True,
        ignore_imprecise=True,
        except_empty=True,
        fields=None,
        output_format="table",
        stats_only=False,
        dict_share=None,
        empty_values=None,
        compression="auto",
        classification_mode=None,
        llm_min_confidence=None,
        sampling="head",
        seed=0,
        explain=False,
    ):
        """Scan a file and classify its fields.
        
        This method processes a data file (CSV, TSV, JSON, JSONL, NDJSON, BSON, 
        Parquet, Avro, ORC, Excel, XML, Pickle) and applies classification rules 
        to identify field types and data patterns. Results can be written to a file 
        or printed to stdout. Format detection is handled automatically by iterabledata.
        
        Args:
            filename: Path to the file to scan. Supports multiple formats including
                CSV, TSV, JSON, JSONL, BSON, Parquet, Avro, ORC, Excel (.xls, .xlsx),
                and XML. Compression codecs (gz, bz2, xz, lz4, zst, br, snappy) are
                automatically detected.
            delimiter: CSV/TSV delimiter character. If None, auto-detected from file.
            tagname: XML tag name for data extraction. If None, auto-detected.
            limit: Maximum number of records to process per field. Defaults to 1000.
            encoding: Character encoding for text files (e.g., 'utf8', 'cp1251').
                If None, auto-detected using chardet.
            contexts: Optional comma-separated string or list of context filters
                (e.g., 'pii,common'). Only rules matching these contexts will be applied.
            langs: Optional comma-separated string or list of language filters
                (e.g., 'en,ru'). Only rules for these languages will be applied.
            dformat: Output detail format - 'short' (only fields with matches) or
                'full' (all fields). Defaults to 'short'.
            output: Optional output file path. If None, results printed to stdout.
                Use '-' or 'stdout' to explicitly write to stdout.
            confidence: Minimum confidence threshold (0-100). Rules with lower
                confidence will be ignored. Defaults to 50.0 (MIN_CONFIDENCE_FOR_MATCH).
            stop_on_match: If True, stop after first rule match per field.
                Defaults to False.
            parse_dates: Enable automatic date/time pattern detection. Defaults to True.
            ignore_imprecise: If True, ignore imprecise rules. Defaults to True.
            except_empty: If True, exclude empty values from confidence calculations.
                Defaults to True.
            fields: Optional comma-separated string or list of specific fields to process.
                If None, all fields are processed.
            output_format: Output format - 'table', 'json', 'yaml', or 'csv'.
                Defaults to 'table'.
            stats_only: If True, return only statistics without classification.
                Defaults to False.
            dict_share: Override dictionary share threshold (percentage). Fields with
                uniqueness below this threshold are treated as dictionaries.
            empty_values: Optional comma-separated list of values treated as empty.
                Use '""' for empty string. Defaults to [None, "", "None", "NaN", "-", "N/A"].
            compression: Compression codec handling - 'auto' (detect), 'none' (disable),
                or specific codec ('gz', 'bz2', etc.). Defaults to 'auto'.
        
        Returns:
            None if output is written to file/stdout, otherwise returns empty list
            on error or None on success.
        
        Raises:
            IOError: If file cannot be opened or read.
            OSError: If file system error occurs.
            ValueError: If file format is unsupported or invalid.
        
        Example:
            >>> cmd = CrafterCmd()
            >>> cmd.scan_file(
            ...     "data.csv",
            ...     limit=100,
            ...     contexts="pii",
            ...     output_format="json",
            ...     output="results.json"
            ... )
        """
        validate_options(limit, confidence if confidence is not None else MIN_CONFIDENCE_FOR_MATCH)
        iterableargs = {}
        if tagname is not None:
            iterableargs['tagname'] = tagname
        if delimiter is not None:
            iterableargs['delimiter'] = delimiter
        else:
            # iterabledata >= 1.1 uses a comma unless asked to detect the delimiter,
            # so semicolon- and pipe-separated CSV files were read as one column.
            iterableargs['autodetect'] = True

        if encoding is not None:
            iterableargs['encoding'] = encoding                         
        if compression is not None and compression.lower() != "auto":
            iterableargs['compression'] = None if compression.lower() == "none" else compression
                   
        if tagname is None and ".xml" in str(filename).lower():
            from metacrafter.classify.utils import xml_quick_analyzer
            discovered = xml_quick_analyzer(filename)
            if discovered:
                iterableargs["tagname"] = discovered["short"]
        try:
            data_file = _open_data_file(filename, iterableargs)
        except (IOError, OSError) as e:
            logging.error(f"Error opening file {filename}: {e}")
            print(f"Error: Could not open file {filename}: {e}", file=sys.stderr)
            return []
        except ValueError as e:
            logging.error(f"Unsupported file type {filename}: {e}")
            supported_formats = ", ".join(sorted(set(SUPPORTED_FILE_TYPES)))
            print(
                f"Unsupported file type: {filename}\n"
                f"Error: {e}\n"
                f"Supported formats: {supported_formats}\n"
                f"Supported compression codecs: {', '.join(sorted(CODECS))}\n"
                f"For more information, see the documentation at "
                f"https://github.com/apicrafter/metacrafter", file=sys.stderr
            )
            return []
        except Exception as e:
            logging.error(f"Unexpected error opening file {filename}: {e}", exc_info=True)
            print(f"Unexpected error processing file {filename}: {e}", file=sys.stderr)
            return []
        # Process file efficiently - collect items from iterator
        # Note: For very large files, consider implementing streaming processing
        # in scan_data() method to avoid loading entire file into memory
        if data_file is None:
            return []
        items = []
        try:
            progress_iter, progress_bar = self._iter_with_progress(
                data_file, desc=f"Reading {filename}", unit="records"
            )
            try:
                from metacrafter.sampling import sample_records
                items, provenance = sample_records(progress_iter, limit, sampling, seed)
            finally:
                if progress_bar:
                    progress_bar.close()
            _strip_bom_from_keys(items)

            if len(items) == 0 and output_format == "table" and not self.quiet:
                print("No records found to process")

            if output_format == "table":
                self._print_scan_header(filename, data_file.id(), len(items), contexts, langs)

            if self.remote is None:
                report = self.scan_data(
                    items,
                    limit,
                    contexts,
                    langs,
                    confidence=confidence,
                    stop_on_match=stop_on_match,
                    parse_dates=parse_dates,
                    ignore_imprecise=ignore_imprecise,
                    except_empty=except_empty,
                    fields=fields,
                    stats_only=stats_only,
                    dict_share=dict_share,
                    empty_values=empty_values,
                    explain=explain,
                    classification_mode=classification_mode,
                    llm_min_confidence=llm_min_confidence,
                )
            else:
                report = self.scan_data_client(
                    self.remote,
                    items,
                    limit,
                    contexts,
                    langs,
                    confidence=confidence,
                    stop_on_match=stop_on_match,
                    parse_dates=parse_dates,
                    ignore_imprecise=ignore_imprecise,
                    except_empty=except_empty,
                    fields=fields,
                    stats_only=stats_only,
                    dict_share=dict_share,
                    empty_values=empty_values,
                )
                if stats_only and not report.get("stats_table"):
                    print("Stats-only mode is not available for remote scans.")
                    return
            report.update(source=str(filename), table=str(filename), sampling=provenance)
            self._write_results(
                report,
                filename,
                dformat,
                output,
                output_format=output_format,
                stats_only=stats_only,
            )
            return report
        finally:
            if data_file is not None:
                data_file.close()
            # Clear items from memory after processing
            del items


    def scan_bulk(
        self,
        dirname: str,
        delimiter: Optional[str] = None,
        tagname: Optional[str] = None,
        limit: int = 1000,
        encoding: str = "utf8",
        contexts: Optional[Union[str, List[str]]] = None,
        langs: Optional[Union[str, List[str]]] = None,
        output: Optional[Union[str, Any]] = None,
        confidence: Optional[float] = None,
        stop_on_match: bool = False,
        parse_dates: bool = True,
        ignore_imprecise: bool = True,
        except_empty: bool = True,
        fields: Optional[Union[str, List[str]]] = None,
        output_format: str = "table",
        stats_only: bool = False,
        dict_share: Optional[float] = None,
        empty_values: Optional[List[str]] = None,
        compression: str = "auto",
    ) -> None:
        """Scan a directory into one valid aggregate with source failures."""
        import io
        reports, errors = {}, []
        for directory, directories, names in os.walk(dirname):
            directories.sort()
            for name in sorted(names):
                filename = os.path.join(directory, name)
                if output and isinstance(output, str) and os.path.abspath(filename) == os.path.abspath(output):
                    continue
                try:
                    report = self.scan_file(filename, delimiter=delimiter, tagname=tagname,
                        limit=limit, encoding=encoding, contexts=contexts, langs=langs,
                        confidence=confidence, stop_on_match=stop_on_match,
                        parse_dates=parse_dates, ignore_imprecise=ignore_imprecise,
                        except_empty=except_empty, fields=fields, stats_only=stats_only,
                        dict_share=dict_share, empty_values=empty_values, compression=compression,
                        output=io.StringIO(), output_format="json")
                    if not isinstance(report, dict):
                        raise ValueError("Source produced no report")
                    reports[filename] = report
                except Exception as error:
                    errors.append({"source": filename, "code": type(error).__name__, "message": "File scan failed"})
        self._write_db_results(reports, "full", output, output_format, stats_only, errors=errors)
        return aggregate_reports(list(reports.values()), errors)


    def _scan_duckdb_native(
        self,
        connectstr: str,
        schema: Optional[str] = None,
        limit: int = 1000,
        contexts: Optional[Union[str, List[str]]] = None,
        langs: Optional[Union[str, List[str]]] = None,
        dformat: str = "short",
        output: Optional[Union[str, Any]] = None,
        confidence: Optional[float] = None,
        stop_on_match: bool = False,
        parse_dates: bool = True,
        ignore_imprecise: bool = True,
        except_empty: bool = True,
        fields: Optional[Union[str, List[str]]] = None,
        output_format: str = "table",
        stats_only: bool = False,
        dict_share: Optional[float] = None,
        empty_values: Optional[List[str]] = None,
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> None:
        """Scan bounded database samples with schema identity and cleanup."""
        from metacrafter.sources import scan_duckdb
        options = dict(contexts=contexts, langs=langs, confidence=confidence,
                       stop_on_match=stop_on_match, parse_dates=parse_dates,
                       ignore_imprecise=ignore_imprecise, except_empty=except_empty,
                       fields=fields, stats_only=stats_only, dict_share=dict_share,
                       empty_values=empty_values)
        reports, errors = scan_duckdb(self, connectstr, schema, limit, options)
        self._write_db_results(reports, dformat, output, output_format, stats_only, errors=errors)
        return aggregate_reports(list(reports.values()), errors)


    def scan_db(
        self,
        connectstr: str = "sqlite:///test.db",
        schema: Optional[str] = None,
        limit: int = 1000,
        contexts: Optional[Union[str, List[str]]] = None,
        langs: Optional[Union[str, List[str]]] = None,
        dformat: str = "short",
        output: Optional[Union[str, Any]] = None,
        confidence: Optional[float] = None,
        stop_on_match: bool = False,
        parse_dates: bool = True,
        ignore_imprecise: bool = True,
        except_empty: bool = True,
        fields: Optional[Union[str, List[str]]] = None,
        output_format: str = "table",
        stats_only: bool = False,
        dict_share: Optional[float] = None,
        empty_values: Optional[List[str]] = None,
        batch_size: int = DEFAULT_BATCH_SIZE,
        tables: Optional[List[str]] = None,
    ) -> None:
        """Scan bounded database samples with schema identity and cleanup.

        ``tables`` limits the scan to these table names (``name`` or ``schema.name``).
        """
        from metacrafter.sources import scan_sql
        options = dict(contexts=contexts, langs=langs, confidence=confidence,
                       stop_on_match=stop_on_match, parse_dates=parse_dates,
                       ignore_imprecise=ignore_imprecise, except_empty=except_empty,
                       fields=fields, stats_only=stats_only, dict_share=dict_share,
                       empty_values=empty_values)
        if connectstr.split(":", 1)[0].split("+", 1)[0] == "duckdb":
            from metacrafter.sources import scan_duckdb
            reports, errors = scan_duckdb(self, connectstr, schema, limit, options)
            if tables:
                wanted = set(tables)
                reports = {k: v for k, v in reports.items() if k in wanted or k.rsplit(".", 1)[-1] in wanted}
        else:
            reports, errors = scan_sql(self, connectstr, schema, limit, options, batch_size, tables=tables)
        self._write_db_results(reports, dformat, output, output_format, stats_only, errors=errors)
        return aggregate_reports(list(reports.values()), errors)




    def scan_mongodb(
        self,
        host: str = "localhost",
        port: int = 27017,
        dbname: str = "test",
        username: Optional[str] = None,
        password: Optional[str] = None,
        limit: int = 1000,
        contexts: Optional[Union[str, List[str]]] = None,
        langs: Optional[Union[str, List[str]]] = None,
        dformat: str = "short",
        output: Optional[Union[str, Any]] = None,
        confidence: Optional[float] = None,
        stop_on_match: bool = False,
        parse_dates: bool = True,
        ignore_imprecise: bool = True,
        except_empty: bool = True,
        fields: Optional[Union[str, List[str]]] = None,
        output_format: str = "table",
        stats_only: bool = False,
        dict_share: Optional[float] = None,
        empty_values: Optional[List[str]] = None,
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> None:
        """Scan bounded database samples with schema identity and cleanup."""
        from metacrafter.sources import scan_mongo
        options = dict(contexts=contexts, langs=langs, confidence=confidence,
                       stop_on_match=stop_on_match, parse_dates=parse_dates,
                       ignore_imprecise=ignore_imprecise, except_empty=except_empty,
                       fields=fields, stats_only=stats_only, dict_share=dict_share,
                       empty_values=empty_values)
        reports, errors = scan_mongo(self, host, port, dbname, username, password, limit, options, batch_size)
        self._write_db_results(reports, dformat, output, output_format, stats_only, errors=errors)
        return aggregate_reports(list(reports.values()), errors)


