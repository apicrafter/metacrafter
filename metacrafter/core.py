#!/usr/bin/env python
# -*- coding: utf8 -*-
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

from metacrafter.classify.processor import RulesProcessor, BASE_URL
from metacrafter.classify.stats import Analyzer, DEFAULT_DICT_SHARE, DEFAULT_EMPTY_VALUES

from metacrafter.config import ConfigLoader

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
MIN_CONFIDENCE_FOR_MATCH = 5.0  # Minimum confidence percentage for a match
DEFAULT_BATCH_SIZE = 1000  # Default batch size for database queries

RESULT_HEADERS = ["key", "ftype", "tags", "matches", "datatype_url"]
STATS_HEADERS = [
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

DEFAULT_JSON_INDENT = 4
DEFAULT_TABLE_FORMAT = "simple"
DEFAULT_REQUEST_TIMEOUT = 30
DEFAULT_RETRY_DELAY = 1.0


def _split_option_list(value: Optional[str]):
    """Split comma-separated option values preserving empty-string marker."""
    if value is None:
        return None
    entries = []
    for token in value.split(","):
        token = token.strip()
        if token == "":
            continue
        if token in {'""', "''"}:
            entries.append("")
        elif token.lower() in ("none", "null"):
            entries.append(None)
        else:
            entries.append(token)
    return entries or None


def _resolve_output_target(output: Optional[str], stdout_flag: bool):
    """Convert output arguments into file-like objects when needed."""
    if stdout_flag:
        return sys.stdout
    if isinstance(output, str) and output.strip().lower() in ("-", "stdout"):
        return sys.stdout
    return output


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
        llm_provider: str = "openai",
        llm_registry_path: Optional[str] = None,
        llm_index_path: Optional[str] = None,
        llm_model: Optional[str] = None,
        llm_api_key: Optional[str] = None,
        llm_base_url: Optional[str] = None,
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
            logging.basicConfig(
                format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                level=log_level,
                force=True,
            )
            logging.captureWarnings(True)
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

        self.processor = None
        if remote is None:
            self.processor = RulesProcessor(countries=self.country_codes)
            self.prepare()
        
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
            if llm_provider == "openai" and llm_config.get("llm_provider"):
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
                    logging.debug(f"Using llm_base_url from config: {llm_base_url}")
            
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
                )
                if self.verbose:
                    logging.info(f"LLM classifier initialized with provider: {llm_provider}")
                logging.debug("LLM classifier initialized successfully")
            except ImportError as e:
                logging.warning(f"Failed to import LLM classifier: {e}. LLM functionality disabled.")
                logging.debug("Import error details:", exc_info=True)
                self.llm_classifier = None
                self.classification_mode = "rules"
            except Exception as e:
                logging.warning(f"Failed to initialize LLM classifier: {e}. LLM functionality disabled.")
                logging.debug("Initialization error details:", exc_info=True)
                self.llm_classifier = None
                self.classification_mode = "rules"

    def prepare(self) -> None:
        """Prepare the processor by loading rules and initializing date parser.
        
        Loads configuration and imports rules from configured paths.
        Uses custom rulepath if provided, otherwise uses config file or defaults.
        
        Raises:
            yaml.YAMLError: If configuration file is malformed
            IOError: If rule files cannot be read
        """
        try:
            rulepath = self.custom_rulepath if self.custom_rulepath else ConfigLoader.get_rulepath()
            for rp in rulepath:
                self.processor.import_rules_path(rp, recursive=True)
            self.dparser = qddate.DateParser(
                patterns=qddate.patterns.PATTERNS_EN + qddate.patterns.PATTERNS_RU
            )
        except (yaml.YAMLError, IOError) as e:
            logging.error(f"Error loading configuration: {e}")
            # Fall back to default rulepath
            rulepath = ConfigLoader.DEFAULT_RULEPATH
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
            # Format for table display
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
            output_text = tabulate(table_data, headers=headers, tablefmt=self.table_format)
            
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
            for key in sorted(self.processor.countries.keys()):
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
                    print(f"Output written to {output}")
            else:
                if stats_table:
                    print(tabulate(stats_table, headers=STATS_HEADERS, tablefmt=self.table_format))
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
            print(f"Output written to {output}")

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
                    payload = {
                        "table": filename,
                        "fields": detailed,
                        "results": filtered,
                        "stats": stats_dict,
                    }
                    self._write_serialized_output(
                        payload,
                        output,
                        "json",
                        line_delimited=not isinstance(output, str),
                    )
                if isinstance(output, str) and not self.quiet:
                    print(f"Output written to {output}")
            else:
                if filtered:
                    print(tabulate(filtered, headers=RESULT_HEADERS, tablefmt=self.table_format))
                else:
                    print("No results")
            return

        if output_format in ("json", "yaml"):
            payload = {
                "table": filename,
                "fields": detailed,
                "results": prepared,
                "stats": stats_dict,
            }
            fmt = output_format
            self._write_serialized_output(payload, output, fmt)
        elif output_format == "csv":
            filtered = self._filter_results_for_display(prepared, dformat)
            self._write_csv_output(filtered, RESULT_HEADERS, output)
        else:
            print(f"Unknown output format {output_format}")
            return

        if isinstance(output, str) and not self.quiet:
            print(f"Output written to {output}")

    def _write_db_results(
        self, db_results, dformat, output, output_format="table", stats_only=False
    ):
        if not db_results:
            print("No tables processed")
            return

        if not output:
            for table, report in db_results.items():
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
                print(f"Output written to {output}")
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
            print(f"Output written to {output}")


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
            params['dictshare'] = dict_share

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

    def _classify_with_llm_only(
        self,
        items: List[Dict[str, Any]],
        datastats: Dict[str, Dict[str, Any]],
        langs: Optional[List[str]] = None,
        contexts: Optional[List[str]] = None,
    ):
        """Classify all fields using LLM only.
        
        Args:
            items: List of data items
            datastats: Field statistics dictionary
            langs: Optional language filters
            contexts: Optional context filters (categories)
            
        Returns:
            TableScanResult compatible with rule-based results
        """
        from metacrafter.classify.processor import TableScanResult, ColumnMatchResult, RuleResult
        
        if not self.llm_classifier:
            return TableScanResult()
        
        # Collect sample values for each field
        field_samples = {}
        for item in items[:100]:  # Limit to first 100 items for efficiency
            for field_name, value in item.items():
                if field_name not in field_samples:
                    field_samples[field_name] = []
                if value and len(field_samples[field_name]) < 10:
                    field_samples[field_name].append(str(value))
        
        # Prepare fields for batch classification
        fields_to_classify = []
        for field_name in datastats.keys():
            sample_values = field_samples.get(field_name, [])[:5]  # Limit to 5 samples
            fields_to_classify.append({
                "field_name": field_name,
                "sample_values": sample_values
            })
        
        # Convert contexts to categories for LLM
        categories = contexts if contexts else None
        
        # Classify with LLM
        try:
            llm_results = self.llm_classifier.classify_batch(
                fields=fields_to_classify,
                langs=langs,
                categories=categories
            )
        except Exception as e:
            logging.warning(f"LLM classification failed: {e}")
            return TableScanResult()
        
        # Convert LLM results to TableScanResult format
        results = TableScanResult()
        for llm_result in llm_results:
            field_name = llm_result.get("field")
            if not field_name:
                continue
            
            datatype_id = llm_result.get("datatype_id")
            confidence = llm_result.get("confidence", 0.0)
            
            # Create column result
            column_result = ColumnMatchResult(field=field_name)
            
            # Add LLM match if found
            if datatype_id and confidence > 0:
                llm_match = RuleResult(
                    ruleid="llm_classifier",
                    dataclass=datatype_id,
                    confidence=confidence * 100.0,  # Convert to 0-100 scale
                    ruletype="llm",
                )
                column_result.add(llm_match)
            
            results.results.append(column_result)
        
        return results
    
    def _merge_llm_results(
        self,
        rule_results,
        items: List[Dict[str, Any]],
        datastats: Dict[str, Dict[str, Any]],
        langs: Optional[List[str]] = None,
        contexts: Optional[List[str]] = None,
        min_confidence: float = 50.0,
    ):
        """Merge LLM classification results with rule-based results.
        
        Args:
            rule_results: Results from rule-based classification
            items: Original data items
            datastats: Field statistics
            langs: Language filters
            contexts: Context filters (categories)
            min_confidence: Minimum confidence for LLM results (0-100)
            
        Returns:
            Merged TableScanResult
        """
        from metacrafter.classify.processor import ColumnMatchResult, RuleResult
        
        if not self.llm_classifier:
            return rule_results
        
        # Get fields that matched with high confidence
        matched_fields = {}
        for res in rule_results.results:
            if res.matches:
                # Get highest confidence match
                max_conf = max(m.confidence for m in res.matches) if res.matches else 0
                matched_fields[res.field] = max_conf
        
        # Collect sample values for each field
        field_samples = {}
        for item in items[:100]:  # Limit to first 100 items
            for field_name, value in item.items():
                if field_name not in field_samples:
                    field_samples[field_name] = []
                if value and len(field_samples[field_name]) < 10:
                    field_samples[field_name].append(str(value))
        
        # Convert contexts to categories for LLM
        categories = contexts if contexts else None
        
        # Try LLM classification for unmatched or low-confidence fields
        for field_name, stats in datastats.items():
            # Skip if already matched with high confidence
            if field_name in matched_fields and matched_fields[field_name] >= min_confidence:
                continue
            
            # Get sample values
            sample_values = field_samples.get(field_name, [])[:5]  # Limit to 5 samples
            
            try:
                # Classify with LLM
                llm_result = self.llm_classifier.classify_field(
                    field_name=field_name,
                    sample_values=sample_values,
                    langs=langs,
                    categories=categories,
                )
                
                # Check if LLM found a match with sufficient confidence
                llm_confidence = llm_result.get("confidence", 0) * 100  # Convert to 0-100
                if llm_result.get("datatype_id") and llm_confidence >= min_confidence:
                    # Find or create column result
                    column_result = None
                    for res in rule_results.results:
                        if res.field == field_name:
                            column_result = res
                            break
                    
                    if not column_result:
                        column_result = ColumnMatchResult(field=field_name)
                        rule_results.results.append(column_result)
                    
                    # Add LLM match as a rule result
                    llm_match = RuleResult(
                        ruleid="llm_classifier",
                        dataclass=llm_result["datatype_id"],
                        confidence=llm_confidence,
                        ruletype="llm",
                    )
                    column_result.add(llm_match)
                    
                    if self.verbose:
                        logging.debug(f"LLM classified {field_name} as {llm_result['datatype_id']} "
                                    f"(confidence: {llm_confidence:.1f}%)")
            
            except Exception as e:
                logging.warning(f"LLM classification failed for field {field_name}: {e}")
                continue
        
        return rule_results

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
        llm_min_confidence: float = 50.0,
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
            
        Returns:
            Dictionary with keys:
                - 'results': List of classification results (summary format)
                - 'data': List of detailed field information with matches
                - 'stats': Dictionary of field statistics
                - 'stats_table': Table format of statistics
        """
        # Parse contexts and langs if they are strings
        if isinstance(contexts, str):
            contexts = [c.strip() for c in contexts.split(',') if c.strip()]
        if isinstance(langs, str):
            langs = [l.strip() for l in langs.split(',') if l.strip()]
        if isinstance(fields, str):
            fields = [f.strip() for f in fields.split(',') if f.strip()]

        analyzer = Analyzer()
        analyzer_options = {"delimiter": ",", "format_in": None, "zipfile": None}
        if dict_share is not None:
            analyzer_options["dictshare"] = dict_share
        if empty_values:
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
            "dictvalues",
        ]
        datastats_dict = {}
        if datastats is not None:
            for row in datastats:
                datastats_dict[row[0]] = {}
                for n in range(0, len(headers)):
                    datastats_dict[row[0]][headers[n]] = row[n]

        if stats_only:
            return {
                "results": [],
                "data": [],
                "stats": datastats_dict,
                "stats_table": datastats,
            }

        # Determine classification mode
        mode = classification_mode if classification_mode is not None else self.classification_mode
        
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
                    ignore_imprecise=ignore_imprecise,
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
                ignore_imprecise=ignore_imprecise,
            )
            
            # Merge LLM results if in hybrid mode
            if mode == "hybrid" and self.llm_classifier:
                results = self._merge_llm_results(
                    rule_results=results,
                    items=items,
                    datastats=datastats_dict,
                    langs=langs,
                    contexts=contexts,
                    min_confidence=llm_min_confidence,
                )
        output = []
        outdata = []
        for res in results.results:
            matches = []
            for match in res.matches:
                s = "%s %0.2f" % (match.dataclass, match.confidence)
                if match.format is not None:
                    s += " (%s)" % (match.format)
                matches.append(s)
            if res.field not in datastats_dict.keys():
                continue
            output.append(
                [
                    res.field,
                    datastats_dict[res.field]["ftype"],
                    ",".join(datastats_dict[res.field]["tags"]),
                    ",".join(matches),
                    BASE_URL.format(dataclass=res.matches[0].dataclass)
                    if len(res.matches) > 0
                    else "",
                ]
            )
            record = res.asdict()
            record["tags"] = datastats_dict[res.field]["tags"]
            record["ftype"] = datastats_dict[res.field]["ftype"]
            record["datatype_url"] = (
                BASE_URL.format(dataclass=res.matches[0].dataclass)
                if len(res.matches) > 0
                else ""
            )
            record["stats"] = datastats_dict[res.field]

            outdata.append(record)
        report = {
            'results': output,
            'data': outdata,
            'stats': datastats_dict,
            'stats_table': datastats,
        }            
        return report


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
        llm_min_confidence=50.0,
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
                confidence will be ignored. Defaults to 5.0.
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
        iterableargs = {}
        if tagname is not None:
            iterableargs['tagname'] = tagname
        if delimiter is not None:
            iterableargs['delimiter'] = delimiter            

        if encoding is not None:
            iterableargs['encoding'] = encoding                         
        if compression is not None and compression.lower() != "auto":
            iterableargs['compression'] = None if compression.lower() == "none" else compression
                   
        try:
            data_file = open_iterable(filename, iterableargs=iterableargs) 
        except (IOError, OSError) as e:
            logging.error(f"Error opening file {filename}: {e}")
            print(f"Error: Could not open file {filename}: {e}")
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
                f"https://github.com/apicrafter/metacrafter"
            )
            return []
        except Exception as e:
            logging.error(f"Unexpected error opening file {filename}: {e}", exc_info=True)
            print(f"Unexpected error processing file {filename}: {e}")
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
                for item in progress_iter:
                    items.append(item)
            finally:
                if progress_bar:
                    progress_bar.close()

            if len(items) == 0:
                if not self.quiet:
                    print("No records found to process")
                return

            if not self.quiet:
                print(f"Processing file {filename}")
                print(f"Filetype identified as {data_file.id()}")
                print(f"Processing {len(items)} records")
            if self.verbose and not self.quiet:
                print(f"Contexts filter: {contexts}, Languages filter: {langs}")

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
            self._write_results(
                report,
                filename,
                dformat,
                output,
                output_format=output_format,
                stats_only=stats_only,
            )
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
        filelist = [
            os.path.join(dp, f) for dp, dn, filenames in os.walk(dirname) for f in filenames
        ]
        output_handle = None
        should_close = False
        if output and hasattr(output, "write"):
            output_handle = output
        elif output:
            output_handle = open(output, "w", encoding="utf8")
            should_close = True
        try:
            for filename in filelist:                
                try:
                    self.scan_file(
                        filename, 
                        delimiter=delimiter, 
                        tagname=tagname, 
                        limit=limit, 
                        encoding=encoding,
                        contexts=contexts, 
                        langs=langs, 
                        dformat="full",
                        output=output_handle if output_handle else None,
                        confidence=confidence,
                        stop_on_match=stop_on_match,
                        parse_dates=parse_dates,
                        ignore_imprecise=ignore_imprecise,
                        except_empty=except_empty,
                        fields=fields,
                        output_format=output_format,
                        stats_only=stats_only,
                        dict_share=dict_share,
                        empty_values=empty_values,
                        compression=compression,
                    )
                except (IOError, OSError) as e:
                    logging.error(f"File I/O error processing {filename}: {e}")
                    if not self.quiet:
                        print(f"File I/O error on {filename}: {e}")
                except ValueError as e:
                    logging.error(f"Value error processing {filename}: {e}")
                    if not self.quiet:
                        print(f"Value error on {filename}: {e}")
                except Exception as e:
                    logging.error(
                        f"Unexpected error processing {filename}: {e}", exc_info=True
                    )
                    if not self.quiet:
                        print(f"Unexpected error on {filename}: {e}")
        finally:
            if output_handle and should_close:
                output_handle.close()

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
        """Scan DuckDB database using native Python API to avoid SQLAlchemy type issues.
        
        This method bypasses SQLAlchemy's type system which has issues with DuckDB's
        unhashable type objects.
        """
        try:
            import duckdb
        except ImportError:
            raise ImportError(
                "DuckDB Python package is required. Install it with: pip install duckdb"
            )
        
        import os
        
        # Parse DuckDB connection string: duckdb:///path/to/file.duckdb
        # or duckdb://:memory: for in-memory database
        db_path = connectstr.replace("duckdb:///", "").replace("duckdb://", "")
        if db_path == ":memory:":
            db_path = ":memory:"
        elif not os.path.isabs(db_path):
            # Relative path - make it absolute
            db_path = os.path.abspath(db_path)
        
        if not self.quiet:
            print(f"Connecting to DuckDB: {db_path}")
        
        # Connect using DuckDB native API
        conn = duckdb.connect(db_path)
        db_results = {}
        
        effective_batch = batch_size if batch_size and batch_size > 0 else DEFAULT_BATCH_SIZE
        
        try:
            # Get list of tables
            # Escape schema name to prevent SQL injection
            if schema:
                safe_schema = schema.replace("'", "''")  # Escape single quotes
                tables_query = f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{safe_schema}'"
            else:
                tables_query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
            
            tables_result = conn.execute(tables_query).fetchall()
            tables = [row[0] for row in tables_result]
            
            if not tables:
                if not self.quiet:
                    print("No tables found in database")
                return
            
            for table in tables:
                if not self.quiet:
                    print(f"- table {table}")
                
                try:
                    # Query table data using DuckDB native API
                    # Use parameterized query to avoid SQL injection
                    # DuckDB uses ? for parameters, but LIMIT doesn't support parameters well
                    # So we validate limit is an integer and quote the table name
                    safe_limit = int(limit)
                    safe_table = table.replace('"', '""')  # Escape double quotes
                    query = f'SELECT * FROM "{safe_table}" LIMIT {safe_limit}'
                    result = conn.execute(query)
                    
                    # Get column names from description
                    column_names = [desc[0] for desc in result.description]
                    
                    items = []
                    fetch_progress = self._create_progress_bar(
                        total=limit if limit else None,
                        desc=f"Fetching rows for {table}",
                        unit="records",
                    )
                    
                    try:
                        # Fetch rows in batches
                        while True:
                            row_batch = result.fetchmany(effective_batch)
                            if not row_batch:
                                break
                            
                            # Convert rows to dictionaries
                            for row in row_batch:
                                row_dict = {}
                                for i, col_name in enumerate(column_names):
                                    val = row[i]
                                    # Convert any problematic types
                                    if val is not None:
                                        type_name = type(val).__name__
                                        if 'DuckDBPyType' in type_name:
                                            val = None
                                    row_dict[col_name] = val
                                items.append(row_dict)
                            
                            if fetch_progress:
                                fetch_progress.update(len(row_batch))
                            
                            if len(items) >= limit:
                                items = items[:limit]
                                break
                    finally:
                        if fetch_progress:
                            fetch_progress.close()
                    
                    # Process the data
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
                            print("Stats-only mode is not available for remote database scans.")
                            return
                    
                    db_results[table] = report
                    
                except Exception as e:
                    logging.error(f"Error processing table {table}: {e}")
                    if not self.quiet:
                        print(f"Error processing table {table}: {e}")
                    continue
            
            self._write_db_results(
                db_results,
                dformat,
                output,
                output_format=output_format,
                stats_only=stats_only,
            )
            
        finally:
            conn.close()

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
    ) -> None:
        """Scan SQL database tables and classify their fields.
        
        Connects to a SQL database using SQLAlchemy connection string and
        scans all tables (optionally filtered by schema) to classify field types.
        
        Args:
            connectstr: SQLAlchemy connection string (e.g., "postgresql://user:pass@host/db")
            schema: Optional database schema name to filter tables
            limit: Maximum records to process per field
            contexts: Optional context filters (comma-separated string or list)
            langs: Optional language filters (comma-separated string or list)
            dformat: Output detail format - "short" or "full"
            output: Optional output file path or file-like object
            confidence: Optional minimum confidence threshold
            stop_on_match: If True, stop after first rule match
            parse_dates: Enable date pattern matching
            ignore_imprecise: Ignore imprecise rules
            except_empty: Exclude empty values from calculations
            fields: Optional list of specific fields to process
            output_format: Output format - "table", "json", "yaml", or "csv"
            stats_only: Return only statistics without classification
            dict_share: Override dictionary share threshold
            empty_values: Override list of values treated as empty
            batch_size: Number of rows to fetch per batch
            
        Raises:
            ValueError: If schema name is invalid or not found
            sqlalchemy.exc.SQLAlchemyError: If database connection or query fails
        """
        from sqlalchemy import create_engine, inspect, text, event
        import sqlalchemy.exc
        import re

        dbtype = connectstr.split(":", 1)[0].lower()
        
        # Special handling for DuckDB to avoid SQLAlchemy type caching issues
        if dbtype == "duckdb":
            return self._scan_duckdb_native(connectstr, schema, limit, contexts, langs, 
                                            dformat, output, confidence, stop_on_match,
                                            parse_dates, ignore_imprecise, except_empty,
                                            fields, output_format, stats_only, dict_share,
                                            empty_values, batch_size)
        
        if not self.quiet:
            print("Connecting to %s" % (connectstr))
        
        # Configure SQLite to handle encoding errors gracefully
        if dbtype == "sqlite":
            import sqlite3
            
            def set_sqlite_text_factory(dbapi_conn, connection_record):
                """Set text factory to handle UTF-8 decoding errors gracefully"""
                def text_factory(data):
                    """Custom text factory that handles encoding errors"""
                    try:
                        return data.decode('utf-8')
                    except UnicodeDecodeError:
                        # Replace invalid bytes with replacement character
                        return data.decode('utf-8', errors='replace')
                dbapi_conn.text_factory = text_factory
            
            dbe = create_engine(connectstr)
            event.listen(dbe, "connect", set_sqlite_text_factory)
        else:
            dbe = create_engine(connectstr)
        inspector = inspect(dbe)
        db_schemas = inspector.get_schema_names()
        con = dbe.connect()
        db_results = {}
        
        if schema:
            if not re.match(r"^[a-zA-Z0-9_.]+$", schema):
                raise ValueError(
                    f"Invalid schema name: {schema}. Only alphanumeric, underscore, and dot characters are allowed."
                )
            if schema not in db_schemas:
                raise ValueError(f"Schema '{schema}' not found in database.")
        
        effective_batch = batch_size if batch_size and batch_size > 0 else DEFAULT_BATCH_SIZE
        
        for db_schema in db_schemas:
            if schema and schema != db_schema:
                continue
            if not self.quiet:
                print("Processing schema: %s" % db_schema)
            if dbtype == "postgres" and db_schema:
                from sqlalchemy.sql import quoted_name

                safe_schema = str(quoted_name(db_schema, quote=True))
                con.execute(text(f"SET search_path TO {safe_schema}"))
            
            valid_tables = inspector.get_table_names(schema=db_schema)
            for table in valid_tables:
                if not self.quiet:
                    print("- table %s" % (table))
                try:
                    from sqlalchemy.sql import quoted_name

                    safe_table = str(quoted_name(table, quote=True))
                    query = text(f"SELECT * FROM {safe_table} LIMIT :limit")
                    queryres = con.execute(query, {"limit": limit})
                except sqlalchemy.exc.ProgrammingError as e:
                    print("Error processing table %s: %s" % (table, str(e)))
                    continue
                
                items = []
                fetch_progress = self._create_progress_bar(
                    total=limit if limit else None,
                    desc=f"Fetching rows for {table}",
                    unit="records",
                )
                try:
                    # Get column names from result set metadata (safer for DuckDB)
                    # This avoids issues with DuckDB type objects in keys()
                    column_names = None
                    
                    # First, try to get column names from inspector (most reliable)
                    try:
                        columns = inspector.get_columns(table, schema=db_schema)
                        column_names = [col['name'] for col in columns]
                    except Exception:
                        pass
                    
                    # If that didn't work, try getting from the result set
                    if column_names is None:
                        try:
                            # Try to get column names from the result set's column metadata
                            if hasattr(queryres, 'keys'):
                                raw_keys = queryres.keys()
                                # Convert all keys to strings to avoid unhashable type issues
                                column_names = [str(k) if k is not None else f"col_{i}" 
                                              for i, k in enumerate(raw_keys)]
                        except Exception:
                            pass
                    
                    # Fetch first batch
                    try:
                        row_batch = queryres.fetchmany(effective_batch)
                    except sqlalchemy.exc.OperationalError as e:
                        error_msg = str(e)
                        if "Could not decode to UTF-8" in error_msg or "decode" in error_msg.lower():
                            if not self.quiet:
                                print(f"Warning: Encoding error in table {table}: {error_msg}")
                                print("Skipping table due to encoding issues. Consider re-encoding the database.")
                            continue
                        else:
                            # Re-raise if it's a different OperationalError
                            raise
                    
                    # If still no column names, try getting from the first row
                    if column_names is None and row_batch:
                        first_row = row_batch[0]
                        try:
                            if hasattr(first_row, '_fields'):
                                # SQLAlchemy 2.x Row has _fields attribute
                                column_names = list(first_row._fields)
                            elif hasattr(first_row, 'keys'):
                                raw_keys = first_row.keys()
                                # Convert all keys to strings to avoid unhashable type issues
                                column_names = [str(k) if k is not None else f"col_{i}" 
                                              for i, k in enumerate(raw_keys)]
                        except Exception:
                            pass
                    
                    # If we still don't have column names, we'll use fallback methods
                    
                    while row_batch:
                        # Convert SQLAlchemy Row objects to dictionaries
                        # Handle both SQLAlchemy 1.x and 2.x Row objects
                        # Special handling for DuckDB which may return unhashable type objects
                        batch_dicts = []
                        for row in row_batch:
                            row_dict = {}
                            try:
                                # Use column names if available, otherwise try row methods
                                if column_names:
                                    # Access by index to avoid type object issues with DuckDB
                                    for i, col_name in enumerate(column_names):
                                        try:
                                            # Ensure col_name is a string (not a DuckDB type object)
                                            col_name_str = str(col_name) if col_name is not None else f"col_{i}"
                                            
                                            # Access value by index to avoid DuckDB type object issues
                                            if hasattr(row, '__getitem__'):
                                                val = row[i]
                                            elif hasattr(row, '_mapping'):
                                                val = row._mapping.get(col_name_str)
                                            elif hasattr(row, 'mapping'):
                                                val = row.mapping.get(col_name_str)
                                            else:
                                                val = getattr(row, col_name_str, None)
                                            
                                            # Convert DuckDB type objects to None
                                            if val is not None:
                                                type_name = type(val).__name__
                                                if 'DuckDBPyType' in type_name:
                                                    val = None
                                            
                                            row_dict[col_name_str] = val
                                        except (TypeError, ValueError, IndexError, AttributeError) as e:
                                            # If there's an error accessing the value, set to None
                                            col_name_str = str(col_name) if col_name is not None else f"col_{i}"
                                            row_dict[col_name_str] = None
                                else:
                                    # Fallback: try standard SQLAlchemy row conversion methods
                                    try:
                                        if hasattr(row, '_mapping'):
                                            row_dict = dict(row._mapping)
                                        elif hasattr(row, 'mapping'):
                                            row_dict = dict(row.mapping)
                                        elif hasattr(row, '_asdict'):
                                            row_dict = row._asdict()
                                        else:
                                            # Last resort: try direct dict conversion
                                            row_dict = dict(row)
                                    except (TypeError, ValueError) as e:
                                        # If dict conversion fails, try manual conversion
                                        # This handles cases where keys() returns unhashable types
                                        row_dict = {}
                                        try:
                                            # Try to get column count and access by index
                                            if hasattr(row, '__len__'):
                                                num_cols = len(row)
                                                for i in range(num_cols):
                                                    try:
                                                        val = row[i]
                                                        # Check for DuckDB type objects
                                                        if val is not None:
                                                            type_name = type(val).__name__
                                                            if 'DuckDBPyType' in type_name:
                                                                val = None
                                                        row_dict[f"col_{i}"] = val
                                                    except (TypeError, ValueError, IndexError):
                                                        row_dict[f"col_{i}"] = None
                                        except Exception:
                                            # If all else fails, create empty dict
                                            pass
                                
                                # Clean up any unhashable types in the final dict
                                # Also ensure all keys are strings
                                cleaned_dict = {}
                                for key, value in row_dict.items():
                                    # Ensure key is a string
                                    key_str = str(key) if key is not None else "unknown"
                                    
                                    if value is not None:
                                        type_name = type(value).__name__
                                        if 'DuckDBPyType' in type_name:
                                            cleaned_dict[key_str] = None
                                        else:
                                            cleaned_dict[key_str] = value
                                    else:
                                        cleaned_dict[key_str] = None
                                
                                batch_dicts.append(cleaned_dict)
                            except (TypeError, ValueError) as e:
                                # If all else fails, log and skip this row
                                logging.warning(f"Failed to convert row to dictionary: {e}")
                                continue
                        
                        items.extend(batch_dicts)
                        if fetch_progress:
                            fetch_progress.update(len(batch_dicts))
                        if len(items) >= limit:
                            items = items[:limit]
                            break
                        try:
                            row_batch = queryres.fetchmany(effective_batch)
                        except sqlalchemy.exc.OperationalError as e:
                            error_msg = str(e)
                            if "Could not decode to UTF-8" in error_msg or "decode" in error_msg.lower():
                                if not self.quiet:
                                    print(f"Warning: Encoding error while fetching from table {table}: {error_msg}")
                                    print("Stopping fetch for this table due to encoding issues.")
                                # Break out of while loop
                                row_batch = []
                                break
                            else:
                                # Re-raise if it's a different OperationalError
                                raise
                finally:
                    if fetch_progress:
                        fetch_progress.close()

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
                        print(
                            "Stats-only mode is not available for remote database scans."
                        )
                        return
                db_results[table] = report

        self._write_db_results(
            db_results,
            dformat,
            output,
            output_format=output_format,
            stats_only=stats_only,
        )



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
        """Scan entire MongoDB database and classify collection fields.
        
        Connects to MongoDB and scans all collections in the specified database
        to classify field types.
        
        Args:
            host: MongoDB hostname or connection URI
            port: MongoDB port number
            dbname: Database name to scan
            username: Optional MongoDB username
            password: Optional MongoDB password
            limit: Maximum documents to process per field
            contexts: Optional context filters (comma-separated string or list)
            langs: Optional language filters (comma-separated string or list)
            dformat: Output detail format - "short" or "full"
            output: Optional output file path or file-like object
            confidence: Optional minimum confidence threshold
            stop_on_match: If True, stop after first rule match
            parse_dates: Enable date pattern matching
            ignore_imprecise: Ignore imprecise rules
            except_empty: Exclude empty values from calculations
            fields: Optional list of specific fields to process
            output_format: Output format - "table", "json", "yaml", or "csv"
            stats_only: Return only statistics without classification
            dict_share: Override dictionary share threshold
            empty_values: Override list of values treated as empty
            batch_size: Number of documents to fetch per batch
            
        Raises:
            pymongo.errors.PyMongoError: If MongoDB connection fails
        """
        if not self.quiet:
            print("Connecting to %s %d" % (host, port))
        from pymongo import MongoClient

        client = MongoClient(host, port, username=username, password=password)
        db = client[dbname]
        tables = db.list_collection_names()
        db_results = {}
        effective_batch = batch_size if batch_size and batch_size > 0 else DEFAULT_BATCH_SIZE

        for table in tables:
            if not self.quiet:
                print("- table %s" % (table))
            cursor = db[table].find().batch_size(effective_batch).limit(limit)
            items = list(cursor)
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
            db_results[table] = report

        self._write_db_results(
            db_results,
            dformat,
            output,
            output_format=output_format,
            stats_only=stats_only,
        )

