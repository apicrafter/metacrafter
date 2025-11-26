#!/usr/bin/env python
# -*- coding: utf8 -*-
import json
import logging
import os
import sys
import time
from typing import List, Optional

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
SUPPORTED_FILE_TYPES = ["jsonl", "bson", "csv", "tsv", "json", "xml", 'ndjson', 'avro', 'parquet', 'xls', 'xlsx', 'orc', 'ndjson']
CODECS = ["lz4", 'gz', 'xz', 'bz2', 'zst', 'br', 'snappy']
BINARY_DATA_FORMATS = ["bson", "parquet"]

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


class CrafterCmd(object):
    """Main command class for Metacrafter operations.
    
    Handles file scanning, database scanning, and rule management.
    """
    
    def __init__(
        self,
        remote: str = None,
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

    def prepare(self):
        """Prepare the processor by loading rules and initializing date parser.
        
        Loads configuration and imports rules from configured paths.
        Uses custom rulepath if provided, otherwise uses config file or defaults.
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

    def _iter_with_progress(self, iterable, desc, unit="records", total=None):
        """Wrap iterable with tqdm if progress reporting is enabled."""
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

    def _create_progress_bar(self, total=None, desc=None, unit="records"):
        """Create a tqdm progress bar that the caller will update manually."""
        if not self.progress_enabled or tqdm is None:
            return None
        return tqdm(
            total=total,
            desc=desc,
            unit=unit,
            leave=False,
        )

    def rules_list(self):
        """Rules list"""
        if not self.processor:
            print("Local rules are unavailable when a remote API endpoint is configured.")
            return
        headers = ['id', 'name', 'type', 'match', 'group', 'group_desc', 'lang']
        all_rules = []
        for item in self.processor.field_rules:
            rule = []
            for h in headers:
                rule.append(item[h])
            rule.append(','.join(item['context']))                             
            all_rules.append(rule)

        for item in self.processor.data_rules:
            rule = []
            for h in headers:
                rule.append(item[h])
            rule.append(','.join(item['context']))        
            all_rules.append(rule)

        for pat in self.dparser.patterns:
            rule = [pat['key'], pat['name'], 'data', 'ppr', 'datetime', "qddate datetime patterns", 'common', 'datetime']    
            all_rules.append(rule) 
        headers.append('context')            
#        print(all_rules)
        print(tabulate(all_rules, headers=headers, tablefmt=self.table_format))            

    def rules_dumpstats(self):
        """Dump rules statistics"""
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
        api_root,
        items,
        limit=1000,
        contexts=None,
        langs=None,
        confidence=None,
        stop_on_match=None,
        parse_dates=None,
        ignore_imprecise=None,
        except_empty=None,
        fields=None,
        stats_only=None,
        dict_share=None,
        empty_values=None,
    ):
        """Scan data using remote API client.
        
        Note: Not all parameters may be supported by the remote API.
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

    def scan_data(
        self,
        items,
        limit=1000,
        contexts=None,
        langs=None,
        confidence=None,
        stop_on_match=False,
        parse_dates=True,
        ignore_imprecise=True,
        except_empty=True,
        fields=None,
        stats_only=False,
        dict_share=None,
        empty_values=None,
    ):
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

        # Use provided confidence or default
        confidence_threshold = confidence if confidence is not None else MIN_CONFIDENCE_FOR_MATCH

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
    ):
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
            print(
                f"Unsupported file type. Supported file types are CSV, TSV, JSON lines, "
                f"BSON, Parquet, JSON. Error: {e}"
            )
            return []
        except Exception as e:
            logging.error(f"Unexpected error opening file {filename}: {e}", exc_info=True)
            print(f"Unexpected error processing file {filename}: {e}")
            return []
        # Process file efficiently - collect items from iterator
        # Note: For very large files, consider implementing streaming processing
        # in scan_data() method to avoid loading entire file into memory
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
            data_file.close()
            # Clear items from memory after processing
            del items


    def scan_bulk(
        self,
        dirname,
        delimiter=None,
        tagname=None,
        limit=1000,
        encoding="utf8",
        contexts=None,
        langs=None,
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
    ):
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

    def scan_db(
        self,
        connectstr="sqlite:///test.db",
        schema=None,
        limit=1000,
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
        batch_size=DEFAULT_BATCH_SIZE,
    ):
        """SQL alchemy way to scan any database"""
        from sqlalchemy import create_engine, inspect, text
        import sqlalchemy.exc
        import re

        dbtype = connectstr.split(":", 1)[0].lower()
        if not self.quiet:
            print("Connecting to %s" % (connectstr))
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
                    row_batch = queryres.fetchmany(effective_batch)
                    while row_batch:
                        batch_dicts = [dict(u) for u in row_batch]
                        items.extend(batch_dicts)
                        if fetch_progress:
                            fetch_progress.update(len(batch_dicts))
                        if len(items) >= limit:
                            items = items[:limit]
                            break
                        row_batch = queryres.fetchmany(effective_batch)
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
        host="localhost",
        port=27017,
        dbname="test",
        username=None,
        password=None,
        limit=1000,
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
        batch_size=DEFAULT_BATCH_SIZE,
    ):
        """Scan entire MongoDB database"""
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


@server_app.command('run')
def server_run(
    host: str = typer.Option("127.0.0.1", "--host", help="IP or hostname to bind the API server to"),
    port: int = typer.Option(10399, "--port", help="Port where the API server listens"),
    debug: bool = typer.Option(False, "--debug", help="Enable verbose server debug logging"),
):
    """Start the API server that exposes the classifier via HTTP."""
    logging.info("Run server with classifier API")
    from metacrafter.server.manager import run_server

    run_server(host, port, debug)


@rules_app.command('stats')
def rules_stats(
    debug: bool = typer.Option(False, help="Enable debug logging"),
    rulepath: Optional[str] = typer.Option(None, "--rulepath", help="Custom rule path(s), comma-separated for multiple paths"),
    country_codes: Optional[str] = typer.Option(None, "--country-codes", help="Comma-separated ISO country codes to include"),
):
    """Display aggregate statistics about loaded rules."""
    # Parse rulepath if provided (comma-separated)
    rulepath_list = None
    if rulepath:
        rulepath_list = [rp.strip() for rp in rulepath.split(',') if rp.strip()]
    country_code_list = None
    if country_codes:
        country_code_list = [
            cc.strip().lower() for cc in country_codes.split(",") if cc.strip()
        ]
    acmd = CrafterCmd(debug=debug, rulepath=rulepath_list, country_codes=country_code_list)
    acmd.rules_dumpstats()

@rules_app.command('list')
def rules_list(
    debug: bool = typer.Option(False, help="Enable debug logging"),
    rulepath: Optional[str] = typer.Option(None, "--rulepath", help="Custom rule path(s), comma-separated for multiple paths"),
    country_codes: Optional[str] = typer.Option(None, "--country-codes", help="Comma-separated ISO country codes to include"),
):
    """List every rule along with key metadata."""
    # Parse rulepath if provided (comma-separated)
    rulepath_list = None
    if rulepath:
        rulepath_list = [rp.strip() for rp in rulepath.split(',') if rp.strip()]
    country_code_list = None
    if country_codes:
        country_code_list = [
            cc.strip().lower() for cc in country_codes.split(",") if cc.strip()
        ]
    acmd = CrafterCmd(debug=debug, rulepath=rulepath_list, country_codes=country_code_list)
    acmd.rules_list()

@scan_app.command('file')
def scan_file(
    filename: str = typer.Argument(..., help="Path to the data file to scan (CSV, JSON, NDJSON, etc.)"),
    delimiter: str = typer.Option(None, help="CSV/TSV delimiter character"),
    tagname: str = typer.Option(None, help="XML tag name for data extraction"),
    encoding: str = typer.Option(None, help="Character encoding for text files (e.g. utf8)"),
    limit: int = typer.Option(100, help="Maximum records to process per field"),
    contexts: str = typer.Option(None, help="Comma-separated list of context filters"),
    langs: str = typer.Option(None, help="Comma-separated list of language filters"),
    format: str = typer.Option("short", help="Output format: short or full"),
    output: str = typer.Option(None, "--output", "-o", help="Output file path"),
    remote: str = typer.Option(None, help="Remote server URL for API calls"),
    debug: bool = typer.Option(False, help="Enable debug logging"),
    confidence: float = typer.Option(None, "--confidence", "-c", help="Minimum confidence threshold (0-100, default: 5.0)"),
    stop_on_match: bool = typer.Option(False, "--stop-on-match", help="Stop processing after first rule match"),
    no_dates: bool = typer.Option(False, "--no-dates", help="Disable date pattern matching"),
    include_imprecise: bool = typer.Option(False, "--include-imprecise", help="Include imprecise rules in matching"),
    include_empty: bool = typer.Option(False, "--include-empty", help="Include empty values in confidence calculations"),
    fields: str = typer.Option(None, "--fields", help="Comma-separated list of specific fields to process"),
    rulepath: Optional[str] = typer.Option(None, "--rulepath", help="Custom rule path(s), comma-separated for multiple paths"),
    country_codes: Optional[str] = typer.Option(None, "--country-codes", help="Comma-separated ISO country codes to include"),
    output_format: str = typer.Option("table", "--output-format", help="Output format: table, json, yaml, or csv"),
    stats_only: bool = typer.Option(False, "--stats-only", help="Output only statistics without classification"),
    verbose: bool = typer.Option(False, "--verbose", help="Enable verbose logging"),
    quiet: bool = typer.Option(False, "--quiet", help="Reduce non-essential output"),
    progress: bool = typer.Option(False, "--progress", help="Show progress while processing"),
    compression: str = typer.Option("auto", "--compression", help="Force compression handling: auto, none, gz, bz2, ..."),
    dict_share: float = typer.Option(None, "--dict-share", help="Dictionary share threshold (percentage)"),
    empty_values: str = typer.Option(None, "--empty-values", help="Comma-separated values treated as empty (use \"\" for empty string)"),
    timeout: float = typer.Option(None, "--timeout", help="Remote request timeout in seconds (<=0 disables)"),
    retries: int = typer.Option(0, "--retries", help="Retry attempts for remote requests"),
    retry_delay: float = typer.Option(DEFAULT_RETRY_DELAY, "--retry-delay", help="Delay between remote retries in seconds"),
    stdout: bool = typer.Option(False, "--stdout", help="Write output to stdout"),
    pretty: bool = typer.Option(False, "--pretty", help="Pretty-print JSON output"),
    indent: Optional[int] = typer.Option(None, "--indent", help="Custom JSON indent (0 for compact)"),
    table_format: str = typer.Option(DEFAULT_TABLE_FORMAT, "--table-format", help="Tabulate format for table outputs"),
):
    """Scan a single file and classify its fields."""
    # Parse rulepath if provided (comma-separated)
    rulepath_list = None
    if rulepath:
        rulepath_list = [rp.strip() for rp in rulepath.split(',') if rp.strip()]
    country_code_list = None
    if country_codes:
        country_code_list = [
            cc.strip().lower() for cc in country_codes.split(",") if cc.strip()
        ]
    
    if verbose and quiet:
        raise typer.BadParameter("Cannot use --verbose and --quiet together")
    if dict_share is not None and dict_share <= 0:
        raise typer.BadParameter("--dict-share must be greater than 0")
    if retries < 0:
        raise typer.BadParameter("--retries must be >= 0")
    if retry_delay < 0:
        raise typer.BadParameter("--retry-delay must be >= 0")
    if timeout is not None and timeout < 0:
        raise typer.BadParameter("--timeout must be >= 0")
    allowed_output_formats = {"table", "json", "yaml", "csv"}
    output_format_value = output_format.lower()
    if output_format_value not in allowed_output_formats:
        raise typer.BadParameter(
            f"Invalid output format '{output_format}'. Choose from {', '.join(sorted(allowed_output_formats))}"
        )

    empty_values_list = _split_option_list(empty_values)
    json_indent_value = indent if indent is not None else DEFAULT_JSON_INDENT
    if indent is not None and indent <= 0:
        json_indent_value = None
    elif indent is None and pretty:
        json_indent_value = DEFAULT_JSON_INDENT
    output_target = _resolve_output_target(output, stdout)
    table_format_value = table_format or DEFAULT_TABLE_FORMAT

    acmd = CrafterCmd(
        remote,
        debug,
        rulepath=rulepath_list,
        country_codes=country_code_list,
        verbose=verbose,
        quiet=quiet,
        progress=progress,
        table_format=table_format_value,
        json_indent=json_indent_value,
        timeout=timeout,
        retries=retries,
        retry_delay=retry_delay,
    )
    acmd.scan_file(
        filename=filename,
        delimiter=delimiter,
        tagname=tagname,
        limit=int(limit),
        encoding=encoding,
        contexts=contexts,
        langs=langs,
        dformat=format,
        output=output_target,
        confidence=confidence,
        stop_on_match=stop_on_match,
        parse_dates=not no_dates,
        ignore_imprecise=not include_imprecise,
        except_empty=not include_empty,
        fields=fields,
        output_format=output_format_value,
        stats_only=stats_only,
        dict_share=dict_share,
        empty_values=empty_values_list,
        compression=compression,
    )

@scan_app.command('sql')
def scan_db(
    connstr: str = typer.Argument(..., help="SQLAlchemy connection string for the target database"),
    schema: str = typer.Option(None, help="Database schema name"),
    limit: int = typer.Option(1000, help="Maximum records to process per field"),
    contexts: str = typer.Option(None, help="Comma-separated list of context filters"),
    langs: str = typer.Option(None, help="Comma-separated list of language filters"),
    format: str = typer.Option("short", help="Output format: short or full"),
    output: str = typer.Option(None, "--output", "-o", help="Output file path"),
    remote: str = typer.Option(None, help="Remote server URL for API calls"),
    debug: bool = typer.Option(False, help="Enable debug logging"),
    confidence: float = typer.Option(None, "--confidence", "-c", help="Minimum confidence threshold (0-100, default: 5.0)"),
    stop_on_match: bool = typer.Option(False, "--stop-on-match", help="Stop processing after first rule match"),
    no_dates: bool = typer.Option(False, "--no-dates", help="Disable date pattern matching"),
    include_imprecise: bool = typer.Option(False, "--include-imprecise", help="Include imprecise rules in matching"),
    include_empty: bool = typer.Option(False, "--include-empty", help="Include empty values in confidence calculations"),
    fields: str = typer.Option(None, "--fields", help="Comma-separated list of specific fields to process"),
    rulepath: Optional[str] = typer.Option(None, "--rulepath", help="Custom rule path(s), comma-separated for multiple paths"),
    country_codes: Optional[str] = typer.Option(None, "--country-codes", help="Comma-separated ISO country codes to include"),
    output_format: str = typer.Option("table", "--output-format", help="Output format: table, json, yaml, or csv"),
    stats_only: bool = typer.Option(False, "--stats-only", help="Output only statistics without classification"),
    verbose: bool = typer.Option(False, "--verbose", help="Enable verbose logging"),
    quiet: bool = typer.Option(False, "--quiet", help="Reduce non-essential output"),
    progress: bool = typer.Option(False, "--progress", help="Show progress while processing"),
    batch_size: int = typer.Option(DEFAULT_BATCH_SIZE, "--batch-size", help="Number of rows fetched per batch"),
    dict_share: float = typer.Option(None, "--dict-share", help="Dictionary share threshold (percentage)"),
    empty_values: str = typer.Option(None, "--empty-values", help="Comma-separated values treated as empty"),
    timeout: float = typer.Option(None, "--timeout", help="Remote request timeout in seconds (<=0 disables)"),
    retries: int = typer.Option(0, "--retries", help="Retry attempts for remote requests"),
    retry_delay: float = typer.Option(DEFAULT_RETRY_DELAY, "--retry-delay", help="Delay between remote retries in seconds"),
    stdout: bool = typer.Option(False, "--stdout", help="Write output to stdout"),
    pretty: bool = typer.Option(False, "--pretty", help="Pretty-print JSON output"),
    indent: Optional[int] = typer.Option(None, "--indent", help="Custom JSON indent (0 for compact)"),
    table_format: str = typer.Option(DEFAULT_TABLE_FORMAT, "--table-format", help="Tabulate format for table outputs"),
):
    """Scan a SQL database using a SQLAlchemy connection string."""
    # Parse rulepath if provided (comma-separated)
    rulepath_list = None
    if rulepath:
        rulepath_list = [rp.strip() for rp in rulepath.split(',') if rp.strip()]
    country_code_list = None
    if country_codes:
        country_code_list = [
            cc.strip().lower() for cc in country_codes.split(",") if cc.strip()
        ]
    
    if verbose and quiet:
        raise typer.BadParameter("Cannot use --verbose and --quiet together")
    allowed_output_formats = {"table", "json", "yaml", "csv"}
    output_format_value = output_format.lower()
    if output_format_value not in allowed_output_formats:
        raise typer.BadParameter(
            f"Invalid output format '{output_format}'. Choose from {', '.join(sorted(allowed_output_formats))}"
        )

    if verbose and quiet:
        raise typer.BadParameter("Cannot use --verbose and --quiet together")
    if dict_share is not None and dict_share <= 0:
        raise typer.BadParameter("--dict-share must be greater than 0")
    if batch_size <= 0:
        raise typer.BadParameter("--batch-size must be greater than 0")
    if retries < 0:
        raise typer.BadParameter("--retries must be >= 0")
    if retry_delay < 0:
        raise typer.BadParameter("--retry-delay must be >= 0")
    if timeout is not None and timeout < 0:
        raise typer.BadParameter("--timeout must be >= 0")
    allowed_output_formats = {"table", "json", "yaml", "csv"}
    output_format_value = output_format.lower()
    if output_format_value not in allowed_output_formats:
        raise typer.BadParameter(
            f"Invalid output format '{output_format}'. Choose from {', '.join(sorted(allowed_output_formats))}"
        )
    empty_values_list = _split_option_list(empty_values)
    json_indent_value = indent if indent is not None else DEFAULT_JSON_INDENT
    if indent is not None and indent <= 0:
        json_indent_value = None
    elif indent is None and pretty:
        json_indent_value = DEFAULT_JSON_INDENT
    output_target = _resolve_output_target(output, stdout)
    table_format_value = table_format or DEFAULT_TABLE_FORMAT

    acmd = CrafterCmd(
        remote,
        debug,
        rulepath=rulepath_list,
        country_codes=country_code_list,
        verbose=verbose,
        quiet=quiet,
        progress=progress,
        table_format=table_format_value,
        json_indent=json_indent_value,
        timeout=timeout,
        retries=retries,
        retry_delay=retry_delay,
    )
    acmd.scan_db(
        connectstr=connstr,
        schema=schema,
        limit=int(limit),
        contexts=contexts,
        langs=langs,
        dformat=format,
        output=output_target,
        confidence=confidence,
        stop_on_match=stop_on_match,
        parse_dates=not no_dates,
        ignore_imprecise=not include_imprecise,
        except_empty=not include_empty,
        fields=fields,
        output_format=output_format_value,
        stats_only=stats_only,
        dict_share=dict_share,
        empty_values=empty_values_list,
        batch_size=batch_size,
    )


@scan_app.command('mongodb')
def scan_mongodb(
    host: str = typer.Argument(..., help="MongoDB host or connection URI to scan"),
    port: int = typer.Option(27017, help="MongoDB port number"),
    dbname: str = typer.Option(None, help="MongoDB database name"),
    username: str = typer.Option(None, help="MongoDB username"),
    password: str = typer.Option(None, help="MongoDB password"),
    limit: int = typer.Option(1000, help="Maximum records to process per field"),
    contexts: str = typer.Option(None, help="Comma-separated list of context filters"),
    langs: str = typer.Option(None, help="Comma-separated list of language filters"),
    format: str = typer.Option("short", help="Output format: short or full"),
    output: str = typer.Option(None, "--output", "-o", help="Output file path"),
    remote: str = typer.Option(None, help="Remote server URL for API calls"),
    debug: bool = typer.Option(False, help="Enable debug logging"),
    confidence: float = typer.Option(None, "--confidence", "-c", help="Minimum confidence threshold (0-100, default: 5.0)"),
    stop_on_match: bool = typer.Option(False, "--stop-on-match", help="Stop processing after first rule match"),
    no_dates: bool = typer.Option(False, "--no-dates", help="Disable date pattern matching"),
    include_imprecise: bool = typer.Option(False, "--include-imprecise", help="Include imprecise rules in matching"),
    include_empty: bool = typer.Option(False, "--include-empty", help="Include empty values in confidence calculations"),
    fields: str = typer.Option(None, "--fields", help="Comma-separated list of specific fields to process"),
    rulepath: Optional[str] = typer.Option(None, "--rulepath", help="Custom rule path(s), comma-separated for multiple paths"),
    country_codes: Optional[str] = typer.Option(None, "--country-codes", help="Comma-separated ISO country codes to include"),
    output_format: str = typer.Option("table", "--output-format", help="Output format: table, json, yaml, or csv"),
    stats_only: bool = typer.Option(False, "--stats-only", help="Output only statistics without classification"),
    verbose: bool = typer.Option(False, "--verbose", help="Enable verbose logging"),
    quiet: bool = typer.Option(False, "--quiet", help="Reduce non-essential output"),
    progress: bool = typer.Option(False, "--progress", help="Show progress while processing"),
    batch_size: int = typer.Option(DEFAULT_BATCH_SIZE, "--batch-size", help="Number of documents fetched per batch"),
    dict_share: float = typer.Option(None, "--dict-share", help="Dictionary share threshold (percentage)"),
    empty_values: str = typer.Option(None, "--empty-values", help="Comma-separated values treated as empty"),
    timeout: float = typer.Option(None, "--timeout", help="Remote request timeout in seconds (<=0 disables)"),
    retries: int = typer.Option(0, "--retries", help="Retry attempts for remote requests"),
    retry_delay: float = typer.Option(DEFAULT_RETRY_DELAY, "--retry-delay", help="Delay between remote retries in seconds"),
    stdout: bool = typer.Option(False, "--stdout", help="Write output to stdout"),
    pretty: bool = typer.Option(False, "--pretty", help="Pretty-print JSON output"),
    indent: Optional[int] = typer.Option(None, "--indent", help="Custom JSON indent (0 for compact)"),
    table_format: str = typer.Option(DEFAULT_TABLE_FORMAT, "--table-format", help="Tabulate format for table outputs"),
):
    """Scan a MongoDB deployment and classify fields within its collections."""
    # Parse rulepath if provided (comma-separated)
    rulepath_list = None
    if rulepath:
        rulepath_list = [rp.strip() for rp in rulepath.split(',') if rp.strip()]
    country_code_list = None
    if country_codes:
        country_code_list = [
            cc.strip().lower() for cc in country_codes.split(",") if cc.strip()
        ]
    
    if verbose and quiet:
        raise typer.BadParameter("Cannot use --verbose and --quiet together")
    allowed_output_formats = {"table", "json", "yaml", "csv"}
    output_format_value = output_format.lower()
    if output_format_value not in allowed_output_formats:
        raise typer.BadParameter(
            f"Invalid output format '{output_format}'. Choose from {', '.join(sorted(allowed_output_formats))}"
        )

    if verbose and quiet:
        raise typer.BadParameter("Cannot use --verbose and --quiet together")
    if dict_share is not None and dict_share <= 0:
        raise typer.BadParameter("--dict-share must be greater than 0")
    if batch_size <= 0:
        raise typer.BadParameter("--batch-size must be greater than 0")
    if retries < 0:
        raise typer.BadParameter("--retries must be >= 0")
    if retry_delay < 0:
        raise typer.BadParameter("--retry-delay must be >= 0")
    if timeout is not None and timeout < 0:
        raise typer.BadParameter("--timeout must be >= 0")
    allowed_output_formats = {"table", "json", "yaml", "csv"}
    output_format_value = output_format.lower()
    if output_format_value not in allowed_output_formats:
        raise typer.BadParameter(
            f"Invalid output format '{output_format}'. Choose from {', '.join(sorted(allowed_output_formats))}"
        )
    empty_values_list = _split_option_list(empty_values)
    json_indent_value = indent if indent is not None else DEFAULT_JSON_INDENT
    if indent is not None and indent <= 0:
        json_indent_value = None
    elif indent is None and pretty:
        json_indent_value = DEFAULT_JSON_INDENT
    output_target = _resolve_output_target(output, stdout)
    table_format_value = table_format or DEFAULT_TABLE_FORMAT

    acmd = CrafterCmd(
        remote,
        debug,
        rulepath=rulepath_list,
        country_codes=country_code_list,
        verbose=verbose,
        quiet=quiet,
        progress=progress,
        table_format=table_format_value,
        json_indent=json_indent_value,
        timeout=timeout,
        retries=retries,
        retry_delay=retry_delay,
    )
    acmd.scan_mongodb(
        host=host,
        port=int(port),
        dbname=dbname,
        username=username,
        password=password,
        limit=int(limit),
        contexts=contexts,
        langs=langs,
        dformat=format,
        output=output_target,
        confidence=confidence,
        stop_on_match=stop_on_match,
        parse_dates=not no_dates,
        ignore_imprecise=not include_imprecise,
        except_empty=not include_empty,
        fields=fields,
        output_format=output_format_value,
        stats_only=stats_only,
        dict_share=dict_share,
        empty_values=empty_values_list,
        batch_size=batch_size,
    )


@scan_app.command('bulk')
def scan_bulk(
    dirname: str = typer.Argument(..., help="Directory containing files to scan recursively"),
    delimiter: str = typer.Option(None, help="CSV/TSV delimiter character"),
    tagname: str = typer.Option(None, help="XML tag name for data extraction"),
    encoding: str = typer.Option("utf8", help="Default encoding for files in the directory"),
    limit: int = typer.Option(100, help="Maximum records to process per field"),
    contexts: str = typer.Option(None, help="Comma-separated list of context filters"),
    langs: str = typer.Option(None, help="Comma-separated list of language filters"),
    format: str = typer.Option(None, help="Output format (not used in bulk mode)"),
    output: str = typer.Option(None, "--output", "-o", help="Output file path"),
    remote: str = typer.Option(None, help="Remote server URL for API calls"),
    debug: bool = typer.Option(False, help="Enable debug logging"),
    confidence: float = typer.Option(None, "--confidence", "-c", help="Minimum confidence threshold (0-100, default: 5.0)"),
    stop_on_match: bool = typer.Option(False, "--stop-on-match", help="Stop processing after first rule match"),
    no_dates: bool = typer.Option(False, "--no-dates", help="Disable date pattern matching"),
    include_imprecise: bool = typer.Option(False, "--include-imprecise", help="Include imprecise rules in matching"),
    include_empty: bool = typer.Option(False, "--include-empty", help="Include empty values in confidence calculations"),
    fields: str = typer.Option(None, "--fields", help="Comma-separated list of specific fields to process"),
    rulepath: Optional[str] = typer.Option(None, "--rulepath", help="Custom rule path(s), comma-separated for multiple paths"),
    country_codes: Optional[str] = typer.Option(None, "--country-codes", help="Comma-separated ISO country codes to include"),
    output_format: str = typer.Option("table", "--output-format", help="Output format: table, json, yaml, or csv"),
    stats_only: bool = typer.Option(False, "--stats-only", help="Output only statistics without classification"),
    verbose: bool = typer.Option(False, "--verbose", help="Enable verbose logging"),
    quiet: bool = typer.Option(False, "--quiet", help="Reduce non-essential output"),
    progress: bool = typer.Option(False, "--progress", help="Show progress while processing"),
    compression: str = typer.Option("auto", "--compression", help="Force compression handling: auto, none, gz, bz2, ..."),
    dict_share: float = typer.Option(None, "--dict-share", help="Dictionary share threshold (percentage)"),
    empty_values: str = typer.Option(None, "--empty-values", help="Comma-separated values treated as empty"),
    timeout: float = typer.Option(None, "--timeout", help="Remote request timeout in seconds (<=0 disables)"),
    retries: int = typer.Option(0, "--retries", help="Retry attempts for remote requests"),
    retry_delay: float = typer.Option(DEFAULT_RETRY_DELAY, "--retry-delay", help="Delay between remote retries in seconds"),
    stdout: bool = typer.Option(False, "--stdout", help="Write output to stdout"),
    pretty: bool = typer.Option(False, "--pretty", help="Pretty-print JSON output"),
    indent: Optional[int] = typer.Option(None, "--indent", help="Custom JSON indent (0 for compact)"),
    table_format: str = typer.Option(DEFAULT_TABLE_FORMAT, "--table-format", help="Tabulate format for table outputs"),
):
    """Scan every supported file inside a directory tree."""
    # Parse rulepath if provided (comma-separated)
    rulepath_list = None
    if rulepath:
        rulepath_list = [rp.strip() for rp in rulepath.split(',') if rp.strip()]
    country_code_list = None
    if country_codes:
        country_code_list = [
            cc.strip().lower() for cc in country_codes.split(",") if cc.strip()
        ]
    
    if verbose and quiet:
        raise typer.BadParameter("Cannot use --verbose and --quiet together")
    allowed_output_formats = {"table", "json", "yaml", "csv"}
    output_format_value = output_format.lower()
    if output_format_value not in allowed_output_formats:
        raise typer.BadParameter(
            f"Invalid output format '{output_format}'. Choose from {', '.join(sorted(allowed_output_formats))}"
        )

    if verbose and quiet:
        raise typer.BadParameter("Cannot use --verbose and --quiet together")
    if dict_share is not None and dict_share <= 0:
        raise typer.BadParameter("--dict-share must be greater than 0")
    if retries < 0:
        raise typer.BadParameter("--retries must be >= 0")
    if retry_delay < 0:
        raise typer.BadParameter("--retry-delay must be >= 0")
    if timeout is not None and timeout < 0:
        raise typer.BadParameter("--timeout must be >= 0")
    allowed_output_formats = {"table", "json", "yaml", "csv"}
    output_format_value = output_format.lower()
    if output_format_value not in allowed_output_formats:
        raise typer.BadParameter(
            f"Invalid output format '{output_format}'. Choose from {', '.join(sorted(allowed_output_formats))}"
        )
    empty_values_list = _split_option_list(empty_values)
    json_indent_value = indent if indent is not None else DEFAULT_JSON_INDENT
    if indent is not None and indent <= 0:
        json_indent_value = None
    elif indent is None and pretty:
        json_indent_value = DEFAULT_JSON_INDENT
    output_target = _resolve_output_target(output, stdout)
    table_format_value = table_format or DEFAULT_TABLE_FORMAT

    acmd = CrafterCmd(
        remote,
        debug,
        rulepath=rulepath_list,
        country_codes=country_code_list,
        verbose=verbose,
        quiet=quiet,
        progress=progress,
        table_format=table_format_value,
        json_indent=json_indent_value,
        timeout=timeout,
        retries=retries,
        retry_delay=retry_delay,
    )
    acmd.scan_bulk(
        dirname=dirname,
        delimiter=delimiter,
        tagname=tagname,
        limit=int(limit),
        encoding=encoding,
        contexts=contexts,
        langs=langs,
        output=output_target,
        confidence=confidence,
        stop_on_match=stop_on_match,
        parse_dates=not no_dates,
        ignore_imprecise=not include_imprecise,
        except_empty=not include_empty,
        fields=fields,
        output_format=output_format_value,
        stats_only=stats_only,
        dict_share=dict_share,
        empty_values=empty_values_list,
        compression=compression,
    )

