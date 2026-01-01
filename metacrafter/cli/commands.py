# -*- coding: utf8 -*-
"""CLI command definitions for Metacrafter.

This module contains all Typer command functions extracted from core.py
as part of Phase 1 refactoring to improve code organization.

All commands are registered with Typer app instances imported from core.py.
"""
import json
import logging
import os
from typing import Optional

import typer

from metacrafter.config import ConfigLoader

# Import Typer app instances, CrafterCmd, helper functions, and constants from core
from metacrafter.core import (
    CrafterCmd,
    DEFAULT_BATCH_SIZE,
    DEFAULT_JSON_INDENT,
    DEFAULT_RETRY_DELAY,
    DEFAULT_TABLE_FORMAT,
    _resolve_output_target,
    _split_option_list,
    export_app,
    rules_app,
    scan_app,
    server_app,
)


def register_commands():
    """Register all CLI commands with their respective Typer app instances.
    
    This function is called automatically when the module is imported.
    Commands are registered via decorators on function definitions.
    """
    # Commands are registered via decorators, so this function
    # is mainly for documentation and potential future dynamic registration
    pass


# ============================================================================
# Server Commands
# ============================================================================

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


# ============================================================================
# Rules Commands
# ============================================================================

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
    output_format: str = typer.Option("table", "--output-format", help="Output format: table, json, yaml, or csv"),
    output: Optional[str] = typer.Option(None, "--output", "-o", help="Output file path"),
    table_format: str = typer.Option(DEFAULT_TABLE_FORMAT, "--table-format", help="Tabulate format for table outputs"),
):
    """List every rule along with key metadata including contexts, countries, and languages."""
    # Parse rulepath if provided (comma-separated)
    rulepath_list = None
    if rulepath:
        rulepath_list = [rp.strip() for rp in rulepath.split(',') if rp.strip()]
    country_code_list = None
    if country_codes:
        country_code_list = [
            cc.strip().lower() for cc in country_codes.split(",") if cc.strip()
        ]
    
    # Validate output format
    allowed_output_formats = {"table", "json", "yaml", "csv"}
    output_format_value = output_format.lower()
    if output_format_value not in allowed_output_formats:
        raise typer.BadParameter(
            f"Invalid output format '{output_format}'. Choose from {', '.join(sorted(allowed_output_formats))}"
        )
    
    acmd = CrafterCmd(
        debug=debug, 
        rulepath=rulepath_list, 
        country_codes=country_code_list,
        table_format=table_format
    )
    acmd.rules_list(output_format=output_format_value, output=output)


# ============================================================================
# Scan Commands
# ============================================================================

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
    classification_mode: str = typer.Option(None, "--classification-mode", help="Classification mode: rules (default), llm, or hybrid"),
    llm_only: bool = typer.Option(False, "--llm-only", help="Use only LLM classification (shortcut for --classification-mode llm)"),
    use_llm: bool = typer.Option(False, "--use-llm", help="Use LLM as fallback (shortcut for --classification-mode hybrid)"),
    llm_provider: str = typer.Option("openai", "--llm-provider", help="LLM provider: openai, openrouter, ollama, lmstudio, perplexity"),
    llm_registry: Optional[str] = typer.Option(None, "--llm-registry", help="Path to registry JSONL file"),
    llm_index: Optional[str] = typer.Option(None, "--llm-index", help="Path to vector index directory"),
    llm_model: Optional[str] = typer.Option(None, "--llm-model", help="LLM model name (provider-specific default if not specified)"),
    llm_base_url: Optional[str] = typer.Option(None, "--llm-base-url", help="Base URL for provider (for Ollama, LM Studio custom URLs)"),
    llm_api_key: Optional[str] = typer.Option(None, "--llm-api-key", help="API key for provider (or use provider-specific env var)"),
    llm_min_confidence: float = typer.Option(50.0, "--llm-min-confidence", help="Minimum confidence for LLM results (default: 50.0)"),
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
    if llm_min_confidence < 0 or llm_min_confidence > 100:
        raise typer.BadParameter("--llm-min-confidence must be between 0 and 100")
    
    # Determine classification mode from flags
    if classification_mode:
        if classification_mode.lower() not in ("rules", "llm", "hybrid"):
            raise typer.BadParameter("--classification-mode must be one of: rules, llm, hybrid")
        final_classification_mode = classification_mode.lower()
    elif llm_only:
        final_classification_mode = "llm"
    elif use_llm:
        final_classification_mode = "hybrid"
    else:
        final_classification_mode = None  # Use default from CrafterCmd
    
    # Determine if LLM should be enabled
    use_llm_flag = final_classification_mode in ("llm", "hybrid") if final_classification_mode else False
    llm_only_flag = final_classification_mode == "llm" if final_classification_mode else False
    
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
        use_llm=use_llm_flag,
        llm_only=llm_only_flag,
        llm_provider=llm_provider,
        llm_registry_path=llm_registry,
        llm_index_path=llm_index,
        llm_model=llm_model,
        llm_api_key=llm_api_key,
        llm_base_url=llm_base_url,
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
        classification_mode=final_classification_mode,
        llm_min_confidence=llm_min_confidence,
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


# ============================================================================
# Export Commands
# ============================================================================

@export_app.command('datahub')
def export_datahub(
    file: str = typer.Argument(..., help="Path to JSON file containing Metacrafter scan results"),
    dataset_urn: str = typer.Option(..., "--dataset-urn", help="DataHub dataset URN (e.g., urn:li:dataset:(platform,name,env))"),
    datahub_url: str = typer.Option(None, "--datahub-url", help="DataHub GMS server URL (defaults to DATAHUB_URL env var)"),
    token: str = typer.Option(None, "--token", help="DataHub authentication token (defaults to DATAHUB_TOKEN env var)"),
    timeout: float = typer.Option(30.0, "--timeout", help="Request timeout in seconds"),
    replace: bool = typer.Option(False, "--replace", help="Replace existing tags/properties instead of merging"),
    add_pii_tags: bool = typer.Option(True, "--add-pii-tags/--no-pii-tags", help="Add PII tags to fields"),
    add_datatype_tags: bool = typer.Option(True, "--add-datatype-tags/--no-datatype-tags", help="Add datatype tags"),
    link_glossary_terms: bool = typer.Option(True, "--link-glossary-terms/--no-glossary-terms", help="Link glossary terms"),
    add_properties: bool = typer.Option(True, "--add-properties/--no-properties", help="Add custom properties"),
    min_confidence: float = typer.Option(0.0, "--min-confidence", help="Minimum confidence threshold (0-100)"),
    verbose: bool = typer.Option(False, "--verbose", help="Enable verbose logging"),
    quiet: bool = typer.Option(False, "--quiet", help="Reduce non-essential output"),
):
    """Export Metacrafter scan results to DataHub metadata catalog.
    
    This command reads a JSON file containing Metacrafter scan results and exports
    the classification metadata (PII labels, datatypes, confidence scores) to DataHub
    as tags, glossary terms, and custom properties on dataset schema fields.
    
    Example:
        metacrafter export datahub results.json \\
            --dataset-urn "urn:li:dataset:(urn:li:dataPlatform:postgres,users,PROD)" \\
            --datahub-url "http://localhost:8080" \\
            --token "your-token"
    """
    if verbose and quiet:
        raise typer.BadParameter("Cannot use --verbose and --quiet together")
    
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    if quiet:
        logging.getLogger().setLevel(logging.WARNING)
    
    # Get DataHub URL from parameter, env var, or config
    if not datahub_url:
        datahub_url = os.getenv("DATAHUB_URL")
    if not datahub_url:
        # Try to get from config
        config = ConfigLoader.load_config()
        if config and "datahub" in config and "url" in config["datahub"]:
            datahub_url = config["datahub"]["url"]
    
    if not datahub_url:
        raise typer.BadParameter(
            "DataHub URL is required. Provide --datahub-url or set DATAHUB_URL environment variable."
        )
    
    # Get token from parameter, env var, or config
    if not token:
        token = os.getenv("DATAHUB_TOKEN")
    if not token:
        config = ConfigLoader.load_config()
        if config and "datahub" in config and "token" in config["datahub"]:
            token = config["datahub"]["token"]
    
    # Load scan results from file
    if not os.path.exists(file):
        raise typer.BadParameter(f"Scan results file not found: {file}")
    
    try:
        with open(file, 'r', encoding='utf8') as f:
            scan_report = json.load(f)
    except json.JSONDecodeError as e:
        raise typer.BadParameter(f"Invalid JSON in scan results file: {e}")
    except Exception as e:
        raise typer.BadParameter(f"Error reading scan results file: {e}")
    
    # Import DataHub exporter
    try:
        from metacrafter.integrations.datahub import DataHubExporter
    except ImportError as e:
        raise typer.BadParameter(
            f"DataHub integration not available: {e}. "
            "Install with: pip install 'acryl-datahub[datahub-rest]'"
        )
    
    # Initialize exporter
    try:
        exporter = DataHubExporter(
            datahub_url=datahub_url,
            token=token,
            timeout=timeout,
            replace=replace,
        )
    except Exception as e:
        raise typer.BadParameter(f"Failed to initialize DataHub exporter: {e}")
    
    # Export scan results
    if not quiet:
        print(f"Exporting scan results to DataHub: {datahub_url}")
        print(f"Dataset URN: {dataset_urn}")
    
    try:
        stats = exporter.export_scan_results(
            dataset_urn=dataset_urn,
            scan_report=scan_report,
            add_pii_tags=add_pii_tags,
            add_datatype_tags=add_datatype_tags,
            link_glossary_terms=link_glossary_terms,
            add_properties=add_properties,
            min_confidence=min_confidence,
        )
        
        if not quiet:
            print("\nExport completed successfully!")
            print(f"Fields processed: {stats['fields_processed']}")
            print(f"Tags added: {stats['tags_added']}")
            print(f"Glossary terms linked: {stats['glossary_terms_linked']}")
            print(f"Properties added: {stats['properties_added']}")
            if stats['errors']:
                print(f"\nErrors encountered: {len(stats['errors'])}")
                for error in stats['errors']:
                    print(f"  - {error}")
        
        if stats['errors']:
            raise typer.Exit(code=1)
            
    except Exception as e:
        if verbose:
            import traceback
            traceback.print_exc()
        raise typer.BadParameter(f"Failed to export to DataHub: {e}")


@export_app.command('openmetadata')
def export_openmetadata(
    file: str = typer.Argument(..., help="Path to JSON file containing Metacrafter scan results"),
    table_fqn: str = typer.Option(..., "--table-fqn", help="OpenMetadata table FQN (e.g., postgres.default.public.users)"),
    openmetadata_url: str = typer.Option(None, "--openmetadata-url", help="OpenMetadata server URL (defaults to OPENMETADATA_URL env var)"),
    token: str = typer.Option(None, "--token", help="OpenMetadata JWT authentication token (defaults to OPENMETADATA_TOKEN env var)"),
    timeout: float = typer.Option(30.0, "--timeout", help="Request timeout in seconds"),
    replace: bool = typer.Option(False, "--replace", help="Replace existing tags/properties instead of merging"),
    add_pii_tags: bool = typer.Option(True, "--add-pii-tags/--no-pii-tags", help="Add PII tags to fields"),
    add_datatype_tags: bool = typer.Option(True, "--add-datatype-tags/--no-datatype-tags", help="Add datatype tags"),
    link_glossary_terms: bool = typer.Option(True, "--link-glossary-terms/--no-glossary-terms", help="Link glossary terms"),
    add_properties: bool = typer.Option(True, "--add-properties/--no-properties", help="Add custom properties"),
    min_confidence: float = typer.Option(0.0, "--min-confidence", help="Minimum confidence threshold (0-100)"),
    verbose: bool = typer.Option(False, "--verbose", help="Enable verbose logging"),
    quiet: bool = typer.Option(False, "--quiet", help="Reduce non-essential output"),
):
    """Export Metacrafter scan results to OpenMetadata metadata catalog.
    
    This command reads a JSON file containing Metacrafter scan results and exports
    the classification metadata (PII labels, datatypes, confidence scores) to OpenMetadata
    as tags, glossary terms, and custom properties on table columns.
    
    Example:
        metacrafter export openmetadata results.json \\
            --table-fqn "postgres.default.public.users" \\
            --openmetadata-url "http://localhost:8585/api" \\
            --token "your-jwt-token"
    """
    if verbose and quiet:
        raise typer.BadParameter("Cannot use --verbose and --quiet together")
    
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    if quiet:
        logging.getLogger().setLevel(logging.WARNING)
    
    # Get OpenMetadata URL from parameter, env var, or config
    if not openmetadata_url:
        openmetadata_url = os.getenv("OPENMETADATA_URL")
    if not openmetadata_url:
        # Try to get from config
        config = ConfigLoader.load_config()
        if config and "openmetadata" in config and "url" in config["openmetadata"]:
            openmetadata_url = config["openmetadata"]["url"]
    
    if not openmetadata_url:
        raise typer.BadParameter(
            "OpenMetadata URL is required. Provide --openmetadata-url or set OPENMETADATA_URL environment variable."
        )
    
    # Get token from parameter, env var, or config
    if not token:
        token = os.getenv("OPENMETADATA_TOKEN")
    if not token:
        config = ConfigLoader.load_config()
        if config and "openmetadata" in config and "token" in config["openmetadata"]:
            token = config["openmetadata"]["token"]
    
    # Load scan results from file
    if not os.path.exists(file):
        raise typer.BadParameter(f"Scan results file not found: {file}")
    
    try:
        with open(file, 'r', encoding='utf8') as f:
            scan_report = json.load(f)
    except json.JSONDecodeError as e:
        raise typer.BadParameter(f"Invalid JSON in scan results file: {e}")
    except Exception as e:
        raise typer.BadParameter(f"Error reading scan results file: {e}")
    
    # Import OpenMetadata exporter
    try:
        from metacrafter.integrations.openmetadata import OpenMetadataExporter
    except ImportError as e:
        raise typer.BadParameter(
            f"OpenMetadata integration not available: {e}. "
            "Install with: pip install openmetadata-ingestion"
        )
    
    # Initialize exporter
    try:
        exporter = OpenMetadataExporter(
            openmetadata_url=openmetadata_url,
            token=token,
            timeout=timeout,
            replace=replace,
        )
    except Exception as e:
        raise typer.BadParameter(f"Failed to initialize OpenMetadata exporter: {e}")
    
    # Export scan results
    if not quiet:
        print(f"Exporting scan results to OpenMetadata: {openmetadata_url}")
        print(f"Table FQN: {table_fqn}")
    
    try:
        stats = exporter.export_scan_results(
            table_fqn=table_fqn,
            scan_report=scan_report,
            add_pii_tags=add_pii_tags,
            add_datatype_tags=add_datatype_tags,
            link_glossary_terms=link_glossary_terms,
            add_properties=add_properties,
            min_confidence=min_confidence,
        )
        
        if not quiet:
            print("\nExport completed successfully!")
            print(f"Fields processed: {stats['fields_processed']}")
            print(f"Tags added: {stats['tags_added']}")
            print(f"Glossary terms linked: {stats['glossary_terms_linked']}")
            print(f"Properties added: {stats['properties_added']}")
            if stats['errors']:
                print(f"\nErrors encountered: {len(stats['errors'])}")
                for error in stats['errors']:
                    print(f"  - {error}")
        
        if stats['errors']:
            raise typer.Exit(code=1)
            
    except Exception as e:
        if verbose:
            import traceback
            traceback.print_exc()
        raise typer.BadParameter(f"Failed to export to OpenMetadata: {e}")


@export_app.command('atlas')
def export_atlas(
    file: str = typer.Argument(..., help="Path to JSON file containing Metacrafter scan results"),
    table_qn: str = typer.Option(..., "--table-qn", help="Apache Atlas table qualified name (e.g., postgres.public.users)"),
    atlas_url: str = typer.Option(None, "--atlas-url", help="Apache Atlas server URL (defaults to ATLAS_URL env var)"),
    username: str = typer.Option(None, "--username", help="Atlas username (defaults to ATLAS_USERNAME env var)"),
    password: str = typer.Option(None, "--password", help="Atlas password (defaults to ATLAS_PASSWORD env var)"),
    timeout: float = typer.Option(30.0, "--timeout", help="Request timeout in seconds"),
    replace: bool = typer.Option(False, "--replace", help="Replace existing classifications/attributes instead of merging"),
    add_pii_classifications: bool = typer.Option(True, "--add-pii-classifications/--no-pii-classifications", help="Add PII classifications"),
    add_datatype_classifications: bool = typer.Option(True, "--add-datatype-classifications/--no-datatype-classifications", help="Add datatype classifications"),
    add_attributes: bool = typer.Option(True, "--add-attributes/--no-attributes", help="Add custom attributes"),
    min_confidence: float = typer.Option(0.0, "--min-confidence", help="Minimum confidence threshold (0-100)"),
    entity_type: str = typer.Option("rdbms_column", "--entity-type", help="Atlas entity type for columns (default: rdbms_column)"),
    verbose: bool = typer.Option(False, "--verbose", help="Enable verbose logging"),
    quiet: bool = typer.Option(False, "--quiet", help="Reduce non-essential output"),
):
    """Export Metacrafter scan results to Apache Atlas metadata catalog.
    
    This command reads a JSON file containing Metacrafter scan results and exports
    the classification metadata (PII labels, datatypes, confidence scores) to Apache Atlas
    as classifications and custom attributes on table columns.
    
    Example:
        metacrafter export atlas results.json \\
            --table-qn "postgres.public.users" \\
            --atlas-url "http://localhost:21000" \\
            --username "admin" \\
            --password "admin"
    """
    if verbose and quiet:
        raise typer.BadParameter("Cannot use --verbose and --quiet together")
    
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    if quiet:
        logging.getLogger().setLevel(logging.WARNING)
    
    # Get Atlas URL from parameter, env var, or config
    if not atlas_url:
        atlas_url = os.getenv("ATLAS_URL")
    if not atlas_url:
        # Try to get from config
        config = ConfigLoader.load_config()
        if config and "atlas" in config and "url" in config["atlas"]:
            atlas_url = config["atlas"]["url"]
    
    if not atlas_url:
        raise typer.BadParameter(
            "Apache Atlas URL is required. Provide --atlas-url or set ATLAS_URL environment variable."
        )
    
    # Get username from parameter, env var, or config
    if not username:
        username = os.getenv("ATLAS_USERNAME")
    if not username:
        config = ConfigLoader.load_config()
        if config and "atlas" in config and "username" in config["atlas"]:
            username = config["atlas"]["username"]
    
    # Get password from parameter, env var, or config
    if not password:
        password = os.getenv("ATLAS_PASSWORD")
    if not password:
        config = ConfigLoader.load_config()
        if config and "atlas" in config and "password" in config["atlas"]:
            password = config["atlas"]["password"]
    
    # Load scan results from file
    if not os.path.exists(file):
        raise typer.BadParameter(f"Scan results file not found: {file}")
    
    try:
        with open(file, 'r', encoding='utf8') as f:
            scan_report = json.load(f)
    except json.JSONDecodeError as e:
        raise typer.BadParameter(f"Invalid JSON in scan results file: {e}")
    except Exception as e:
        raise typer.BadParameter(f"Error reading scan results file: {e}")
    
    # Import Atlas exporter
    try:
        from metacrafter.integrations.atlas import AtlasExporter
    except ImportError as e:
        raise typer.BadParameter(
            f"Apache Atlas integration not available: {e}. "
            "Install with: pip install requests"
        )
    
    # Initialize exporter
    try:
        exporter = AtlasExporter(
            atlas_url=atlas_url,
            username=username,
            password=password,
            timeout=timeout,
            replace=replace,
        )
    except Exception as e:
        raise typer.BadParameter(f"Failed to initialize Apache Atlas exporter: {e}")
    
    # Export scan results
    if not quiet:
        print(f"Exporting scan results to Apache Atlas: {atlas_url}")
        print(f"Table qualified name: {table_qn}")
    
    try:
        stats = exporter.export_scan_results(
            table_qualified_name=table_qn,
            scan_report=scan_report,
            entity_type=entity_type,
            add_pii_classifications=add_pii_classifications,
            add_datatype_classifications=add_datatype_classifications,
            add_attributes=add_attributes,
            min_confidence=min_confidence,
        )
        
        if not quiet:
            print("\nExport completed successfully!")
            print(f"Fields processed: {stats['fields_processed']}")
            print(f"Classifications added: {stats['classifications_added']}")
            print(f"Attributes added: {stats['attributes_added']}")
            if stats['errors']:
                print(f"\nErrors encountered: {len(stats['errors'])}")
                for error in stats['errors']:
                    print(f"  - {error}")
        
        if stats['errors']:
            raise typer.Exit(code=1)
            
    except Exception as e:
        if verbose:
            import traceback
            traceback.print_exc()
        raise typer.BadParameter(f"Failed to export to Apache Atlas: {e}")

