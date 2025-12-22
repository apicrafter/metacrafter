"""Shared validation utilities for Metacrafter CLI commands."""
from typing import Optional, List, Union
import typer


def validate_scan_params(
    verbose: bool,
    quiet: bool,
    output_format: str,
    dict_share: Optional[float] = None,
    batch_size: Optional[int] = None,
    retries: Optional[int] = None,
    retry_delay: Optional[float] = None,
    timeout: Optional[float] = None,
) -> None:
    """Validate common scan parameters.
    
    Args:
        verbose: Verbose flag
        quiet: Quiet flag
        output_format: Output format string
        dict_share: Dictionary share threshold (optional)
        batch_size: Batch size for database operations (optional)
        retries: Number of retry attempts (optional)
        retry_delay: Delay between retries in seconds (optional)
        timeout: Request timeout in seconds (optional)
    
    Raises:
        typer.BadParameter: If validation fails
    """
    if verbose and quiet:
        raise typer.BadParameter("Cannot use --verbose and --quiet together")
    
    allowed_formats = {"table", "json", "yaml", "csv"}
    output_format_lower = output_format.lower()
    if output_format_lower not in allowed_formats:
        raise typer.BadParameter(
            f"Invalid output format '{output_format}'. "
            f"Choose from {', '.join(sorted(allowed_formats))}"
        )
    
    if dict_share is not None and dict_share <= 0:
        raise typer.BadParameter("--dict-share must be greater than 0")
    
    if batch_size is not None and batch_size <= 0:
        raise typer.BadParameter("--batch-size must be greater than 0")
    
    if retries is not None and retries < 0:
        raise typer.BadParameter("--retries must be >= 0")
    
    if retry_delay is not None and retry_delay < 0:
        raise typer.BadParameter("--retry-delay must be >= 0")
    
    if timeout is not None and timeout < 0:
        raise typer.BadParameter("--timeout must be >= 0")


def parse_list_option(value: Optional[str]) -> Optional[List[str]]:
    """Parse comma-separated option values.
    
    Args:
        value: Comma-separated string or None
    
    Returns:
        List of strings or None if value is None/empty
    """
    if not value:
        return None
    return [v.strip() for v in value.split(",") if v.strip()] or None


def parse_rulepath(rulepath: Optional[str]) -> Optional[List[str]]:
    """Parse comma-separated rulepath values.
    
    Args:
        rulepath: Comma-separated rule paths or None
    
    Returns:
        List of rule paths or None
    """
    return parse_list_option(rulepath)


def parse_country_codes(country_codes: Optional[str]) -> Optional[List[str]]:
    """Parse comma-separated country codes and normalize to lowercase.
    
    Args:
        country_codes: Comma-separated ISO country codes or None
    
    Returns:
        List of lowercase country codes or None
    """
    if not country_codes:
        return None
    codes = parse_list_option(country_codes)
    if codes:
        return [cc.strip().lower() for cc in codes]
    return None

