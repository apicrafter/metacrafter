"""Compatibility module: use ``metacrafter.interfaces.http_api.run_server``."""
from metacrafter.interfaces.http_api import run_server  # noqa: F401

CLASSIFY_HOST = "127.0.0.1"
CLASSIFY_PORT = 10399

__all__ = ["run_server", "CLASSIFY_HOST", "CLASSIFY_PORT"]
