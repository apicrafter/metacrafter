"""Compatibility module: the HTTP API now lives in ``metacrafter.interfaces.http_api``.

The API runs on FastAPI (``pip install 'metacrafter[server]'``). ``MetacrafterApp`` is
kept as an alias of :class:`metacrafter.interfaces.http_api.MetacrafterAPI`.
"""
from metacrafter.interfaces.http_api import MetacrafterAPI, create_app, run_server  # noqa: F401

MetacrafterApp = MetacrafterAPI

__all__ = ["MetacrafterAPI", "MetacrafterApp", "create_app", "run_server"]
