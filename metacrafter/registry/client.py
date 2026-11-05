# -*- coding: utf-8 -*-
"""Client module for accessing semantic data types registry."""
import os

import requests

BASE_REGISTRY_URL = "https://registry.apicrafter.io"
DEFAULT_TIMEOUT = 30  # seconds


def default_registry_url():
    """Resolve the registry base URL.

    Precedence: ``METACRAFTER_REGISTRY_URL`` environment variable, then the
    built-in :data:`BASE_REGISTRY_URL` default.
    """
    from metacrafter.config import ConfigLoader
    return ConfigLoader.get_registry_url().rstrip("/")


class RegistryClient:
    """Client to access semantic data types registry.

    .. deprecated:: 0.1.0
        Use :class:`metacrafter.registry.adapter.RegistryAdapter`, which verifies
        downloads, works offline and resolves aliases.
    """

    def __init__(self, connstr=None, preload=False):
        import warnings

        warnings.warn(
            "RegistryClient is deprecated; use metacrafter.registry.adapter.RegistryAdapter",
            DeprecationWarning,
            stacklevel=2,
        )
        self.connstr = (connstr or default_registry_url()).rstrip("/")
        self.cached = None
        self._cache = {}
        if preload:
            self.preload()

    def preload(self):
        """Preloads all semantic data types from registry"""
        response = requests.get(f"{self.connstr}/registry.json", timeout=DEFAULT_TIMEOUT)
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise ValueError("Registry response must be an object")
        self.cached = payload

    def getlist(self):
        """List all semantic types ids"""
        if not self.cached:
            self.preload()
        return self.cached.keys()

    def has(self, datatype_id):
        """Returns true if id exists in registry, otherwise false"""
        if self.cached:
            return datatype_id in self.cached.keys()
        resp = requests.get(
            f"{self.connstr}/datatype/{datatype_id}.json", timeout=DEFAULT_TIMEOUT
        )
        return resp.status_code == 200

    def get(self, datatype_id):
        """Returns selected semantic data type"""
        if self.cached:
            return self.cached[datatype_id]
        if datatype_id in self._cache:
            return self._cache[datatype_id]
        from urllib.parse import quote
        response = requests.get(f"{self.connstr}/datatype/{quote(datatype_id, safe='')}.json", timeout=DEFAULT_TIMEOUT)
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise ValueError("Datatype response must be an object")
        if len(self._cache) >= 256:
            self._cache.pop(next(iter(self._cache)))
        self._cache[datatype_id] = payload
        return payload
