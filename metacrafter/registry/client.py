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
    return os.environ.get("METACRAFTER_REGISTRY_URL", BASE_REGISTRY_URL).rstrip("/")


class RegistryClient:
    """Client to access semantic data types registry"""

    def __init__(self, connstr=None, preload=False):
        self.connstr = (connstr or default_registry_url()).rstrip("/")
        self.cached = None
        if preload:
            self.preload()

    def preload(self):
        """Preloads all semantic data types from registry"""
        self.cached = requests.get(
            f"{self.connstr}/registry.json", timeout=DEFAULT_TIMEOUT
        ).json()

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
        return requests.get(
            f"{self.connstr}/datatype/{datatype_id}.json", timeout=DEFAULT_TIMEOUT
        ).json()
