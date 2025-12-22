# -*- coding: utf-8 -*-
"""Client module for accessing semantic data types registry."""
import requests

BASE_REGISTRY_URL = "https://registry.apicrafter.io"
DEFAULT_TIMEOUT = 30  # seconds


class RegistryClient:
    """Client to access semantic data types registry"""

    def __init__(self, connstr=BASE_REGISTRY_URL, preload=False):
        self.connstr = connstr
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
