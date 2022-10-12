# -*- coding: utf-8 -*- 
import pytest
from metacrafter.registry.client import RegistryClient



class TestRegistry:
    def test_registry_connect(self):
        client = RegistryClient()
        assert client != None

    def test_registry_preload_init(self):
        client = RegistryClient(preload=True)
        assert client.cached != None

    def test_registry_not_preload_init(self):
        client = RegistryClient(preload=False)
        assert client.cached == None

    def test_registry_preload(self):
        client = RegistryClient(preload=False)
        assert client.cached == None
        client.preload()
        assert client.cached != None

    def test_registry_list(self):
        client = RegistryClient(preload=False)
        assert len(client.getlist()) > 0

    def test_registry_has(self):
        client = RegistryClient(preload=True)
        assert client.has('year')
        assert client.has('month')
        assert not client.has('notexists')

    def test_registry_get(self):
        client = RegistryClient(preload=True)
        assert client.get('year')['name'] == 'Year'
        assert client.get('url')['id'] == 'url'
        assert client.get('birthday')['is_pii'] == True
        assert client.get('inn')['is_pii'] == False

    def test_registry_get_error(self):
        client = RegistryClient(preload=True)
        with pytest.raises(KeyError):
            item = client.get('notexists')
