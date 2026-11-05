# -*- coding: utf-8 -*-
"""HTTP API (FastAPI): v1 compatibility route, v2 routes, limits and readiness."""
import pytest

pytest.importorskip("fastapi")
from fastapi.testclient import TestClient  # noqa: E402

from metacrafter.interfaces.http_api import MetacrafterAPI  # noqa: E402
from metacrafter.server.api import MetacrafterApp  # noqa: E402


@pytest.fixture(scope="module")
def api():
    return MetacrafterAPI()


@pytest.fixture(scope="module")
def client(api):
    return TestClient(api.app)


def test_health_and_ready(client, api):
    assert client.get("/health").json() == {"status": "ok"}
    assert client.get("/ready").status_code == 200
    assert api.service is not None


def test_openapi_lists_routes(client):
    paths = client.get("/openapi.json").json()["paths"]
    assert {"/v2/scan", "/v2/classify", "/api/v1/scan_data", "/health", "/ready"} <= set(paths)


def test_scan_data_endpoint_success(client):
    response = client.post("/api/v1/scan_data", json=[{"name": "John", "email": "john@example.com"}], params={"limit": 100})
    assert response.status_code == 200
    data = response.json()
    assert "results" in data and "data" in data


def test_scan_data_comma_separated_langs_contexts(client):
    response = client.post(
        "/api/v1/scan_data",
        json=[{"name": "John", "email": "a@b.test"}],
        params={"langs": "en,ru", "contexts": "pii,finance", "confidence": "100", "fields": "email"},
    )
    assert response.status_code == 200


def test_scan_data_invalid_json(client):
    response = client.post("/api/v1/scan_data", content=b"not json", headers={"Content-Type": "application/json"})
    assert response.status_code == 400


def test_scan_data_missing_data(client):
    response = client.post("/api/v1/scan_data", content=b"", headers={"Content-Type": "application/json"})
    assert response.status_code == 400


def test_v2_scan_returns_report_v3_without_samples(client):
    response = client.post("/v2/scan", json={"records": [{"contact_email": "a@example.com"}], "options": {"parse_dates": False}})
    assert response.status_code == 200
    report = response.json()
    assert report["report_version"] == 3
    assert report["samples_included"] is False
    field = next(f for f in report["data"] if f["field"] == "contact_email")
    assert field["matches"][0]["datatype"] == "email"


def test_v2_classify_columns(client):
    response = client.post(
        "/v2/classify",
        json={"columns": [{"name": "contact_email", "values": ["a@example.com", "b@example.org"]}]},
    )
    assert response.status_code == 200
    assert response.json()["data"][0]["pii"]["is_pii"] is True


def test_v2_wrong_shape_is_400(client):
    assert client.post("/v2/scan", json=3).status_code == 400
    assert client.post("/v2/classify", json={"columns": []}).status_code == 400


def test_compatibility_alias():
    assert MetacrafterApp is MetacrafterAPI


def test_api_matches_sdk(api, client):
    payload = [{"x": "1", "email": "a@b.test"}]
    response = client.post("/v2/scan", json={"records": payload, "options": {"parse_dates": False, "include_samples": True}})
    expected = api.service.scan_data(payload, parse_dates=False)
    import json

    assert response.json()["data"] == json.loads(json.dumps(expected["data"], default=str))
