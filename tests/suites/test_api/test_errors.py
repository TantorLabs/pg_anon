from __future__ import annotations

from fastapi.testclient import TestClient

from .helpers import build_view_fields_request
from pg_anon.common.errors import ErrorCode, PgAnonError
from pg_anon.rest_api.api import app


async def test_pganon_error_maps_to_400(api_client, api_source_db, db_params):
    body = build_view_fields_request(
        db_params=db_params,
        db_name=api_source_db,
        sens_dict="this is not valid python dict",
    )
    resp = await api_client.post("/api/stateless/view-fields", json=body)
    assert resp.status == 400
    payload = await resp.json()
    assert payload["error_code"]
    assert payload["message"]


async def test_pydantic_validation_returns_422(api_client):
    resp = await api_client.post("/api/stateless/view-fields", json={"foo": "bar"})
    assert resp.status == 422


def test_global_exception_handler_maps_to_500_with_internal_error_code():
    original_routes = list(app.router.routes)

    @app.get("/_test_only/raise_generic")
    async def _raise():
        raise RuntimeError("test_error")

    @app.get("/_test_only/raise_pganon")
    async def _raise_pganon():
        raise PgAnonError(ErrorCode.INVALID_DICT_FILE, "bad dict")

    try:
        with TestClient(app, raise_server_exceptions=False) as client:
            resp = client.get("/_test_only/raise_generic")
            assert resp.status_code == 500
            body = resp.json()
            assert body["error_code"] == ErrorCode.INTERNAL_ERROR.value
            assert "test_error" in body["message"]

            resp = client.get("/_test_only/raise_pganon")
            assert resp.status_code == 400
            body = resp.json()
            assert body["error_code"] == ErrorCode.INVALID_DICT_FILE.value
            assert body["message"] == "bad dict"
    finally:
        # `app` is shared across the whole suite: probe routes left behind would
        # leak into the OpenAPI schema that test_packaging asserts on.
        app.router.routes = original_routes
        app.openapi_schema = None
