from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from .conftest import input_dict_text
from .helpers import (
    build_dump_request,
    build_preview_tables_request,
    build_scan_request,
    build_view_data_request,
    build_view_fields_request,
    dict_entry,
    uuid_short,
)
from pg_anon.common.errors import ErrorCode
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

    try:
        with TestClient(app, raise_server_exceptions=False) as client:
            resp = client.get("/_test_only/raise_generic")
            assert resp.status_code == 500
            body = resp.json()
            assert body["error_code"] == ErrorCode.INTERNAL_ERROR.value
            assert "test_error" in body["message"]
    finally:
        # `app` is shared across the whole suite: probe routes left behind would
        # leak into the OpenAPI schema that test_packaging asserts on.
        app.router.routes = original_routes
        app.openapi_schema = None


RAW_SQL_DICT = repr(
    {
        "dictionary": [
            {
                "schema": "billing",
                "table": "customer",
                "raw_sql": "SELECT id, 'N' || id AS name FROM billing.customer",
            }
        ]
    }
)
RAW_SQL_ERROR_START = "raw_sql is no longer supported: rewrite the rule with fields and sql_condition. File "
RAW_SQL_ERROR_END = ", rules with raw_sql: billing.customer"


@pytest.mark.parametrize("endpoint", ["preview/billing", "view-fields", "view-data"])
async def test_sens_dict_with_raw_sql_is_rejected_with_400(api_client, api_source_db, db_params, endpoint):
    if endpoint == "view-fields":
        body = build_view_fields_request(db_params=db_params, db_name=api_source_db, sens_dict=RAW_SQL_DICT)
    elif endpoint == "view-data":
        body = build_view_data_request(
            db_params=db_params,
            db_name=api_source_db,
            sens_dict=RAW_SQL_DICT,
            schema_name="billing",
            table_name="customer",
        )
    else:
        body = build_preview_tables_request(db_params=db_params, db_name=api_source_db, sens_dict=RAW_SQL_DICT)

    resp = await api_client.post(f"/api/stateless/{endpoint}", json=body)

    assert resp.status == 400
    payload = await resp.json()
    assert payload["error_code"] == ErrorCode.INVALID_DICT_FILE.value
    assert payload["message"].startswith(RAW_SQL_ERROR_START)
    assert payload["message"].endswith(RAW_SQL_ERROR_END)


@pytest.mark.parametrize("endpoint", ["scan", "dump"])
async def test_sens_dict_with_raw_sql_sends_error_webhook(
    api_client, api_source_db, db_params, webhook_recorder, dump_storage_base_dir, endpoint
):
    output_path = f"/raw_sql_{uuid_short()}"
    if endpoint == "scan":
        body = build_scan_request(
            db_params=db_params,
            db_name=api_source_db,
            meta_dict=input_dict_text("meta_dict.py"),
            webhook_url=webhook_recorder.url,
            sens_dicts=[dict_entry("sens_dict.py", RAW_SQL_DICT)],
            need_no_sens_dict=True,
        )
    else:
        body = build_dump_request(
            db_params=db_params,
            db_name=api_source_db,
            sens_dict=RAW_SQL_DICT,
            output_path=output_path,
            webhook_url=webhook_recorder.url,
        )

    resp = await api_client.post(f"/api/stateless/{endpoint}", json=body)
    assert resp.status == 201
    terminal = await webhook_recorder.wait_for_terminal(max_wait=180)

    assert terminal["status"] == "error", terminal
    assert terminal["error_code"] == ErrorCode.INVALID_DICT_FILE.value
    assert RAW_SQL_ERROR_START in terminal["error"]
    assert RAW_SQL_ERROR_END in terminal["error"]
    if endpoint == "scan":
        assert not terminal.get("sens_dict_content")
        assert not terminal.get("no_sens_dict_content")
    else:
        assert not (dump_storage_base_dir / output_path.lstrip("/")).exists()
