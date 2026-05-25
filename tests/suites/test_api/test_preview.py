from __future__ import annotations

from .conftest import input_dict_text
from .helpers import build_preview_schemas_request, build_preview_tables_request


async def test_preview_schemas_lists_known_schemas(api_client, api_source_db, db_params):
    body = build_preview_schemas_request(db_params=db_params, db_name=api_source_db)
    resp = await api_client.post("/api/stateless/preview", json=body)
    assert resp.status == 200
    data = await resp.json()
    assert data["status"] == "success"
    assert "hr" in data["content"]
    assert "billing" in data["content"]


async def test_preview_schemas_filter_narrows_result(api_client, api_source_db, db_params):
    body = build_preview_schemas_request(db_params=db_params, db_name=api_source_db, schema_filter="hr")
    resp = await api_client.post("/api/stateless/preview", json=body)
    schemas = (await resp.json())["content"]
    assert "hr" in schemas
    assert "billing" not in schemas


async def test_preview_schema_tables_marks_sensitive(api_client, api_source_db, db_params):
    body = build_preview_tables_request(
        db_params=db_params,
        db_name=api_source_db,
        sens_dict=input_dict_text("sens_dict.py"),
    )
    resp = await api_client.post("/api/stateless/preview/hr", json=body)
    assert resp.status == 200
    tables = (await resp.json())["content"]
    by_name = {t["table_name"]: t for t in tables}
    assert by_name["employee"]["is_sensitive"] is True
    fields = {f["field_name"]: f for f in by_name["employee"]["fields"]}
    assert fields["email"]["is_sensitive"] is True
    assert fields["email"]["rule"] is not None
    assert fields["id"]["is_sensitive"] is False


async def test_preview_schema_tables_view_only_sensitive(api_client, api_source_db, db_params):
    body = build_preview_tables_request(
        db_params=db_params,
        db_name=api_source_db,
        sens_dict=input_dict_text("sens_dict.py"),
        view_only_sensitive_tables=True,
    )
    resp = await api_client.post("/api/stateless/preview/hr", json=body)
    tables = (await resp.json())["content"]
    assert tables
    assert all(t["is_sensitive"] for t in tables)


async def test_preview_schema_tables_pagination(api_client, api_source_db, db_params):
    body = build_preview_tables_request(
        db_params=db_params,
        db_name=api_source_db,
        sens_dict=input_dict_text("sens_dict.py"),
        limit=1,
        offset=0,
    )
    resp = await api_client.post("/api/stateless/preview/billing", json=body)
    tables = (await resp.json())["content"]
    assert len(tables) == 1


async def test_preview_schema_tables_filter(api_client, api_source_db, db_params):
    body = build_preview_tables_request(
        db_params=db_params,
        db_name=api_source_db,
        sens_dict=input_dict_text("sens_dict.py"),
        table_filter="customer",
    )
    resp = await api_client.post("/api/stateless/preview/billing", json=body)
    tables = (await resp.json())["content"]
    assert tables
    assert all("customer" in t["table_name"] for t in tables)
