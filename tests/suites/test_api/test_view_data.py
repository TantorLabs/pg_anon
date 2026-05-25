from __future__ import annotations

from .conftest import input_dict_text
from .helpers import build_view_data_request


async def test_view_data_returns_rows(api_client, api_source_db, db_params):
    body = build_view_data_request(
        db_params=db_params,
        db_name=api_source_db,
        sens_dict=input_dict_text("sens_dict.py"),
        schema_name="hr",
        table_name="employee",
        limit=5,
    )
    resp = await api_client.post("/api/stateless/view-data", json=body)
    assert resp.status == 200
    content = (await resp.json())["content"]
    assert content["schema_name"] == "hr"
    assert content["table_name"] == "employee"
    assert content["total_rows_count"] > 0
    assert len(content["rows_before"]) == len(content["rows_after"]) <= 5
    email_idx = content["field_names"].index("email")
    befores = {row[email_idx] for row in content["rows_before"]}
    afters = {row[email_idx] for row in content["rows_after"]}
    assert befores.isdisjoint(afters)


async def test_view_data_offset_past_end_is_empty(api_client, api_source_db, db_params):
    body = build_view_data_request(
        db_params=db_params,
        db_name=api_source_db,
        sens_dict=input_dict_text("sens_dict.py"),
        schema_name="hr",
        table_name="employee",
        limit=5,
        offset=10**6,
    )
    resp = await api_client.post("/api/stateless/view-data", json=body)
    assert resp.status == 200
    content = (await resp.json())["content"]
    assert content["rows_before"] == []
    assert content["rows_after"] == []


async def test_view_data_rows_same_width(api_client, api_source_db, db_params):
    body = build_view_data_request(
        db_params=db_params,
        db_name=api_source_db,
        sens_dict=input_dict_text("sens_dict.py"),
        schema_name="billing",
        table_name="payment_card",
        limit=3,
    )
    resp = await api_client.post("/api/stateless/view-data", json=body)
    content = (await resp.json())["content"]
    widths = {len(row) for row in content["rows_before"] + content["rows_after"]}
    assert widths == {len(content["field_names"])}
