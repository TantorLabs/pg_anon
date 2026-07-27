from __future__ import annotations

import json

from .conftest import input_dict_text
from .helpers import build_view_fields_request


async def test_view_fields_returns_content(api_client, api_source_db, db_params):
    body = build_view_fields_request(
        db_params=db_params,
        db_name=api_source_db,
        sens_dict=input_dict_text("sens_dict.py"),
        schema_name="hr",
    )
    resp = await api_client.post("/api/stateless/view-fields", json=body)
    assert resp.status == 200
    data = await resp.json()
    assert data["status"] == "success"
    fields = data["content"]
    assert any(f["schema_name"] == "hr" and f["table_name"] == "employee" for f in fields)


async def test_view_fields_includes_rule_only_for_matched(api_client, api_source_db, db_params):
    body = build_view_fields_request(
        db_params=db_params,
        db_name=api_source_db,
        sens_dict=input_dict_text("sens_dict.py"),
        schema_name="hr",
        table_name="employee",
    )
    resp = await api_client.post("/api/stateless/view-fields", json=body)
    assert resp.status == 200
    fields = (await resp.json())["content"]
    by_name = {f["field_name"]: f for f in fields}
    assert by_name["email"]["rule"] is not None
    assert by_name["email"]["dict_data"] is not None
    assert by_name["email"]["dict_data"]["name"] == "sens_dict.py"
    assert by_name["department_id"]["rule"] is None
    assert by_name["department_id"]["dict_data"] is None


async def test_view_fields_only_sensitive_filter(api_client, api_source_db, db_params):
    body = build_view_fields_request(
        db_params=db_params,
        db_name=api_source_db,
        sens_dict=input_dict_text("sens_dict.py"),
        schema_name="hr",
        table_name="employee",
        view_only_sensitive_fields=True,
    )
    resp = await api_client.post("/api/stateless/view-fields", json=body)
    fields = (await resp.json())["content"]
    assert fields
    assert all(f["rule"] is not None for f in fields)


async def test_view_fields_fields_limit_count(api_client, api_source_db, db_params):
    body = build_view_fields_request(
        db_params=db_params,
        db_name=api_source_db,
        sens_dict=input_dict_text("sens_dict.py"),
        schema_name="hr",
        table_name="employee",
        fields_limit_count=2,
    )
    resp = await api_client.post("/api/stateless/view-fields", json=body)
    fields = (await resp.json())["content"]
    assert len(fields) <= 2


async def test_view_fields_orm_dict_translates_names(api_client, api_source_db, db_params):
    orm_dict_content = json.dumps(
        [
            {
                "schema": "hr",
                "table_name": "Employee",
                "table_alias": "Справочник.Сотрудники",
                "fields": {"First_Name": "Имя", "Last_Name": ""},
                "comment": "Основная",
            }
        ],
        ensure_ascii=False,
    )
    body = build_view_fields_request(
        db_params=db_params,
        db_name=api_source_db,
        sens_dict=input_dict_text("sens_dict.py"),
        schema_name="hr",
        table_name="employee",
        orm_dict_content=orm_dict_content,
    )
    resp = await api_client.post("/api/stateless/view-fields", json=body)
    assert resp.status == 200
    fields = (await resp.json())["content"]

    assert all(f["table_name"] == "Справочник.Сотрудники" for f in fields)
    field_names = {f["field_name"] for f in fields}
    assert "Имя" in field_names
    assert "first_name" not in field_names
    assert "last_name" in field_names  # empty ORM alias keeps the SQL name


async def test_view_fields_schema_mask(api_client, api_source_db, db_params):
    body = build_view_fields_request(
        db_params=db_params,
        db_name=api_source_db,
        sens_dict=input_dict_text("sens_dict.py"),
        schema_mask="^billing$",
    )
    resp = await api_client.post("/api/stateless/view-fields", json=body)
    fields = (await resp.json())["content"]
    assert fields
    assert {f["schema_name"] for f in fields} == {"billing"}
