from __future__ import annotations

import hashlib

import pytest

from .helpers import build_preview_tables_request, build_view_data_request

E = 'md5("email")'
N = 'md5("note")'
I = 'md5("info")'  # noqa: E741

ORDERS = {"schema": "sp", "table": "orders"}
RU = {"schema": "sp", "table": "orders_2024_ru"}
DEF = {"schema": "sp", "table": "orders_2024_def"}
CONTACT = {"schema": "sp_crm", "table": "contact"}


def rule(table: dict, **fields: str) -> dict:
    return {**table, "fields": fields}


async def _preview(api_client, db_params, db_name, schema, sens_dict) -> dict[str, dict]:
    body = build_preview_tables_request(db_params=db_params, db_name=db_name, sens_dict=repr(sens_dict))
    resp = await api_client.post(f"/api/stateless/preview/{schema}", json=body)
    assert resp.status == 200
    tables = (await resp.json())["content"]
    return {
        table["table_name"]: {
            "is_sensitive": table["is_sensitive"],
            "is_excluded": table["is_excluded"],
            "rules": {field["field_name"]: field["rule"] for field in table["fields"] if field["is_sensitive"]},
        }
        for table in tables
    }


def _table(*, sensitive: bool, excluded: bool = False, **rules: str) -> dict:
    return {"is_sensitive": sensitive, "is_excluded": excluded, "rules": rules}


@pytest.mark.min_pg(100000)
@pytest.mark.parametrize(
    ("sens_dict", "expected"),
    [
        pytest.param(
            {"dictionary": [rule(ORDERS, email=E)]},
            {
                "sp.orders_2024_ru": _table(sensitive=True, email=E),
                "sp.orders_2024_def": _table(sensitive=True, email=E),
                "sp_archive.orders_2023": _table(sensitive=True, email=E),
            },
            id="root-rule",
        ),
        pytest.param(
            {"dictionary": [rule(ORDERS, email=E, note=N), rule(RU, note="'RU'")]},
            {
                "sp.orders_2024_ru": _table(sensitive=True, email=E, note="'RU'"),
                "sp.orders_2024_def": _table(sensitive=True, email=E, note=N),
            },
            id="own-rule-merged-with-root",
        ),
        pytest.param(
            {"dictionary": [], "dictionary_exclude": [ORDERS]},
            {
                "sp.orders_2024_ru": _table(sensitive=False, excluded=True),
                "sp.orders_2024_def": _table(sensitive=False, excluded=True),
                "sp_archive.orders_2023": _table(sensitive=False, excluded=True),
            },
            id="exclude-root",
        ),
        pytest.param(
            {"dictionary": [rule(ORDERS, email=E)], "dictionary_exclude": [DEF]},
            {
                "sp.orders_2024_ru": _table(sensitive=True, email=E),
                "sp.orders_2024_def": _table(sensitive=False, excluded=True),
            },
            id="own-exclude-beats-root-rule",
        ),
    ],
)
async def test_preview_marks_partitions_by_rule_that_dump_applies(
    api_client, api_source_db, db_params, sens_dict, expected
):
    actual = {}
    for schema in ("sp", "sp_archive"):
        tables = await _preview(api_client, db_params, api_source_db, schema, sens_dict)
        actual.update({f"{schema}.{name}": table for name, table in tables.items()})
    assert {name: actual[name] for name in expected} == expected


async def test_preview_does_not_apply_inherits_parent_rule_to_children(api_client, api_source_db, db_params):
    tables = await _preview(api_client, db_params, api_source_db, "sp_crm", {"dictionary": [rule(CONTACT, info=I)]})
    assert tables["contact"] == _table(sensitive=True, info=I)
    assert tables["person"] == _table(sensitive=False)
    assert tables["company"] == _table(sensitive=False)
    assert tables["partner"] == _table(sensitive=False)


async def test_rest_view_data_of_inherits_parent_shows_only_its_own_rows(api_client, api_source_db, db_params):
    body = build_view_data_request(
        db_params=db_params,
        db_name=api_source_db,
        sens_dict=repr({"dictionary": [rule(CONTACT, info=I)]}),
        schema_name="sp_crm",
        table_name="contact",
        limit=50,
    )
    resp = await api_client.post("/api/stateless/view-data", json=body)
    assert resp.status == 200
    content = (await resp.json())["content"]
    assert content["total_rows_count"] == 5
    assert len(content["rows_before"]) == len(content["rows_after"]) == 5


def _md5(value: str) -> str:
    return hashlib.md5(value.encode()).hexdigest()  # noqa: S324


@pytest.mark.min_pg(100000)
async def test_rest_view_data_of_partitioned_table_shows_each_partition_by_its_rule(
    api_client, api_source_db, db_params, db_manager
):
    body = build_view_data_request(
        db_params=db_params,
        db_name=api_source_db,
        sens_dict=repr({"dictionary": [rule(ORDERS, email=E, note=N), rule(RU, note="'RU'")]}),
        schema_name="sp",
        table_name="orders",
        limit=50,
    )
    resp = await api_client.post("/api/stateless/view-data", json=body)
    assert resp.status == 200
    content = (await resp.json())["content"]
    assert content["total_rows_count"] == 20
    names = content["field_names"]
    before = [dict(zip(names, row, strict=True)) for row in content["rows_before"]]
    after = [dict(zip(names, row, strict=True)) for row in content["rows_after"]]
    assert len(before) == 20
    assert [row["id"] for row in before] == [row["id"] for row in after]

    ru_rows = await db_manager.fetch(api_source_db, "SELECT id FROM sp.orders_2024_ru")
    ru_ids = {str(row["id"]) for row in ru_rows}
    source = {row["id"]: row for row in before}
    for row in after:
        original = source[row["id"]]
        assert row["email"] == _md5(original["email"]), row["id"]
        expected_note = "RU" if row["id"] in ru_ids else _md5(original["note"])
        assert row["note"] == expected_note, row["id"]
