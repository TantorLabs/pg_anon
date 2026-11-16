from __future__ import annotations

import hashlib

import pytest

from tests.infrastructure.assertions import value_kind

from pg_anon import PgAnonApp
from pg_anon.cli import build_run_options
from pg_anon.common.enums import ResultCode
from pg_anon.common.errors import ErrorCode
from pg_anon.modes.view_data import ViewDataMode

E = 'md5("email")'
N = 'md5("note")'
I = 'md5("info")'  # noqa: E741

ORDERS = {"schema": "sp", "table": "orders"}
RU = {"schema": "sp", "table": "orders_2024_ru"}
DEF = {"schema": "sp", "table": "orders_2024_def"}
CONTACT = {"schema": "sp_crm", "table": "contact"}


def rule(table: dict, **fields: str) -> dict:
    return {**table, "fields": fields}


def _options(db_params, source_db, dict_file, schema: str, table: str) -> list[str]:
    return [
        "view-data",
        f"--db-host={db_params.test_db_host}",
        f"--db-name={source_db}",
        f"--db-user={db_params.test_db_user}",
        f"--db-port={db_params.test_db_port}",
        f"--db-user-password={db_params.test_db_user_password}",
        f"--config={db_params.test_config}",
        f"--prepared-sens-dict-file={dict_file}",
        f"--schema-name={schema}",
        f"--table-name={table}",
        "--limit=50",
    ]


async def _view_data(db_params, source_db, tmp_path, sens_dict, schema, table) -> tuple[list[dict], int]:
    dict_file = tmp_path / "sens_dict.py"
    dict_file.write_text(repr(sens_dict))
    executor = ViewDataMode(
        PgAnonApp(build_run_options(_options(db_params, source_db, dict_file, schema, table))).context
    )
    await executor.run()
    await executor.get_rows_count()
    names = [name.removeprefix("* ") for name in executor.field_names]
    rows = [dict(zip(names, row, strict=True)) for row in executor.data]
    return rows, executor.rows_count


def _partition(row: dict) -> str:
    """Name the partition of a sp.orders row by its date and region."""
    if row["created"].year == 2023:
        return "sp_archive.orders_2023"
    return "sp.orders_2024_ru" if row["region"] == "ru" else "sp.orders_2024_def"


async def _source_orders(db_manager, source_db) -> dict[int, dict]:
    """Rows of sp.orders in the source database, by id."""
    rows = await db_manager.fetch(source_db, "SELECT id, email, note FROM sp.orders")
    return {row["id"]: row for row in rows}


def _kind(row: dict, column: str, source: dict[int, dict]) -> str:
    """Name a shown value: "md5" of the source value, "as is", or the value itself."""
    return value_kind(row[column], source[row["id"]][column])


@pytest.mark.min_pg(100000)
async def test_view_data_of_partition_uses_rule_of_partitioned_table(source_db, db_manager, db_params, tmp_path):
    rows, _ = await _view_data(
        db_params, source_db, tmp_path, {"dictionary": [rule(ORDERS, email=E)]}, "sp", "orders_2024_def"
    )
    source = await _source_orders(db_manager, source_db)
    assert len(rows) == 6
    assert {_kind(row, "email", source) for row in rows} == {"md5"}


async def test_view_data_of_inherits_child_ignores_parent_rule(source_db, db_params, tmp_path):
    rows, _ = await _view_data(
        db_params, source_db, tmp_path, {"dictionary": [rule(CONTACT, info=I)]}, "sp_crm", "person"
    )
    assert len(rows) == 5
    assert all(row["info"].startswith("+7 900") for row in rows)


async def test_view_data_of_inherits_parent_shows_only_its_own_rows(source_db, db_params, tmp_path):
    rows, rows_count = await _view_data(
        db_params, source_db, tmp_path, {"dictionary": [rule(CONTACT, info=I)]}, "sp_crm", "contact"
    )
    assert len(rows) == 5
    assert rows_count == 5
    assert {row["info"] for row in rows} == {hashlib.md5(b"n/a").hexdigest()}  # noqa: S324


@pytest.mark.min_pg(100000)
@pytest.mark.parametrize(
    ("sens_dict", "table", "expected"),
    [
        pytest.param(
            {"dictionary": [rule(ORDERS, email=E, note=N), rule(RU, note="'RU'")]},
            "orders",
            {
                "sp_archive.orders_2023": (8, {"md5"}, {"md5"}),
                "sp.orders_2024_ru": (6, {"md5"}, {"RU"}),
                "sp.orders_2024_def": (6, {"md5"}, {"md5"}),
            },
            id="root-shows-each-partition-by-its-rule",
        ),
        pytest.param(
            {"dictionary": [rule(ORDERS, email=E, note=N), rule(RU, note="'RU'")]},
            "orders_2024",
            {"sp.orders_2024_ru": (6, {"md5"}, {"RU"}), "sp.orders_2024_def": (6, {"md5"}, {"md5"})},
            id="middle-partition-shows-its-partitions",
        ),
        pytest.param(
            {"dictionary": [rule(ORDERS, email=E)], "dictionary_exclude": [DEF]},
            "orders",
            {"sp_archive.orders_2023": (8, {"md5"}, {"as is"}), "sp.orders_2024_ru": (6, {"md5"}, {"as is"})},
            id="partition-without-data-gives-no-rows",
        ),
    ],
)
async def test_view_data_of_partitioned_table_shows_rows_of_partitions(
    source_db, db_manager, db_params, tmp_path, sens_dict, table, expected
):
    rows, _ = await _view_data(db_params, source_db, tmp_path, sens_dict, "sp", table)
    source = await _source_orders(db_manager, source_db)

    actual: dict[str, tuple[int, set, set]] = {}
    for row in rows:
        count, emails, notes = actual.get(_partition(row), (0, set(), set()))
        email, note = _kind(row, "email", source), _kind(row, "note", source)
        actual[_partition(row)] = (count + 1, emails | {email}, notes | {note})
    assert actual == expected


@pytest.mark.min_pg(100000)
async def test_view_data_of_partitioned_table_without_data_in_partitions_fails(source_db, db_params, tmp_path):
    dict_file = tmp_path / "sens_dict.py"
    dict_file.write_text(repr({"dictionary": [], "dictionary_exclude": [ORDERS]}))
    res = await PgAnonApp(build_run_options(_options(db_params, source_db, dict_file, "sp", "orders"))).run()

    assert res.result_code == ResultCode.FAIL
    assert res.exception.code == ErrorCode.TABLE_EXCLUDED
