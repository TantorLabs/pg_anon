from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from pg_anon.common.enums import ResultCode

SUITE = Path(__file__).resolve().parent

SOURCE_DB = "pg_anon_wide_source"
TABLES = int(os.getenv("PG_ANON_WIDE_TABLES", "20000"))
# Bump when build_wide_1c changes, so a kept source database is built again.
SOURCE_VERSION = f"wide_1c:v1:{TABLES}"
KINDS = ("_reference", "_document", "_inforg", "_accumrg")


def table_name(i: int) -> str:
    return f"{KINDS[i % 4]}{i}"


def all_tables() -> list[str]:
    return [table_name(i) for i in range(1, TABLES + 1)]


def input_dict(name: str) -> str:
    return str(SUITE / "input_dict" / name)


def output_path(name: str) -> str:
    out = SUITE / "output" / name
    out.mkdir(parents=True, exist_ok=True)
    return str(out)


def write_output_file(name: str, content: str) -> str:
    path = Path(output_path("files")) / name
    path.write_text(content, encoding="utf-8")
    return str(path)


def write_tables_dict(name: str, tables: list[str]) -> str:
    rules = [{"schema": "public", "table": t} for t in tables]
    return write_output_file(name, repr({"tables": rules}))


async def public_tables(db_manager, db_name: str) -> set[str]:
    rows = await db_manager.fetch(db_name, "SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
    return {r["tablename"] for r in rows}


async def _check_lock_slots(db_manager) -> None:
    rows = await db_manager.fetch(
        "postgres",
        """
        SELECT current_setting('max_locks_per_transaction')::int
            * (current_setting('max_connections')::int + current_setting('max_prepared_transactions')::int)
        """,
    )
    slots = rows[0][0]
    # pg_dump locks every table in one transaction.
    if slots < TABLES * 1.2:
        pytest.skip(
            f"{TABLES} tables need about {int(TABLES * 1.2)} lock slots, the server has {slots}:"
            f" increase max_locks_per_transaction"
        )


async def _is_source_ready(db_manager) -> bool:
    rows = await db_manager.fetch(
        "postgres",
        f"SELECT shobj_description(oid, 'pg_database') FROM pg_database WHERE datname = '{SOURCE_DB}'",
    )
    return bool(rows) and rows[0][0] == SOURCE_VERSION


@pytest.fixture(scope="module")
async def source_db(db_manager, pg_anon_runner, fixtures):
    await _check_lock_slots(db_manager)
    # Building takes minutes; with KEEP_TEST_DBS the next run reuses the database.
    if not await _is_source_ready(db_manager):
        await db_manager.create_db(SOURCE_DB)
        res = await pg_anon_runner.run("init", SOURCE_DB)
        assert res.result_code == ResultCode.DONE
        await fixtures.build_wide_1c(SOURCE_DB, TABLES)
        await db_manager.execute("postgres", f"COMMENT ON DATABASE {SOURCE_DB} IS '{SOURCE_VERSION}'")
    yield SOURCE_DB
    await db_manager.drop_db(SOURCE_DB)


@pytest.fixture
async def target_db(db_manager, request):
    name = f"pg_anon_wide_tgt_{request.node.name}"[:60]
    await db_manager.create_db(name)
    yield name
    await db_manager.drop_db(name)


@pytest.fixture(scope="module")
async def sens_dict(source_db, pg_anon_runner, db_params) -> str:
    path = str(Path(output_path("files")) / "sens.json")
    res = await pg_anon_runner.run(
        "create-dict",
        source_db,
        [
            "--scan-mode=partial",
            f"--meta-dict-file={input_dict('meta.py')}",
            f"--output-sens-dict-file={path}",
            f"--db-connections={db_params.db_connections_per_process}",
        ],
    )
    assert res.result_code == ResultCode.DONE
    return path


@pytest.fixture(scope="module")
async def full_dump(source_db, sens_dict, pg_anon_runner, db_params) -> str:
    out = output_path("full_dump")
    res = await pg_anon_runner.run(
        "dump",
        source_db,
        [
            f"--prepared-sens-dict-file={sens_dict}",
            f"--output-dir={out}",
            f"--db-connections={db_params.db_connections_per_process}",
            "--clear-output-dir",
        ],
    )
    assert res.result_code == ResultCode.DONE
    return out


@pytest.fixture(scope="module")
def orm_dict() -> str:
    entries = [
        {
            "schema": "public",
            "table_name": t,
            "table_alias": f"Справочник.Объект{t}",
            "fields": {"_description": "Наименование"},
        }
        for t in all_tables()
    ]
    return write_output_file("orm_structure.json", json.dumps(entries, ensure_ascii=False))
