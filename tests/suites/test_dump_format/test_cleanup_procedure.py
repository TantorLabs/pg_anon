"""Cleanup in a staging database: its structure files replace the structure files of the full dump."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from .conftest import cleaned_structure_dump, copy_dump_files, count_rows, dump, input_dict, restore, STRUCTURE_FILES
from pg_anon.common.enums import ResultCode

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

CLEANUP_SOURCE_DB = "pg_anon_dump_format_cleanup_source"
CLEANUP_STAGING_DB = "pg_anon_dump_format_cleanup_staging"

CLEANUP_SQL = """
DROP TRIGGER payments_touch_trg ON cln.payments;
DROP FUNCTION cln.touch_payment();
DROP FUNCTION cln.secret_rate();
DROP INDEX cln.payments_amount_idx;
ALTER TABLE cln.clients DROP CONSTRAINT clients_balance_check;
DROP SERVER cln_partner_srv;
DROP EXTENSION postgres_fdw;
"""

REMOVED_OBJECTS = {
    "trigger payments_touch_trg",
    "function touch_payment",
    "function secret_rate",
    "index payments_amount_idx",
    "check clients_balance_check",
    "server cln_partner_srv",
    "extension postgres_fdw",
}
SPARE_OBJECTS = {"table spare", "index spare_pkey"}
KEPT_OBJECTS = {
    "table clients",
    "table payments",
    "index clients_pkey",
    "index payments_pkey",
    "foreign key payments_client_fk",
    # the init mode of pg_anon creates it in the source
    "extension pgcrypto",
}
ROWS = {"cln.clients": 5, "cln.payments": 10, "cln.spare": 3}

OBJECTS_QUERY = """
SELECT 'table ' || tablename AS object FROM pg_tables WHERE schemaname = 'cln'
UNION ALL
SELECT 'index ' || indexname FROM pg_indexes WHERE schemaname = 'cln'
UNION ALL
SELECT 'function ' || p.proname FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = 'cln'
UNION ALL
SELECT 'trigger ' || t.tgname FROM pg_trigger t
    JOIN pg_class c ON c.oid = t.tgrelid JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'cln' AND NOT t.tgisinternal
UNION ALL
SELECT CASE con.contype WHEN 'c' THEN 'check ' ELSE 'foreign key ' END || con.conname FROM pg_constraint con
    JOIN pg_namespace n ON n.oid = con.connamespace
    WHERE n.nspname = 'cln' AND con.contype IN ('c', 'f')
UNION ALL
SELECT 'server ' || srvname FROM pg_foreign_server
UNION ALL
SELECT 'extension ' || extname FROM pg_extension WHERE extname <> 'plpgsql'
"""


async def objects_of(db_manager, db_name: str) -> set[str]:
    """Tables, indexes, functions, triggers, CHECK and foreign keys of the schema cln, servers and extensions."""
    return {row["object"] for row in await db_manager.fetch(db_name, OBJECTS_QUERY)}


@pytest.fixture(scope="module")
async def cleanup_source_db(db_manager, pg_anon_runner, fixtures) -> AsyncIterator[str]:
    if not await db_manager.fetch("postgres", "SELECT 1 FROM pg_available_extensions WHERE name = 'postgres_fdw'"):
        pytest.skip("a foreign server needs postgres_fdw, and this server does not have it")
    await db_manager.create_db(CLEANUP_SOURCE_DB)
    res = await pg_anon_runner.run("init", CLEANUP_SOURCE_DB)
    assert res.result_code == ResultCode.DONE
    await fixtures.build_cleanup_case(CLEANUP_SOURCE_DB)
    yield CLEANUP_SOURCE_DB
    await db_manager.drop_db(CLEANUP_SOURCE_DB)


@pytest.fixture(scope="module")
async def cleanup_full_dump(cleanup_source_db, db_params, pg_anon_runner) -> str:
    return await dump(pg_anon_runner, db_params, cleanup_source_db, "cleanup_full")


@pytest.fixture(scope="module")
async def cleaned_dump(cleanup_full_dump, db_manager, db_params, pg_anon_runner) -> AsyncIterator[str]:
    """Structure dump of a staging database: the structure of the full dump without the removed objects."""
    await db_manager.create_db(CLEANUP_STAGING_DB)
    out = await cleaned_structure_dump(
        pg_anon_runner, db_params, db_manager, CLEANUP_STAGING_DB, cleanup_full_dump, "cleanup_cleaned", CLEANUP_SQL
    )
    assert await objects_of(db_manager, CLEANUP_STAGING_DB) == KEPT_OBJECTS | SPARE_OBJECTS
    yield out
    await db_manager.drop_db(CLEANUP_STAGING_DB)


def replace_structure_files(full_dump: str, structure_dump: str, target_dir: Path) -> Path:
    """Copy the full dump, then copy every file of the structure dump over it, as a user does."""
    dump_dir = copy_dump_files(full_dump, target_dir)
    copy_dump_files(structure_dump, dump_dir)
    return dump_dir


async def test_source_and_full_dump_have_the_objects_that_the_cleanup_removes(
    cleanup_source_db, cleanup_full_dump, target_db, db_params, pg_anon_runner, db_manager
):
    everything = KEPT_OBJECTS | SPARE_OBJECTS | REMOVED_OBJECTS
    assert await objects_of(db_manager, cleanup_source_db) == everything

    res = await restore(pg_anon_runner, db_params, target_db, cleanup_full_dump)

    assert res.result_code == ResultCode.DONE, f"restore failed: {res.error_message}"
    assert await objects_of(db_manager, target_db) == everything


async def test_structure_dump_of_the_staging_database_has_only_structure_files(cleaned_dump):
    assert {item.name for item in Path(cleaned_dump).iterdir()} == set(STRUCTURE_FILES)


async def test_restore_after_cleanup_keeps_the_data_and_does_not_bring_removed_objects_back(
    cleanup_full_dump, cleaned_dump, target_db, db_params, pg_anon_runner, db_manager, tmp_path
):
    dump_dir = replace_structure_files(cleanup_full_dump, cleaned_dump, tmp_path / "dump")

    res = await restore(pg_anon_runner, db_params, target_db, dump_dir)

    assert res.result_code == ResultCode.DONE, f"restore failed: {res.error_message}"
    assert await objects_of(db_manager, target_db) == KEPT_OBJECTS | SPARE_OBJECTS
    for table, rows in ROWS.items():
        assert await count_rows(db_manager, target_db, table) == rows, f"rows of {table} are lost"


async def test_restore_after_cleanup_with_an_exclude_list_does_not_create_the_excluded_table(
    cleanup_full_dump, cleaned_dump, target_db, db_params, pg_anon_runner, db_manager, tmp_path
):
    dump_dir = replace_structure_files(cleanup_full_dump, cleaned_dump, tmp_path / "dump")

    res = await restore(
        pg_anon_runner,
        db_params,
        target_db,
        dump_dir,
        extra=[f"--partial-tables-exclude-dict-file={input_dict('exclude_cln_spare.py')}"],
    )

    assert res.result_code == ResultCode.DONE, f"restore failed: {res.error_message}"
    assert await objects_of(db_manager, target_db) == KEPT_OBJECTS
    for table in ("cln.clients", "cln.payments"):
        assert await count_rows(db_manager, target_db, table) == ROWS[table], f"rows of {table} are lost"
