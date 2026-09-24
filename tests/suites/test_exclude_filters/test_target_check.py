from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pytest

from .conftest import dump, input_dict, output_path, restore
from pg_anon.common.enums import ResultCode
from pg_anon.common.errors import ErrorCode

if TYPE_CHECKING:
    from collections.abc import AsyncIterator


async def dump_in_mode(pg_anon_runner, db_params, source_db, mode: str) -> str:
    out = output_path(mode)
    res = await dump(pg_anon_runner, db_params, source_db, out_dir=out, dict_file=input_dict("empty.py"), mode=mode)
    assert res.result_code == ResultCode.DONE
    return out


@pytest.fixture
async def data_dump(source_db, db_params, pg_anon_runner) -> str:
    return await dump_in_mode(pg_anon_runner, db_params, source_db, "sync-data-dump")


@pytest.fixture
async def struct_dump(source_db, db_params, pg_anon_runner) -> str:
    return await dump_in_mode(pg_anon_runner, db_params, source_db, "sync-struct-dump")


async def restore_structure(pg_anon_runner, db_params, target_db, struct_dump, extra=None) -> None:
    res = await restore(
        pg_anon_runner, db_params, target_db, in_dir=struct_dump, mode="sync-struct-restore", extra=extra
    )
    assert res.result_code == ResultCode.DONE, f"structure restore failed: {res.error_message}"


async def test_restore_stops_when_target_misses_tables(
    struct_dump, data_dump, target_db, db_params, pg_anon_runner, db_manager
):
    """Loading data into a structure that does not match must fail before the first table."""
    await restore_structure(pg_anon_runner, db_params, target_db, struct_dump)
    await db_manager.execute(target_db, 'DROP TABLE s_keep."MixedCase"')

    res = await restore(pg_anon_runner, db_params, target_db, in_dir=data_dump, mode="sync-data-restore")

    assert res.result_code == ResultCode.FAIL
    assert res.exception.code == ErrorCode.TARGET_MISSING_TABLES
    assert "1 table (s_keep.MixedCase)" in str(res.exception)

    rows = await db_manager.fetch(target_db, "SELECT count(*) AS count FROM s_keep.b")
    assert rows[0]["count"] == 0, "the target must stay untouched"


async def test_excluded_schema_is_not_required_in_target(
    struct_dump, data_dump, target_db, db_params, pg_anon_runner, db_manager
):
    """Structure and data restored in two steps with the same exclusion fit each other."""
    options = ["--exclude-schema-name=s_excl"]
    await restore_structure(pg_anon_runner, db_params, target_db, struct_dump, extra=options)

    res = await restore(pg_anon_runner, db_params, target_db, in_dir=data_dump, mode="sync-data-restore", extra=options)

    assert res.result_code == ResultCode.DONE, f"data restore failed: {res.error_message}"
    rows = await db_manager.fetch(target_db, "SELECT count(*) AS count FROM s_keep.b")
    assert rows[0]["count"] == 5


async def test_extra_tables_in_target_are_ignored(
    struct_dump, data_dump, target_db, db_params, pg_anon_runner, db_manager, caplog
):
    """A target wider than the dump is a normal top-up case and must stay quiet."""
    await restore_structure(pg_anon_runner, db_params, target_db, struct_dump)
    await db_manager.execute(target_db, "CREATE TABLE s_keep.extra (id int PRIMARY KEY)")

    with caplog.at_level(logging.WARNING, logger="pg_anon.logger"):
        res = await restore(pg_anon_runner, db_params, target_db, in_dir=data_dump, mode="sync-data-restore")

    assert res.result_code == ResultCode.DONE
    assert not [message for message in caplog.messages if "s_keep.extra" in message]


async def test_pg_restore_options_are_reported_as_unused(
    struct_dump, data_dump, target_db, db_params, pg_anon_runner, caplog
):
    await restore_structure(pg_anon_runner, db_params, target_db, struct_dump)

    with caplog.at_level(logging.WARNING, logger="pg_anon.logger"):
        res = await restore(
            pg_anon_runner,
            db_params,
            target_db,
            in_dir=data_dump,
            mode="sync-data-restore",
            extra=["--pg-restore-options=-n s_keep"],
        )

    assert res.result_code == ResultCode.DONE, f"an unused option must not break the restore: {res.error_message}"
    assert any("--pg-restore-options is ignored" in message for message in caplog.messages), (
        "options that cannot be applied must be reported"
    )


NO_DATA_FILE_DB = "pg_anon_exclude_filters_no_data_file"

# no foreign keys here: --clean-db cannot drop a referenced table before the table that refers to it
NO_DATA_FILE_DDL = """
CREATE SCHEMA refs;
CREATE TABLE refs.excluded_parent (id int PRIMARY KEY);
INSERT INTO refs.excluded_parent SELECT generate_series(1, 3);
CREATE TABLE refs.events (id int, day int) PARTITION BY RANGE (day);
CREATE TABLE refs.events_old PARTITION OF refs.events FOR VALUES FROM (0) TO (100);
CREATE TABLE refs.events_new PARTITION OF refs.events FOR VALUES FROM (100) TO (200);
INSERT INTO refs.events VALUES (1, 10), (2, 150);
"""


@pytest.fixture(scope="module")
async def no_data_file_dump(db_manager, db_params, pg_anon_runner) -> AsyncIterator[str]:
    """The data of refs.excluded_parent and refs.events_old is left out, refs.events is a partitioned root."""
    await db_manager.create_db(NO_DATA_FILE_DB)
    res = await pg_anon_runner.run("init", NO_DATA_FILE_DB)
    assert res.result_code == ResultCode.DONE
    await db_manager.execute(NO_DATA_FILE_DB, NO_DATA_FILE_DDL)

    out = output_path("no_data_file")
    res = await dump(pg_anon_runner, db_params, NO_DATA_FILE_DB, out_dir=out, dict_file=input_dict("fk_cases.py"))
    assert res.result_code == ResultCode.DONE
    yield out
    await db_manager.drop_db(NO_DATA_FILE_DB)


async def restore_twice(pg_anon_runner, db_params, db_manager, target_db, dump_dir, prepare_sql: str):
    res = await restore(pg_anon_runner, db_params, target_db, in_dir=dump_dir)
    assert res.result_code == ResultCode.DONE, f"restore failed: {res.error_message}"
    await db_manager.execute(target_db, prepare_sql)
    return await restore(pg_anon_runner, db_params, target_db, in_dir=dump_dir, extra=["--clean-db"])


async def test_clean_db_accepts_tables_without_data_file(
    no_data_file_dump, target_db, db_params, pg_anon_runner, db_manager
):
    """A partitioned root and a table with excluded data are in the dump structure, so they are not extra."""
    res = await restore_twice(
        pg_anon_runner,
        db_params,
        db_manager,
        target_db,
        no_data_file_dump,
        "INSERT INTO refs.excluded_parent VALUES (100)",
    )

    assert res.result_code == ResultCode.DONE, f"restore with --clean-db failed: {res.error_message}"
    rows = await db_manager.fetch(target_db, "SELECT count(*) AS count FROM refs.excluded_parent")
    assert rows[0]["count"] == 0, "the target is cleaned before the restore"
    rows = await db_manager.fetch(target_db, "SELECT count(*) AS count FROM refs.events")
    assert rows[0]["count"] == 1


async def test_clean_db_still_reports_extra_tables(no_data_file_dump, target_db, db_params, pg_anon_runner, db_manager):
    res = await restore_twice(
        pg_anon_runner, db_params, db_manager, target_db, no_data_file_dump, "CREATE TABLE refs.extra (id int)"
    )

    assert res.result_code == ResultCode.FAIL
    assert res.exception.code == ErrorCode.TARGET_HAS_EXTRA_TABLES
    assert 'Target DB has 1 table(s) not present in the dump: "refs"."extra"' in str(res.exception)
