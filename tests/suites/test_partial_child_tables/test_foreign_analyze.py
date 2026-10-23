"""A table with foreign child tables is not analyzed together with them, and the dump and the restore warn about it."""

from __future__ import annotations

import logging

import pytest

from .conftest import dump, list_options, output_path, restore, SUITE, tables_state
from pg_anon.common.enums import ResultCode

WARNING_START = "Foreign child tables found under"
FCHILD_TABLES = {"accounts": 3, "local_parent": 2, "remote_archive": "foreign"}

RESTORE_WARNING = (
    "Foreign child tables found under 1 table (fchild.local_parent). "
    "Without a user mapping, queries to these tables without ONLY fail. "
    "These tables are not analyzed and have no statistics."
)
DUMP_WARNING = (
    "In the target database, queries to these tables without ONLY go to the foreign tables and fail without "
    "a user mapping. To avoid this, exclude the foreign tables with an exclude list, restore with "
    "--keep-fdw-user-mappings, or create the user mappings by hand."
)


def foreign_child_warnings(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [message for message in caplog.messages if message.startswith(WARNING_START)]


async def stats_flags(db_manager, db_name: str, schema: str, table: str) -> list[bool]:
    """Return the `inherited` flags of the statistics rows of the table."""
    rows = await db_manager.fetch(
        db_name,
        f"SELECT DISTINCT inherited FROM pg_stats WHERE schemaname = '{schema}' AND tablename = '{table}'",
    )
    return sorted(row["inherited"] for row in rows)


async def restore_with_log(pg_anon_runner, db_params, target_db, in_dir, caplog, extra=None):
    caplog.clear()
    with caplog.at_level(logging.WARNING):
        res = await restore(pg_anon_runner, db_params, target_db, in_dir, extra)
    assert res.result_code == ResultCode.DONE, f"restore failed: {res.error_message}"
    return foreign_child_warnings(caplog)


async def dump_with_log(pg_anon_runner, db_params, source_db, name, caplog, extra=None):
    caplog.clear()
    with caplog.at_level(logging.WARNING):
        out, res = await dump(pg_anon_runner, db_params, source_db, name, extra)
    assert res.result_code == ResultCode.DONE, f"dump failed: {res.error_message}"
    return out, foreign_child_warnings(caplog)


async def test_restore_of_a_parent_with_a_foreign_child_is_done_without_its_statistics(
    foreign_child_full_dump, target_dbs, db_manager, db_params, pg_anon_runner, caplog
):
    target = target_dbs[0]

    warnings = await restore_with_log(pg_anon_runner, db_params, target, foreign_child_full_dump, caplog)

    assert await tables_state(db_manager, target, "fchild") == FCHILD_TABLES
    assert warnings == [RESTORE_WARNING]
    assert await stats_flags(db_manager, target, "fchild", "local_parent") == []
    assert await stats_flags(db_manager, target, "fchild", "accounts") == [False], "other tables are analyzed"


async def test_restore_does_not_connect_to_the_foreign_server_with_kept_user_mappings(
    foreign_child_db, target_dbs, db_manager, db_params, pg_anon_runner, caplog
):
    # the server of the builder has an unreachable host, so any connection to it fails
    await db_manager.execute(foreign_child_db, "CREATE USER MAPPING FOR CURRENT_USER SERVER fchild_srv")
    try:
        out, _ = await dump_with_log(pg_anon_runner, db_params, foreign_child_db, "fchild_user_mapping", caplog)
    finally:
        await db_manager.execute(foreign_child_db, "DROP USER MAPPING FOR CURRENT_USER SERVER fchild_srv")
    target = target_dbs[0]

    warnings = await restore_with_log(pg_anon_runner, db_params, target, out, caplog, ["--keep-fdw-user-mappings"])

    assert await tables_state(db_manager, target, "fchild") == FCHILD_TABLES
    assert warnings == [RESTORE_WARNING]


async def test_parent_is_analyzed_when_its_foreign_child_is_excluded_on_restore(
    foreign_child_full_dump, target_dbs, db_manager, db_params, pg_anon_runner, caplog
):
    target = target_dbs[0]
    options = list_options("fchild_analyze_exclude_child", "fchild", [], ["remote_archive"])

    warnings = await restore_with_log(pg_anon_runner, db_params, target, foreign_child_full_dump, caplog, options)

    assert await tables_state(db_manager, target, "fchild") == {"accounts": 3, "local_parent": 2}
    assert await stats_flags(db_manager, target, "fchild", "local_parent") == [False]
    assert warnings == []


async def test_sync_data_restore_into_a_parent_with_a_foreign_child_is_done(
    foreign_child_db, foreign_child_full_dump, target_dbs, db_manager, db_params, pg_anon_runner, caplog
):
    target = target_dbs[0]
    res = await pg_anon_runner.run("sync-struct-restore", target, [f"--input-dir={foreign_child_full_dump}"])
    assert res.result_code == ResultCode.DONE, f"sync-struct-restore failed: {res.error_message}"
    data_dump = str(output_path("fchild_sync_data"))
    res = await pg_anon_runner.run(
        "sync-data-dump",
        foreign_child_db,
        [
            f"--prepared-sens-dict-file={SUITE / 'input_dict' / 'empty.py'}",
            f"--output-dir={data_dump}",
            f"--db-connections-per-process={db_params.db_connections_per_process}",
            "--clear-output-dir",
        ],
    )
    assert res.result_code == ResultCode.DONE, f"sync-data-dump failed: {res.error_message}"

    caplog.clear()
    with caplog.at_level(logging.WARNING):
        res = await pg_anon_runner.run(
            "sync-data-restore",
            target,
            [f"--input-dir={data_dump}", f"--db-connections-per-process={db_params.db_connections_per_process}"],
        )

    assert res.result_code == ResultCode.DONE, f"sync-data-restore failed: {res.error_message}"
    assert await tables_state(db_manager, target, "fchild") == FCHILD_TABLES
    warnings = foreign_child_warnings(caplog)
    assert warnings == [RESTORE_WARNING]


async def test_dump_warns_about_a_parent_with_a_foreign_child(foreign_child_db, db_params, pg_anon_runner, caplog):
    _, warnings = await dump_with_log(pg_anon_runner, db_params, foreign_child_db, "fchild_dump_warning", caplog)

    assert warnings == [f"{WARNING_START} 1 table (fchild.local_parent). {DUMP_WARNING}"]


@pytest.mark.min_pg(100000)
async def test_dump_warns_about_a_table_with_a_foreign_partition(
    foreign_partition_db, db_params, pg_anon_runner, caplog
):
    _, warnings = await dump_with_log(pg_anon_runner, db_params, foreign_partition_db, "fpart_dump_warning", caplog)

    assert warnings == [f"{WARNING_START} 1 table (fpart.events). {DUMP_WARNING}"]


async def test_dump_does_not_warn_when_the_foreign_child_is_excluded(
    foreign_child_db, db_params, pg_anon_runner, caplog
):
    options = list_options("fchild_dump_exclude_child", "fchild", [], ["remote_archive"])

    _, warnings = await dump_with_log(
        pg_anon_runner, db_params, foreign_child_db, "fchild_dump_exclude_child", caplog, options
    )

    assert warnings == []


async def test_no_warnings_without_foreign_tables(inherits_db, target_dbs, db_params, pg_anon_runner, caplog):
    out, warnings = await dump_with_log(pg_anon_runner, db_params, inherits_db, "inherits_no_foreign", caplog)
    assert warnings == []

    assert await restore_with_log(pg_anon_runner, db_params, target_dbs[0], out, caplog) == []
