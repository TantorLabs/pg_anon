from __future__ import annotations

import json
import logging
from pathlib import Path

import pytest

from .conftest import (
    cleaned_structure_dump,
    copy_dump_files,
    count_rows,
    dump,
    input_dict,
    read_json,
    restore,
    STRUCTURE_FILES,
    user_schemas,
)
from pg_anon.common.enums import ResultCode
from pg_anon.common.errors import ErrorCode
from pg_anon.context import Context
from pg_anon.modes import restore as restore_mode
from pg_anon.version import __version__

MARKER_DDL = "CREATE SCHEMA marker; CREATE TABLE marker.kept (id int)"
NEWER_VERSION = "99.0.0"


async def assert_target_is_untouched(db_manager, target_db: str) -> None:
    """The target had only the marker table before the restore and must have only it after."""
    assert await user_schemas(db_manager, target_db) == {"marker"}, "the restore must stop before any change"
    assert await count_rows(db_manager, target_db, "marker.kept") == 0


@pytest.mark.parametrize("mode", ["restore", "sync-struct-restore"])
async def test_restore_stops_without_structure_json(
    full_dump, target_db, db_params, pg_anon_runner, db_manager, tmp_path, mode
):
    dump_dir = copy_dump_files(full_dump, tmp_path / "dump", skip=["structure.json"])
    await db_manager.execute(target_db, MARKER_DDL)

    res = await restore(pg_anon_runner, db_params, target_db, dump_dir, mode=mode, extra=["--drop-db"])

    assert res.result_code == ResultCode.FAIL
    assert res.exception.code == ErrorCode.DUMP_STRUCTURE_NOT_FOUND
    assert "structure.json" in str(res.exception)
    assert mode in str(res.exception), "the error must name the mode that needs the structure"
    await assert_target_is_untouched(db_manager, target_db)


async def test_sync_data_restore_does_not_need_structure_json(
    struct_dump, data_dump, target_db, db_params, pg_anon_runner, db_manager
):
    res = await restore(pg_anon_runner, db_params, target_db, struct_dump, mode="sync-struct-restore")
    assert res.result_code == ResultCode.DONE, f"structure restore failed: {res.error_message}"

    res = await restore(pg_anon_runner, db_params, target_db, data_dump, mode="sync-data-restore")

    assert res.result_code == ResultCode.DONE, f"data restore failed: {res.error_message}"
    assert await count_rows(db_manager, target_db, "fmt.child") == 5


@pytest.mark.parametrize(
    ("mode", "file_name"),
    [
        ("restore", "structure.json"),
        ("restore", "metadata.json"),
        ("sync-struct-restore", "structure.json"),
        ("sync-data-restore", "metadata.json"),
    ],
)
async def test_restore_stops_when_a_file_needs_a_newer_pg_anon(
    full_dump, target_db, db_params, pg_anon_runner, db_manager, tmp_path, mode, file_name
):
    dump_dir = copy_dump_files(full_dump, tmp_path / "dump")
    description = read_json(dump_dir, file_name)
    description["min_pg_anon_version"] = NEWER_VERSION
    (dump_dir / file_name).write_text(json.dumps(description), encoding="utf-8")
    await db_manager.execute(target_db, MARKER_DDL)

    res = await restore(pg_anon_runner, db_params, target_db, dump_dir, mode=mode, extra=["--drop-db"])

    assert res.result_code == ResultCode.FAIL
    assert res.exception.code == ErrorCode.DUMP_VERSION_NOT_SUPPORTED
    assert NEWER_VERSION in str(res.exception)
    assert __version__ in str(res.exception), "the error must name the running version"
    await assert_target_is_untouched(db_manager, target_db)


@pytest.mark.parametrize("mode", ["restore", "sync-struct-restore"])
async def test_restore_stops_when_archives_and_structure_json_are_from_different_dumps(
    full_dump, other_db, target_db, db_params, pg_anon_runner, db_manager, tmp_path, mode
):
    other_dump = await dump(pg_anon_runner, db_params, other_db, "other_struct", "sync-struct-dump")
    dump_dir = copy_dump_files(full_dump, tmp_path / "dump")
    # only the archives are replaced: structure.json still describes the first database
    copy_dump_files(other_dump, dump_dir, skip=["structure.json"])
    await db_manager.execute(target_db, MARKER_DDL)

    res = await restore(pg_anon_runner, db_params, target_db, dump_dir, mode=mode, extra=["--drop-db"])

    assert res.result_code == ResultCode.FAIL
    assert res.exception.code == ErrorCode.DUMP_STRUCTURE_MISMATCH
    assert "not from the same dump" in str(res.exception)
    assert "2 of 2 tables" in str(res.exception), "the error must say how many tables are not described"
    await assert_target_is_untouched(db_manager, target_db)


async def test_restore_stops_when_archives_come_from_a_changed_copy_of_the_same_database(
    full_dump, struct_dump, staging_db, target_db, db_params, pg_anon_runner, db_manager, tmp_path
):
    """The same tables in another database have other OIDs, so the old structure.json does not fit new archives."""
    cleaned = await cleaned_structure_dump(
        pg_anon_runner, db_params, db_manager, staging_db, struct_dump, "cleaned_archives", "SELECT 1"
    )
    dump_dir = copy_dump_files(full_dump, tmp_path / "dump")
    copy_dump_files(cleaned, dump_dir, skip=["structure.json"])

    res = await restore(pg_anon_runner, db_params, target_db, dump_dir)

    assert res.result_code == ResultCode.FAIL
    assert res.exception.code == ErrorCode.DUMP_STRUCTURE_MISMATCH
    assert not await user_schemas(db_manager, target_db), "the restore must stop before any change"


async def test_restore_stops_when_a_table_with_data_is_not_in_the_structure(
    full_dump, struct_dump, staging_db, target_db, db_params, pg_anon_runner, db_manager, tmp_path
):
    cleaned = await cleaned_structure_dump(
        pg_anon_runner, db_params, db_manager, staging_db, struct_dump, "cleaned_no_spare", "DROP TABLE fmt.spare"
    )
    dump_dir = copy_dump_files(full_dump, tmp_path / "dump")
    copy_dump_files(cleaned, dump_dir)

    res = await restore(pg_anon_runner, db_params, target_db, dump_dir)

    assert res.result_code == ResultCode.FAIL
    assert res.exception.code == ErrorCode.DUMP_DATA_TABLES_NOT_IN_STRUCTURE
    assert "1 table (fmt.spare)" in str(res.exception)
    assert not await user_schemas(db_manager, target_db), "the restore must stop before any change"


async def test_restore_with_drop_db_stops_when_a_table_with_data_is_not_in_the_structure(
    full_dump, struct_dump, staging_db, target_db, db_params, pg_anon_runner, db_manager, tmp_path
):
    cleaned = await cleaned_structure_dump(
        pg_anon_runner, db_params, db_manager, staging_db, struct_dump, "cleaned_no_spare_drop", "DROP TABLE fmt.spare"
    )
    dump_dir = copy_dump_files(full_dump, tmp_path / "dump")
    copy_dump_files(cleaned, dump_dir)
    await db_manager.execute(target_db, MARKER_DDL)

    res = await restore(pg_anon_runner, db_params, target_db, dump_dir, extra=["--drop-db"])

    assert res.result_code == ResultCode.FAIL
    assert res.exception.code == ErrorCode.DUMP_DATA_TABLES_NOT_IN_STRUCTURE
    await assert_target_is_untouched(db_manager, target_db)


async def test_restore_accepts_a_table_with_data_that_is_excluded_by_a_list(
    full_dump, struct_dump, staging_db, target_db, db_params, pg_anon_runner, db_manager, tmp_path
):
    cleaned = await cleaned_structure_dump(
        pg_anon_runner, db_params, db_manager, staging_db, struct_dump, "cleaned_no_spare_list", "DROP TABLE fmt.spare"
    )
    dump_dir = copy_dump_files(full_dump, tmp_path / "dump")
    copy_dump_files(cleaned, dump_dir)

    res = await restore(
        pg_anon_runner,
        db_params,
        target_db,
        dump_dir,
        extra=[f"--partial-tables-exclude-dict-file={input_dict('exclude_spare.py')}"],
    )

    assert res.result_code == ResultCode.DONE, f"restore failed: {res.error_message}"
    assert await count_rows(db_manager, target_db, "fmt.parent") == 5
    assert await count_rows(db_manager, target_db, "fmt.child") == 5
    tables = await db_manager.fetch(target_db, "SELECT tablename FROM pg_tables WHERE schemaname = 'fmt'")
    assert {row["tablename"] for row in tables} == {"parent", "child"}


@pytest.mark.parametrize("mode", ["restore", "sync-data-restore"])
async def test_restore_stops_without_data_files(struct_dump, target_db, db_params, pg_anon_runner, db_manager, mode):
    """A structure dump has no metadata.json; a restore of it must not end as DONE with empty tables."""
    await db_manager.execute(target_db, MARKER_DDL)

    res = await restore(pg_anon_runner, db_params, target_db, struct_dump, mode=mode, extra=["--drop-db"])

    assert res.result_code == ResultCode.FAIL
    assert res.exception.code == ErrorCode.DUMP_DATA_NOT_FOUND
    assert "metadata.json" in str(res.exception)
    assert mode in str(res.exception), "the error must name the mode that needs the data"
    await assert_target_is_untouched(db_manager, target_db)


async def test_restore_stops_when_metadata_json_lists_no_data_files(
    full_dump, target_db, db_params, pg_anon_runner, db_manager, tmp_path
):
    dump_dir = copy_dump_files(full_dump, tmp_path / "dump")
    metadata = read_json(dump_dir, "metadata.json")
    del metadata["files"]
    (dump_dir / "metadata.json").write_text(json.dumps(metadata), encoding="utf-8")

    res = await restore(pg_anon_runner, db_params, target_db, dump_dir)

    assert res.result_code == ResultCode.FAIL
    assert res.exception.code == ErrorCode.DUMP_DATA_NOT_FOUND
    assert not await user_schemas(db_manager, target_db), "the restore must stop before any change"


async def test_sync_struct_restore_needs_only_structure_files(
    full_dump, target_db, db_params, pg_anon_runner, db_manager, tmp_path
):
    other_files = [item.name for item in Path(full_dump).iterdir() if item.name not in STRUCTURE_FILES]
    dump_dir = copy_dump_files(full_dump, tmp_path / "dump", skip=other_files)
    assert {item.name for item in dump_dir.iterdir()} == set(STRUCTURE_FILES)

    res = await restore(pg_anon_runner, db_params, target_db, dump_dir, mode="sync-struct-restore")

    assert res.result_code == ResultCode.DONE, f"structure restore failed: {res.error_message}"
    assert await count_rows(db_manager, target_db, "fmt.child") == 0


async def test_sequence_value_is_skipped_when_the_target_has_no_such_sequence(
    full_dump, struct_dump, staging_db, target_db, db_params, pg_anon_runner, db_manager, tmp_path, caplog
):
    """The data files keep the value of a sequence that the structure files do not create any more."""
    cleaned = await cleaned_structure_dump(
        pg_anon_runner,
        db_params,
        db_manager,
        staging_db,
        struct_dump,
        "cleaned_no_sequence",
        "ALTER TABLE fmt.child ALTER COLUMN id DROP DEFAULT; DROP SEQUENCE fmt.child_id_seq",
    )
    dump_dir = copy_dump_files(full_dump, tmp_path / "dump")
    copy_dump_files(cleaned, dump_dir)
    assert "fmt.child_id_seq" in read_json(dump_dir, "metadata.json")["seq_lastvals"]

    caplog.clear()
    with caplog.at_level(logging.WARNING, logger="pg_anon.logger"):
        res = await restore(pg_anon_runner, db_params, target_db, dump_dir)

    assert res.result_code == ResultCode.DONE, f"restore failed: {res.error_message}"
    assert await count_rows(db_manager, target_db, "fmt.child") == 5
    sequences = await db_manager.fetch(target_db, "SELECT relname FROM pg_class WHERE relkind = 'S'")
    assert {row["relname"] for row in sequences} == {"parent_id_seq"}
    rows = await db_manager.fetch(target_db, "SELECT last_value FROM fmt.parent_id_seq")
    assert rows[0]["last_value"] == 6, "the value of a sequence that exists is still set"
    assert [message for message in caplog.messages if "Sequence fmt.child_id_seq is not in the target" in message]


def set_dump_pg_versions(dump_dir: Path, version: str) -> None:
    """Write the server and pg_dump version of the source into both description files."""
    for file_name in ("structure.json", "metadata.json"):
        description = read_json(dump_dir, file_name)
        description["pg_version"] = version
        description["pg_dump_version"] = version
        (dump_dir / file_name).write_text(json.dumps(description), encoding="utf-8")


def fake_target_versions(monkeypatch, server_version: str, pg_restore_version: str) -> None:
    """Only the version check sees these versions; the restore uses the real server and pg_restore."""
    original = Context.set_postgres_version

    def set_postgres_version(self, pg_version: str) -> None:
        original(self, pg_version)
        self.pg_version = server_version

    monkeypatch.setattr(Context, "set_postgres_version", set_postgres_version)
    monkeypatch.setattr(restore_mode, "get_pg_util_version", lambda _util: pg_restore_version)


@pytest.mark.parametrize(
    ("dump_version", "server_version", "pg_restore_version", "message"),
    [
        ("15.4", "9.6", "15.4", "Target PostgreSQL major version 9.6 is below than source 15!"),
        ("15.4", "15.4", "9.6", "pg_restore major version 9.6 is below than source pg_dump version 15!"),
        ("10.23", "9.6", "10.23", "Target PostgreSQL major version 9.6 is below than source 10!"),
        ("9.6", "9.5", "9.6", "Target PostgreSQL major version 9.5 is below than source 9.6!"),
        ("9.6", "9.6", "9.5", "pg_restore major version 9.5 is below than source pg_dump version 9.6!"),
    ],
)
async def test_restore_stops_when_target_versions_are_below_the_dump(
    full_dump,
    target_db,
    db_params,
    pg_anon_runner,
    db_manager,
    tmp_path,
    monkeypatch,
    dump_version,
    server_version,
    pg_restore_version,
    message,
):
    dump_dir = copy_dump_files(full_dump, tmp_path / "dump")
    set_dump_pg_versions(dump_dir, dump_version)
    fake_target_versions(monkeypatch, server_version, pg_restore_version)
    await db_manager.execute(target_db, MARKER_DDL)

    res = await restore(pg_anon_runner, db_params, target_db, dump_dir, extra=["--drop-db"])

    assert res.result_code == ResultCode.FAIL
    assert res.exception.code == ErrorCode.VERSION_INCOMPATIBLE
    assert str(res.exception) == message
    await assert_target_is_untouched(db_manager, target_db)


@pytest.mark.parametrize(
    ("dump_version", "target_version"),
    [("9.6", "15.4"), ("9.6", "10.23"), ("9.5", "9.6"), ("15.4", "15.2"), ("15.4", "18.0")],
)
async def test_restore_accepts_target_versions_not_below_the_dump(
    full_dump, target_db, db_params, pg_anon_runner, db_manager, tmp_path, monkeypatch, dump_version, target_version
):
    dump_dir = copy_dump_files(full_dump, tmp_path / "dump")
    set_dump_pg_versions(dump_dir, dump_version)
    fake_target_versions(monkeypatch, target_version, target_version)

    res = await restore(pg_anon_runner, db_params, target_db, dump_dir)

    assert res.result_code == ResultCode.DONE, f"restore failed: {res.error_message}"
    assert await count_rows(db_manager, target_db, "fmt.child") == 5
