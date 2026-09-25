"""Data of extension-owned tables is not dumped, except the configuration ones.

An extension script creates its tables on CREATE EXTENSION and often fills them, so dumping their
data ends with duplicate keys on restore.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from .conftest import input_dict, output_path
from pg_anon.common.enums import ResultCode

TABLES_SOURCE_DB = "pg_anon_ext_tables_source"


@pytest.fixture(scope="module")
async def ext_tables_source_db(db_manager, pg_anon_runner):
    """Source database where an extension owns tables filled with data."""
    await db_manager.create_db(TABLES_SOURCE_DB)
    res = await pg_anon_runner.run("init", TABLES_SOURCE_DB)
    assert res.result_code == ResultCode.DONE

    # pg_extension_config_dump() only works inside a CREATE EXTENSION script, so tables joined by
    # ALTER EXTENSION ADD TABLE are always plain extension-owned ones
    await db_manager.execute(
        TABLES_SOURCE_DB,
        """
        CREATE SCHEMA ext_home;
        CREATE EXTENSION pg_trgm SCHEMA ext_home;

        CREATE TABLE ext_home.internal_state (id int PRIMARY KEY, note text);
        INSERT INTO ext_home.internal_state VALUES (1, 'filled by the extension');
        ALTER EXTENSION pg_trgm ADD TABLE ext_home.internal_state;

        CREATE SCHEMA app;
        CREATE TABLE app.users (id serial PRIMARY KEY, email text NOT NULL);
        INSERT INTO app.users (email) VALUES ('alice@example.com'), ('bob@example.com');
    """,
    )

    if await _extension_available(db_manager, TABLES_SOURCE_DB, "pg_partman"):
        await db_manager.execute(
            TABLES_SOURCE_DB,
            "CREATE SCHEMA partman; CREATE EXTENSION pg_partman SCHEMA partman;",
        )

    yield TABLES_SOURCE_DB
    await db_manager.drop_db(TABLES_SOURCE_DB)


async def _extension_available(db_manager, db_name: str, extension: str) -> bool:
    rows = await db_manager.fetch(db_name, f"SELECT 1 FROM pg_available_extensions WHERE name = '{extension}'")
    return bool(rows)


async def _dump(pg_anon_runner, db_params, source_db: str, out_name: str) -> str:
    out = output_path(out_name)
    res = await pg_anon_runner.run(
        "dump",
        source_db,
        [
            f"--prepared-sens-dict-file={input_dict('empty.py')}",
            f"--output-dir={out}",
            f"--db-connections-per-process={db_params.db_connections_per_process}",
            "--clear-output-dir",
        ],
    )
    assert res.result_code == ResultCode.DONE, "dump must succeed"
    return out


def _dumped_tables(out: str) -> set[tuple[str, str]]:
    files = json.loads(Path(out, "metadata.json").read_text(encoding="utf-8"))["files"]
    return {(info["schema"], info["table"]) for info in files.values()}


async def test_extension_owned_tables_are_not_dumped(ext_tables_source_db, db_params, pg_anon_runner):
    out = await _dump(pg_anon_runner, db_params, ext_tables_source_db, "extension_owned_tables")

    tables = _dumped_tables(out)
    assert ("ext_home", "internal_state") not in tables, "an extension fills its own tables on CREATE EXTENSION"
    assert ("app", "users") in tables, "user tables must still be dumped"


async def test_configuration_tables_are_still_dumped(ext_tables_source_db, db_manager, db_params, pg_anon_runner):
    """Tables marked by pg_extension_config_dump hold user data and must survive the filter."""
    if not await _extension_available(db_manager, ext_tables_source_db, "pg_partman"):
        pytest.skip("pg_partman is not available, no contrib extension has configuration tables")

    out = await _dump(pg_anon_runner, db_params, ext_tables_source_db, "configuration_tables")

    assert ("partman", "part_config") in _dumped_tables(out), (
        "configuration tables carry user settings and must stay in the dump"
    )


async def test_restore_does_not_clash_with_extension_data(
    ext_tables_source_db, target_db, db_manager, db_params, pg_anon_runner
):
    """Restoring dumped copies over the rows inserted by CREATE EXTENSION breaks on the primary key."""
    out = await _dump(pg_anon_runner, db_params, ext_tables_source_db, "extension_data_clash")

    res = await pg_anon_runner.run(
        "restore",
        target_db,
        [
            f"--db-connections-per-process={db_params.db_connections_per_process}",
            f"--input-dir={out}",
        ],
    )
    assert res.result_code == ResultCode.DONE, "restore must succeed"

    users = await db_manager.fetch(target_db, "SELECT count(*) AS c FROM app.users")
    assert users[0]["c"] == 2, "user data must be restored"

    # pg_dump does not dump the structure of extension members either: on the target such a table
    # exists only when the extension script itself creates it
    orphans = await db_manager.fetch(target_db, "SELECT to_regclass('ext_home.internal_state') IS NOT NULL AS present")
    assert not orphans[0]["present"], "a table owned by an extension is not recreated by the dump"
