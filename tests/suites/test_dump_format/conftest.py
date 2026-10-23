from __future__ import annotations

import json
import re
import shutil
import zlib
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from pg_anon.common.enums import ResultCode

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterable

SUITE = Path(__file__).resolve().parent

SOURCE_DB = "pg_anon_dump_format_source"
OTHER_DB = "pg_anon_dump_format_other"
INHERITS_DB = "pg_anon_dump_format_inherits"
PARTITIONS_DB = "pg_anon_dump_format_partitions"

STRUCTURE_FILES = ("pre_data.backup", "post_data.backup", "structure.json")

SOURCE_DDL = """
CREATE SCHEMA fmt;
CREATE TABLE fmt.parent (id serial PRIMARY KEY, name text NOT NULL);
CREATE TABLE fmt.child (
    id serial PRIMARY KEY,
    parent_id int NOT NULL CONSTRAINT child_parent_fk REFERENCES fmt.parent (id),
    note text
);
CREATE INDEX child_note_idx ON fmt.child (note);
CREATE TABLE fmt.spare (id int PRIMARY KEY, label text);

INSERT INTO fmt.parent (name) SELECT 'parent ' || g FROM generate_series(1, 5) g;
INSERT INTO fmt.child (parent_id, note) SELECT g, 'note ' || g FROM generate_series(1, 5) g;
INSERT INTO fmt.spare SELECT g, 'label ' || g FROM generate_series(1, 3) g;
"""

OTHER_DDL = """
CREATE SCHEMA other;
CREATE TABLE other.x (id int PRIMARY KEY);
CREATE TABLE other.y (id int PRIMARY KEY);
INSERT INTO other.x SELECT generate_series(1, 2);
"""


def input_dict(name: str) -> str:
    return str(SUITE / "input_dict" / name)


def output_path(name: str) -> str:
    out = SUITE / "output" / re.sub(r"\W", "_", name)
    out.mkdir(parents=True, exist_ok=True)
    return str(out)


def read_json(dump_dir: str | Path, file_name: str) -> dict:
    return json.loads((Path(dump_dir) / file_name).read_text(encoding="utf-8"))


def copy_dump_files(dump_dir: str | Path, target_dir: Path, skip: Iterable[str] = ()) -> Path:
    """Copy the files of a dump into a directory, as a user does; files with the same name are replaced."""
    target_dir.mkdir(parents=True, exist_ok=True)
    for item in Path(dump_dir).iterdir():
        if item.is_file() and item.name not in skip:
            shutil.copy2(item, target_dir / item.name)
    return target_dir


async def dump(pg_anon_runner, db_params, source_db: str, name: str, mode: str = "dump") -> str:
    out = output_path(name)
    res = await pg_anon_runner.run(
        mode,
        source_db,
        [
            f"--prepared-sens-dict-file={input_dict('empty.py')}",
            f"--output-dir={out}",
            f"--db-connections-per-process={db_params.db_connections_per_process}",
            "--clear-output-dir",
        ],
    )
    assert res.result_code == ResultCode.DONE, f"{mode} failed: {res.error_message}"
    return out


async def restore(pg_anon_runner, db_params, target_db: str, in_dir: str | Path, mode: str = "restore", extra=None):
    args = [
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        f"--input-dir={in_dir}",
    ]
    if extra:
        args.extend(extra)
    return await pg_anon_runner.run(mode, target_db, args)


async def cleaned_structure_dump(pg_anon_runner, db_params, db_manager, staging_db, struct_dump, name, cleanup_sql):
    """Restore the structure into a staging database, change it there and dump the structure again."""
    res = await restore(pg_anon_runner, db_params, staging_db, struct_dump, mode="sync-struct-restore")
    assert res.result_code == ResultCode.DONE, f"structure restore failed: {res.error_message}"
    await db_manager.execute(staging_db, cleanup_sql)
    return await dump(pg_anon_runner, db_params, staging_db, name, "sync-struct-dump")


async def count_rows(db_manager, db_name: str, table: str) -> int:
    rows = await db_manager.fetch(db_name, f"SELECT count(*) AS count FROM {table}")  # noqa: S608
    return rows[0]["count"]


async def user_schemas(db_manager, db_name: str) -> set[str]:
    rows = await db_manager.fetch(
        db_name,
        "SELECT nspname FROM pg_namespace "
        "WHERE nspname NOT LIKE 'pg\\_%' AND nspname NOT IN ('information_schema', 'public')",
    )
    return {row["nspname"] for row in rows}


async def _create_source(db_manager, pg_anon_runner, db_name: str, ddl: str) -> None:
    await db_manager.create_db(db_name)
    res = await pg_anon_runner.run("init", db_name)
    assert res.result_code == ResultCode.DONE
    await db_manager.execute(db_name, ddl)


@pytest.fixture(scope="module")
async def source_db(db_manager, pg_anon_runner) -> AsyncIterator[str]:
    await _create_source(db_manager, pg_anon_runner, SOURCE_DB, SOURCE_DDL)
    yield SOURCE_DB
    await db_manager.drop_db(SOURCE_DB)


@pytest.fixture(scope="module")
async def other_db(db_manager, pg_anon_runner) -> AsyncIterator[str]:
    await _create_source(db_manager, pg_anon_runner, OTHER_DB, OTHER_DDL)
    yield OTHER_DB
    await db_manager.drop_db(OTHER_DB)


async def _built_source(db_manager, pg_anon_runner, db_name: str, build) -> AsyncIterator[str]:
    await db_manager.create_db(db_name)
    res = await pg_anon_runner.run("init", db_name)
    assert res.result_code == ResultCode.DONE
    await build(db_name)
    yield db_name
    await db_manager.drop_db(db_name)


@pytest.fixture(scope="module")
async def inherits_db(db_manager, pg_anon_runner, fixtures) -> AsyncIterator[str]:
    async for name in _built_source(db_manager, pg_anon_runner, INHERITS_DB, fixtures.build_inherits_lists):
        yield name


@pytest.fixture(scope="module")
async def partitions_db(db_manager, pg_anon_runner, fixtures) -> AsyncIterator[str]:
    async for name in _built_source(db_manager, pg_anon_runner, PARTITIONS_DB, fixtures.build_partition_lists):
        yield name


@pytest.fixture(scope="module")
async def full_dump(source_db, db_params, pg_anon_runner) -> str:
    return await dump(pg_anon_runner, db_params, source_db, "full")


@pytest.fixture(scope="module")
async def struct_dump(source_db, db_params, pg_anon_runner) -> str:
    return await dump(pg_anon_runner, db_params, source_db, "struct", "sync-struct-dump")


@pytest.fixture(scope="module")
async def data_dump(source_db, db_params, pg_anon_runner) -> str:
    return await dump(pg_anon_runner, db_params, source_db, "data", "sync-data-dump")


async def _temp_db(db_manager, request, prefix: str) -> AsyncIterator[str]:
    name = f"pg_anon_dump_format_{prefix}_{zlib.crc32(request.node.name.encode())}"
    await db_manager.create_db(name)
    yield name
    await db_manager.drop_db(name)


@pytest.fixture
async def target_db(db_manager, request) -> AsyncIterator[str]:
    async for name in _temp_db(db_manager, request, "tgt"):
        yield name


@pytest.fixture
async def staging_db(db_manager, request) -> AsyncIterator[str]:
    async for name in _temp_db(db_manager, request, "stg"):
        yield name
