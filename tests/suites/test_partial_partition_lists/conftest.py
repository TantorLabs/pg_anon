from __future__ import annotations

import re
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from pg_anon.common.enums import ResultCode

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

SUITE = Path(__file__).resolve().parent

SOURCE_DB = "pg_anon_partition_lists_source"
TARGET_DBS = ("pg_anon_partition_lists_tgt_dump", "pg_anon_partition_lists_tgt_restore")


def input_dict(name: str) -> str:
    return str(SUITE / "input_dict" / name)


def output_path(name: str) -> str:
    out = SUITE / "output" / re.sub(r"\W", "_", name)
    out.mkdir(parents=True, exist_ok=True)
    return str(out)


async def dump(pg_anon_runner, db_params, source_db, name: str, extra: list[str] | None = None):
    out = output_path(name)
    args = [
        f"--prepared-sens-dict-file={input_dict('empty.py')}",
        f"--output-dir={out}",
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        "--clear-output-dir",
        *(extra or []),
    ]
    return out, await pg_anon_runner.run("dump", source_db, args)


async def restore(pg_anon_runner, db_params, target_db, in_dir: str, extra: list[str] | None = None):
    args = [
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        f"--input-dir={in_dir}",
        *(extra or []),
    ]
    return await pg_anon_runner.run("restore", target_db, args)


async def tables_with_rows(db_manager, db_name: str) -> dict[str, int]:
    """Return the tables of schema plist with the number of rows; a parent counts the rows of its partitions."""
    names = await db_manager.fetch(
        db_name,
        """
        SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'plist' AND c.relkind IN ('r', 'p')
        """,
    )
    result = {}
    for row in names:
        counted = await db_manager.fetch(db_name, f'SELECT count(*) FROM plist."{row[0]}"')
        result[row[0]] = counted[0][0]
    return result


@pytest.fixture(scope="module")
async def source_db(db_manager, pg_anon_runner, fixtures) -> AsyncIterator[str]:
    await db_manager.create_db(SOURCE_DB)
    res = await pg_anon_runner.run("init", SOURCE_DB)
    assert res.result_code == ResultCode.DONE
    await fixtures.build_partition_lists(SOURCE_DB)

    yield SOURCE_DB
    await db_manager.drop_db(SOURCE_DB)


@pytest.fixture(scope="module")
async def full_dump(source_db, db_params, pg_anon_runner) -> str:
    out, res = await dump(pg_anon_runner, db_params, source_db, "full")
    assert res.result_code == ResultCode.DONE, f"dump failed: {res.error_message}"
    return out


@pytest.fixture
async def target_dbs(db_manager) -> AsyncIterator[tuple[str, str]]:
    for name in TARGET_DBS:
        await db_manager.create_db(name)
    yield TARGET_DBS
    for name in TARGET_DBS:
        await db_manager.drop_db(name)
