from __future__ import annotations

import re
import shutil
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from pg_anon.common.enums import ResultCode

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

SUITE = Path(__file__).resolve().parent

SOURCE_DB = "pg_anon_pub_member_source"
TARGET_DB = "pg_anon_pub_member_target"


def output_path(name: str) -> Path:
    out = SUITE / "output" / re.sub(r"\W", "_", name)
    out.mkdir(parents=True, exist_ok=True)
    return out


def input_dict(name: str) -> str:
    return str(SUITE / "input_dict" / name)


async def dump(pg_anon_runner, db_params, source_db: str, name: str, extra: list[str] | None = None):
    out = str(output_path(name))
    args = [
        f"--prepared-sens-dict-file={input_dict('empty.py')}",
        f"--output-dir={out}",
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        "--clear-output-dir",
        *(extra or []),
    ]
    return out, await pg_anon_runner.run("dump", source_db, args)


async def restore(pg_anon_runner, db_params, target_db: str, in_dir: str, extra: list[str] | None = None):
    args = [
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        f"--input-dir={in_dir}",
        *(extra or []),
    ]
    return await pg_anon_runner.run("restore", target_db, args)


def copy_of(dump_dir: Path | str, name: str) -> str:
    """Copy a dump into the output directory: restore writes its TOC lists next to the dump."""
    work_dir = output_path(name)
    shutil.copytree(dump_dir, work_dir, dirs_exist_ok=True)
    return str(work_dir)


async def memberships(db_manager, db_name: str) -> dict[str, list[str]]:
    """Return the tables of every publication as `schema.table`."""
    rows = await db_manager.fetch(
        db_name,
        "SELECT pubname, schemaname || '.' || tablename AS name FROM pg_publication_tables ORDER BY 1, 2",
    )
    result: dict[str, list[str]] = {}
    for row in rows:
        result.setdefault(row["pubname"], []).append(row["name"])
    return result


@pytest.fixture(scope="module")
async def source_db(db_manager, pg_anon_runner, fixtures) -> AsyncIterator[str]:
    await db_manager.create_db(SOURCE_DB)
    try:
        res = await pg_anon_runner.run("init", SOURCE_DB)
        assert res.result_code == ResultCode.DONE
        await fixtures.build_publication_membership(SOURCE_DB)
        yield SOURCE_DB
    finally:
        await db_manager.drop_db(SOURCE_DB)


@pytest.fixture(scope="module")
async def full_dump(source_db, db_params, pg_anon_runner) -> str:
    out, res = await dump(pg_anon_runner, db_params, source_db, "full")
    assert res.result_code == ResultCode.DONE, f"dump failed: {res.error_message}"
    return out


@pytest.fixture
async def target_db(db_manager) -> AsyncIterator[str]:
    await db_manager.create_db(TARGET_DB)
    yield TARGET_DB
    await db_manager.drop_db(TARGET_DB)
