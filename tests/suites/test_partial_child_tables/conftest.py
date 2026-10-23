from __future__ import annotations

import re
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from pg_anon.common.enums import ResultCode

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

SUITE = Path(__file__).resolve().parent

TARGET_DBS = ("pg_anon_child_tables_tgt_dump", "pg_anon_child_tables_tgt_restore")


def output_path(name: str) -> Path:
    out = SUITE / "output" / re.sub(r"\W", "_", name)
    out.mkdir(parents=True, exist_ok=True)
    return out


def list_options(name: str, schema: str, include: list[str], exclude: list[str]) -> list[str]:
    """Write the white and the black list of the scenario as dictionary files and return their options."""
    options = []
    for option, tables in (("--partial-tables-dict-file", include), ("--partial-tables-exclude-dict-file", exclude)):
        if not tables:
            continue
        path = output_path("lists") / f"{name}{option.replace('-', '_')}.py"
        path.write_text(repr({"tables": [{"schema": schema, "table": table} for table in tables]}), encoding="utf-8")
        options.append(f"{option}={path}")
    return options


async def dump(pg_anon_runner, db_params, source_db, name: str, extra: list[str] | None = None):
    out = str(output_path(name))
    args = [
        f"--prepared-sens-dict-file={SUITE / 'input_dict' / 'empty.py'}",
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


async def tables_state(db_manager, db_name: str, schema: str) -> dict[str, int | str]:
    """Return the tables of the schema: own rows of a table, or the kind of a foreign or partitioned table."""
    relations = await db_manager.fetch(
        db_name,
        f"""
        SELECT c.relname, c.relkind::text AS relkind FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = '{schema}' AND c.relkind IN ('r', 'p', 'f')
        """,
    )
    kinds = {"f": "foreign", "p": "partitioned"}
    result: dict[str, int | str] = {}
    for row in relations:
        if row["relkind"] in kinds:
            result[row["relname"]] = kinds[row["relkind"]]
            continue
        counted = await db_manager.fetch(db_name, f'SELECT count(*) FROM ONLY "{schema}"."{row["relname"]}"')
        result[row["relname"]] = counted[0][0]
    return result


async def _source_db(db_manager, pg_anon_runner, name: str, build, needs_fdw: bool = False) -> AsyncIterator[str]:
    await db_manager.create_db(name)
    try:
        if needs_fdw and not await db_manager.fetch(
            name, "SELECT 1 FROM pg_available_extensions WHERE name = 'postgres_fdw'"
        ):
            pytest.skip("postgres_fdw is not available on this server")
        res = await pg_anon_runner.run("init", name)
        assert res.result_code == ResultCode.DONE
        await build(name)
        yield name
    finally:
        await db_manager.drop_db(name)


async def _full_dump(pg_anon_runner, db_params, source_db: str) -> str:
    out, res = await dump(pg_anon_runner, db_params, source_db, f"full_{source_db}")
    assert res.result_code == ResultCode.DONE, f"dump failed: {res.error_message}"
    return out


@pytest.fixture(scope="module")
async def inherits_db(db_manager, pg_anon_runner, fixtures) -> AsyncIterator[str]:
    async for name in _source_db(
        db_manager, pg_anon_runner, "pg_anon_child_tables_inherits", fixtures.build_inherits_lists
    ):
        yield name


@pytest.fixture(scope="module")
async def inherits_full_dump(inherits_db, db_params, pg_anon_runner) -> str:
    return await _full_dump(pg_anon_runner, db_params, inherits_db)


@pytest.fixture(scope="module")
async def foreign_db(db_manager, pg_anon_runner, fixtures) -> AsyncIterator[str]:
    async for name in _source_db(
        db_manager, pg_anon_runner, "pg_anon_child_tables_foreign", fixtures.build_foreign_lists, needs_fdw=True
    ):
        yield name


@pytest.fixture(scope="module")
async def foreign_full_dump(foreign_db, db_params, pg_anon_runner) -> str:
    return await _full_dump(pg_anon_runner, db_params, foreign_db)


@pytest.fixture(scope="module")
async def foreign_child_db(db_manager, pg_anon_runner, fixtures) -> AsyncIterator[str]:
    async for name in _source_db(
        db_manager, pg_anon_runner, "pg_anon_child_tables_fchild", fixtures.build_foreign_child, needs_fdw=True
    ):
        yield name


@pytest.fixture(scope="module")
async def foreign_child_full_dump(foreign_child_db, db_params, pg_anon_runner) -> str:
    return await _full_dump(pg_anon_runner, db_params, foreign_child_db)


@pytest.fixture(scope="module")
async def foreign_partition_db(db_manager, pg_anon_runner, fixtures) -> AsyncIterator[str]:
    async for name in _source_db(
        db_manager, pg_anon_runner, "pg_anon_child_tables_fpart", fixtures.build_foreign_partition, needs_fdw=True
    ):
        yield name


@pytest.fixture(scope="module")
async def foreign_partition_full_dump(foreign_partition_db, db_params, pg_anon_runner) -> str:
    return await _full_dump(pg_anon_runner, db_params, foreign_partition_db)


@pytest.fixture
async def target_dbs(db_manager) -> AsyncIterator[tuple[str, str]]:
    for name in TARGET_DBS:
        await db_manager.create_db(name)
    yield TARGET_DBS
    for name in TARGET_DBS:
        await db_manager.drop_db(name)


async def check_both_paths(
    scenario, schema, lists, expected, source_db, full_dump, target_dbs, db_manager, db_params, pg_anon_runner
) -> None:
    """Apply the lists on dump and on restore of the full dump; both databases must be equal to the expected one."""
    from tests.infrastructure.assertions import catalog_snapshot, snapshot_diff  # noqa: PLC0415

    options = list_options(scenario, schema, lists.get("include", []), lists.get("exclude", []))
    listed_on_dump, listed_on_restore = target_dbs

    out, res = await dump(pg_anon_runner, db_params, source_db, scenario, options)
    assert res.result_code == ResultCode.DONE, f"dump with the lists failed: {res.error_message}"
    res = await restore(pg_anon_runner, db_params, listed_on_dump, out)
    assert res.result_code == ResultCode.DONE, f"restore of the dump with the lists failed: {res.error_message}"

    res = await restore(pg_anon_runner, db_params, listed_on_restore, full_dump, options)
    assert res.result_code == ResultCode.DONE, f"restore with the lists failed: {res.error_message}"

    assert await tables_state(db_manager, listed_on_dump, schema) == expected, "lists on dump"
    assert await tables_state(db_manager, listed_on_restore, schema) == expected, "lists on restore"
    assert not snapshot_diff(
        await catalog_snapshot(db_manager, listed_on_dump), await catalog_snapshot(db_manager, listed_on_restore)
    ), "the same lists must give the same database on dump and on restore"
