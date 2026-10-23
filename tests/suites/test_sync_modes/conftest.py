from __future__ import annotations

import logging
import re
import zlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from pg_anon.common.enums import ResultCode

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

SUITE = Path(__file__).resolve().parent

UNCHECKED_KEYS_WARNING = "Data was loaded with foreign key checks turned off."


def input_dict(name: str) -> str:
    return str(SUITE / "input_dict" / name)


def output_path(name: str) -> Path:
    out = SUITE / "output" / re.sub(r"\W", "_", name)
    out.mkdir(parents=True, exist_ok=True)
    return out


@dataclass
class Filters:
    """Schema options and rules of the white and the black list of tables."""

    schema_options: list[str] = field(default_factory=list)
    include: list[dict] = field(default_factory=list)
    exclude: list[dict] = field(default_factory=list)

    def options(self, name: str) -> list[str]:
        """Write the lists as dictionary files and return all options of the filters."""
        options = list(self.schema_options)
        for option, rules in (
            ("--partial-tables-dict-file", self.include),
            ("--partial-tables-exclude-dict-file", self.exclude),
        ):
            if not rules:
                continue
            file_name = re.sub(r"\W", "_", name + option)
            path = output_path("lists") / f"{file_name}.py"
            path.write_text(repr({"tables": rules}), encoding="utf-8")
            options.append(f"{option}={path}")
        return options


def tables(schema: str, *names: str) -> list[dict]:
    return [{"schema": schema, "table": name} for name in names]


@dataclass
class Run:
    """Result of one pg_anon run with its warnings and its whole log."""

    result: object
    warnings: list[str]
    messages: list[str]


async def dump(
    pg_anon_runner, db_params, source_db: str, name: str, mode: str = "dump", extra=None, dict_file: str = "empty.py"
) -> str:
    out = str(output_path(name))
    args = [
        f"--prepared-sens-dict-file={input_dict(dict_file)}",
        f"--output-dir={out}",
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        "--clear-output-dir",
        *(extra or []),
    ]
    res = await pg_anon_runner.run(mode, source_db, args)
    assert res.result_code == ResultCode.DONE, f"{mode} failed: {res.error_message}"
    return out


async def try_restore(pg_anon_runner, db_params, target_db: str, in_dir: str, mode: str = "restore", extra=None):
    args = [
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        f"--input-dir={in_dir}",
        *(extra or []),
    ]
    return await pg_anon_runner.run(mode, target_db, args)


async def restore(pg_anon_runner, db_params, target_db: str, in_dir: str, mode: str = "restore", extra=None):
    res = await try_restore(pg_anon_runner, db_params, target_db, in_dir, mode, extra)
    assert res.result_code == ResultCode.DONE, f"{mode} failed: {res.error_message}"
    return res


async def restore_with_log(
    pg_anon_runner, db_params, target_db: str, caplog, in_dir: str, mode: str = "restore", extra=None
) -> Run:
    caplog.clear()
    with caplog.at_level(logging.INFO, logger="pg_anon.logger"):
        res = await restore(pg_anon_runner, db_params, target_db, in_dir, mode, extra)
    return Run(
        res,
        [record.getMessage() for record in caplog.records if record.levelno == logging.WARNING],
        [record.getMessage() for record in caplog.records],
    )


async def tables_rows(db_manager, db_name: str, schemas: list[str]) -> dict[str, int]:
    """Return own rows of every plain table of the schemas as `{"schema.table": rows}`."""
    names = ", ".join(f"'{schema}'" for schema in schemas)
    relations = await db_manager.fetch(
        db_name,
        f"""
        SELECT n.nspname, c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname IN ({names}) AND c.relkind = 'r'
        """,
    )
    result = {}
    for row in relations:
        counted = await db_manager.fetch(db_name, f'SELECT count(*) FROM ONLY "{row[0]}"."{row[1]}"')
        result[f"{row[0]}.{row[1]}"] = counted[0][0]
    return result


async def relations(db_manager, db_name: str, schema: str) -> dict[str, str]:
    """Return tables, partitioned tables and views of the schema with their relkind."""
    rows = await db_manager.fetch(
        db_name,
        f"""
        SELECT c.relname, c.relkind::text AS relkind FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = '{schema}' AND c.relkind IN ('r', 'p', 'v')
        """,
    )
    return {row["relname"]: row["relkind"] for row in rows}


async def fk_states(db_manager, db_name: str) -> dict[str, bool]:
    """Return the keys of root tables: name -> convalidated."""
    # conparentid is new in PostgreSQL 11, so it is read through JSON
    rows = await db_manager.fetch(
        db_name,
        """
        SELECT con.conname, con.convalidated FROM pg_constraint con
        WHERE con.contype = 'f' AND coalesce((to_jsonb(con) ->> 'conparentid')::bigint, 0) = 0
        """,
    )
    return {row["conname"]: row["convalidated"] for row in rows}


def fk_commands(run: Run) -> list[str]:
    """Commands of pg_anon that add, validate or drop a constraint, as the log has them."""
    return [message for message in run.messages if message.startswith("ALTER TABLE") and "CONSTRAINT" in message]


def unchecked_keys_warnings(run: Run) -> list[str]:
    return [message for message in run.warnings if message.startswith(UNCHECKED_KEYS_WARNING)]


async def _source_db(db_manager, pg_anon_runner, name: str, build) -> AsyncIterator[str]:
    await db_manager.create_db(name)
    try:
        res = await pg_anon_runner.run("init", name)
        assert res.result_code == ResultCode.DONE
        await build(name)
        yield name
    finally:
        await db_manager.drop_db(name)


@dataclass
class Dumps:
    """Dumps of one source database without filters: full, only structure and only data."""

    source_db: str
    full: str
    struct: str
    data: str


async def _dumps(pg_anon_runner, db_params, source_db: str) -> Dumps:
    return Dumps(
        source_db,
        await dump(pg_anon_runner, db_params, source_db, f"{source_db}_full"),
        await dump(pg_anon_runner, db_params, source_db, f"{source_db}_struct", "sync-struct-dump"),
        await dump(pg_anon_runner, db_params, source_db, f"{source_db}_data", "sync-data-dump"),
    )


@pytest.fixture(scope="module")
async def filters_dumps(db_manager, pg_anon_runner, db_params, fixtures) -> AsyncIterator[Dumps]:
    async for name in _source_db(db_manager, pg_anon_runner, "pg_anon_sync_modes_filters", fixtures.build_sync_filters):
        yield await _dumps(pg_anon_runner, db_params, name)


@pytest.fixture(scope="module")
async def views_dumps(db_manager, pg_anon_runner, db_params, fixtures) -> AsyncIterator[Dumps]:
    async for name in _source_db(db_manager, pg_anon_runner, "pg_anon_sync_modes_views", fixtures.build_sync_views):
        yield await _dumps(pg_anon_runner, db_params, name)


@pytest.fixture(scope="module")
async def partitions_dumps(db_manager, pg_anon_runner, db_params, fixtures) -> AsyncIterator[Dumps]:
    async for name in _source_db(
        db_manager, pg_anon_runner, "pg_anon_sync_modes_partitions", fixtures.build_partition_lists
    ):
        yield await _dumps(pg_anon_runner, db_params, name)


@pytest.fixture(scope="module")
async def inherits_dumps(db_manager, pg_anon_runner, db_params, fixtures) -> AsyncIterator[Dumps]:
    async for name in _source_db(
        db_manager, pg_anon_runner, "pg_anon_sync_modes_inherits", fixtures.build_inherits_lists
    ):
        yield await _dumps(pg_anon_runner, db_params, name)


@pytest.fixture(scope="module")
async def row_type_dumps(db_manager, pg_anon_runner, db_params, fixtures) -> AsyncIterator[Dumps]:
    async def build(name: str) -> None:
        await fixtures.build_partial_dependency_case(name, "row_column")

    async for name in _source_db(db_manager, pg_anon_runner, "pg_anon_sync_modes_row_type", build):
        yield await _dumps(pg_anon_runner, db_params, name)


@pytest.fixture(scope="module")
async def partial_fk_dumps(db_manager, pg_anon_runner, db_params, fixtures) -> AsyncIterator[Dumps]:
    async for name in _source_db(
        db_manager, pg_anon_runner, "pg_anon_sync_modes_partial_fk", fixtures.build_partial_fk
    ):
        yield await _dumps(pg_anon_runner, db_params, name)


@pytest.fixture(scope="module")
async def fk_dumps(db_manager, pg_anon_runner, db_params, fixtures) -> AsyncIterator[Dumps]:
    async for name in _source_db(db_manager, pg_anon_runner, "pg_anon_sync_modes_fk", fixtures.build_sync_fk):
        yield await _dumps(pg_anon_runner, db_params, name)


@pytest.fixture
async def target_db(db_manager, request) -> AsyncIterator[str]:
    name = f"pg_anon_sync_modes_tgt_{zlib.crc32(request.node.name.encode())}"
    await db_manager.create_db(name)
    yield name
    await db_manager.drop_db(name)


@pytest.fixture
async def second_target_db(db_manager, request) -> AsyncIterator[str]:
    name = f"pg_anon_sync_modes_tgt2_{zlib.crc32(request.node.name.encode())}"
    await db_manager.create_db(name)
    yield name
    await db_manager.drop_db(name)
