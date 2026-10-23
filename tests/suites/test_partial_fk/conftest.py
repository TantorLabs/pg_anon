from __future__ import annotations

import logging
import re
import zlib
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from pg_anon.common.enums import ResultCode

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

SUITE = Path(__file__).resolve().parent

SOURCE_DB = "pg_anon_partial_fk_source"

NOT_VALID_WARNING = (
    "Foreign key {key} is created as NOT VALID: the dump does not have all rows of the referenced table."
)
NOT_VALID_SUMMARY = (
    "{count} created as NOT VALID. Load the missing rows, then run for each of them: "
    "ALTER TABLE ... VALIDATE CONSTRAINT ..."
)
NOT_CREATED_WARNING = (
    "Foreign key {key} is not created: the server did not accept it, also as NOT VALID. "
    "Make the rows of these two tables match, then create the key."
)


def input_dict(name: str) -> str:
    return str(SUITE / "input_dict" / name)


def output_path(name: str) -> str:
    out = SUITE / "output" / re.sub(r"\W", "_", name)
    out.mkdir(parents=True, exist_ok=True)
    return str(out)


@dataclass
class Restored:
    """Target database after a dump and a restore, with the warnings and the whole log of the restore."""

    db_name: str
    warnings: list[str]
    messages: list[str]


async def dump(pg_anon_runner, db_params, source_db, name: str, *, dict_file: str, exclude_file: str | None = None):
    out = output_path(name)
    args = [
        f"--prepared-sens-dict-file={input_dict(dict_file)}",
        f"--output-dir={out}",
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        "--clear-output-dir",
    ]
    if exclude_file:
        args.append(f"--partial-tables-exclude-dict-file={input_dict(exclude_file)}")
    res = await pg_anon_runner.run("dump", source_db, args)
    assert res.result_code == ResultCode.DONE, f"dump failed: {res.error_message}"
    return out


async def restore(pg_anon_runner, db_params, target_db, caplog, *, in_dir: str, exclude_file: str | None = None):
    args = [
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        f"--input-dir={in_dir}",
    ]
    if exclude_file:
        args.append(f"--partial-tables-exclude-dict-file={input_dict(exclude_file)}")
    caplog.clear()
    with caplog.at_level(logging.INFO, logger="pg_anon.logger"):
        res = await pg_anon_runner.run("restore", target_db, args)
    assert res.result_code == ResultCode.DONE, f"restore failed: {res.error_message}"
    return Restored(
        target_db,
        [record.getMessage() for record in caplog.records if record.levelno == logging.WARNING],
        [record.getMessage() for record in caplog.records],
    )


async def fk_state(db_manager, db_name: str, name: str) -> str:
    """Return `valid`, `not valid` or `missing` for the key of the root table."""
    # conparentid is new in PostgreSQL 11, so it is read through JSON
    rows = await db_manager.fetch(
        db_name,
        f"""
        SELECT con.convalidated
        FROM pg_constraint con
        WHERE con.contype = 'f'
          AND con.conname = '{name}'
          AND coalesce((to_jsonb(con) ->> 'conparentid')::bigint, 0) = 0
        """,
    )
    if not rows:
        return "missing"
    return "valid" if rows[0]["convalidated"] else "not valid"


async def count_rows(db_manager, db_name: str, table: str) -> int:
    rows = await db_manager.fetch(db_name, f"SELECT count(*) FROM pfk.{table}")
    return rows[0][0]


def key_label(key: str, tables: tuple[str, str], schema: str = "pfk") -> str:
    """Name of the key in a warning: the key, its table and the table it refers to."""
    return f"{schema}.{key} from {schema}.{tables[0]} to {schema}.{tables[1]}"


def key_commands(restored: Restored, key: str) -> list[str]:
    """Commands of pg_anon for the key, in the order of the log: `ADD`, `ADD NOT VALID` or `VALIDATE`."""
    commands = []
    for message in restored.messages:
        if not message.startswith("ALTER TABLE") or f'CONSTRAINT "{key}"' not in message:
            continue
        if "VALIDATE CONSTRAINT" in message:
            commands.append("VALIDATE")
        else:
            commands.append("ADD NOT VALID" if message.endswith(" NOT VALID") else "ADD")
    return commands


def not_valid_warnings(restored: Restored, key: str) -> list[str]:
    """Warnings that say the key is left as NOT VALID."""
    return [message for message in restored.warnings if "created as NOT VALID" in message and f"pfk.{key}" in message]


def not_valid_summaries(warnings: list[str]) -> list[str]:
    """Final warnings about all keys left as NOT VALID."""
    return [message for message in warnings if "VALIDATE CONSTRAINT" in message]


def not_created_warnings(restored: Restored, key: str) -> list[str]:
    """Warnings that say the key is not created."""
    return [message for message in restored.warnings if "is not created" in message and f"pfk.{key}" in message]


def fk_warnings(restored: Restored) -> list[str]:
    return [message for message in restored.warnings if "NOT VALID" in message or "is not created" in message]


@pytest.fixture(scope="module")
async def source_db(db_manager, pg_anon_runner, fixtures) -> AsyncIterator[str]:
    await db_manager.create_db(SOURCE_DB)
    res = await pg_anon_runner.run("init", SOURCE_DB)
    assert res.result_code == ResultCode.DONE
    await fixtures.build_partial_fk(SOURCE_DB)
    await fixtures.build_partial_fk_two_levels(SOURCE_DB)
    await fixtures.build_partial_fk_null_refs(SOURCE_DB)

    yield SOURCE_DB
    await db_manager.drop_db(SOURCE_DB)


@pytest.fixture(scope="module")
async def full_dump(source_db, db_params, pg_anon_runner) -> str:
    return await dump(pg_anon_runner, db_params, source_db, "full", dict_file="empty.py")


@pytest.fixture
async def target_db(db_manager, request) -> AsyncIterator[str]:
    name = f"pg_anon_partial_fk_tgt_{zlib.crc32(request.node.name.encode())}"
    await db_manager.create_db(name)
    yield name
    await db_manager.drop_db(name)
