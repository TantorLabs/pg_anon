"""Two foreign keys with one name on different tables: each of them is handled on its own."""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from .conftest import dump, NOT_VALID_SUMMARY, NOT_VALID_WARNING, restore
from pg_anon.common.enums import ResultCode

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

SAME_NAME_SOURCE_DB = "pg_anon_partial_fk_same_name_source"


@pytest.fixture(scope="module")
async def same_name_source_db(db_manager, pg_anon_runner, fixtures) -> AsyncIterator[str]:
    await db_manager.create_db(SAME_NAME_SOURCE_DB)
    res = await pg_anon_runner.run("init", SAME_NAME_SOURCE_DB)
    assert res.result_code == ResultCode.DONE
    await fixtures.build_same_name_fk(SAME_NAME_SOURCE_DB)

    yield SAME_NAME_SOURCE_DB
    await db_manager.drop_db(SAME_NAME_SOURCE_DB)


async def key_states(db_manager, db_name: str) -> dict[str, str]:
    """State of every key with the name fk_ref: `{table: "valid" or "not valid"}`."""
    rows = await db_manager.fetch(
        db_name,
        "SELECT conrelid::regclass::text AS table_name, convalidated FROM pg_constraint "
        "WHERE contype = 'f' AND conname = 'fk_ref'",
    )
    return {row["table_name"]: "valid" if row["convalidated"] else "not valid" for row in rows}


async def row_counts(db_manager, db_name: str) -> dict[str, int]:
    counts = {}
    for table in ("a", "b", "x", "y"):
        rows = await db_manager.fetch(db_name, f"SELECT count(*) AS count FROM dup.{table}")  # noqa: S608
        counts[table] = rows[0]["count"]
    return counts


def not_valid_lines(warnings: list[str]) -> list[str]:
    return [message for message in warnings if "created as NOT VALID" in message]


async def test_both_same_name_keys_are_in_the_dump_and_become_not_valid(
    same_name_source_db, target_db, db_manager, db_params, pg_anon_runner, caplog
):
    out = await dump(pg_anon_runner, db_params, same_name_source_db, "dup_both", dict_file="dup_both_trimmed.py")
    constraints = json.loads((Path(out) / "structure.json").read_text(encoding="utf-8"))["constraints"]
    assert {"dup.x.fk_ref", "dup.y.fk_ref"} <= set(constraints), "one key must not replace the other"
    assert constraints["dup.x.fk_ref"]["table_name_to"] == "a"
    assert constraints["dup.y.fk_ref"]["table_name_to"] == "b"

    restored = await restore(pg_anon_runner, db_params, target_db, caplog, in_dir=out)

    assert await key_states(db_manager, target_db) == {"dup.x": "not valid", "dup.y": "not valid"}
    assert await row_counts(db_manager, target_db) == {"a": 50, "b": 50, "x": 100, "y": 100}
    lines = not_valid_lines(restored.warnings)
    assert sorted(lines[:-1]) == [
        NOT_VALID_WARNING.format(key="dup.fk_ref from dup.x to dup.a"),
        NOT_VALID_WARNING.format(key="dup.fk_ref from dup.y to dup.b"),
    ], "one warning for each key must name the key with its tables"
    assert lines[-1] == NOT_VALID_SUMMARY.format(count="2 foreign keys")


async def test_same_name_keys_do_not_share_their_state(
    same_name_source_db, target_db, db_manager, db_params, pg_anon_runner, caplog
):
    out = await dump(pg_anon_runner, db_params, same_name_source_db, "dup_a", dict_file="dup_a_trimmed.py")

    restored = await restore(pg_anon_runner, db_params, target_db, caplog, in_dir=out)

    assert await key_states(db_manager, target_db) == {"dup.x": "not valid", "dup.y": "valid"}
    assert await row_counts(db_manager, target_db) == {"a": 50, "b": 100, "x": 100, "y": 100}
    assert not_valid_lines(restored.warnings) == [
        NOT_VALID_WARNING.format(key="dup.fk_ref from dup.x to dup.a"),
        NOT_VALID_SUMMARY.format(count="1 foreign key"),
    ], "the warnings must name only the key of dup.x"
