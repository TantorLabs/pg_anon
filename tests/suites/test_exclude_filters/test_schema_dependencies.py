from __future__ import annotations

import json
from pathlib import Path

import pytest

from .conftest import dump, input_dict, output_path
from pg_anon.common.constants import ANON_UTILS_DB_SCHEMA_NAME
from pg_anon.common.enums import ResultCode
from pg_anon.common.errors import ErrorCode

DEPS_DB = "pg_anon_exclude_filters_deps"

# keep depends on lib through a column type and through a partition parent
DEPS_DDL = """
CREATE SCHEMA lib;
CREATE TYPE lib.color AS ENUM ('red', 'green');
CREATE TABLE lib.events (id int, day date) PARTITION BY RANGE (day);

CREATE SCHEMA keep;
CREATE TABLE keep.uses_type (id int PRIMARY KEY, tone lib.color);
CREATE TABLE keep.events_2024 PARTITION OF lib.events FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
CREATE TABLE keep.plain (id int PRIMARY KEY);

CREATE SCHEMA other;
CREATE TABLE other.t (id int PRIMARY KEY);
"""


@pytest.fixture(scope="module")
async def deps_db(db_manager, pg_anon_runner):
    await db_manager.create_db(DEPS_DB)
    res = await pg_anon_runner.run("init", DEPS_DB)
    assert res.result_code == ResultCode.DONE

    await db_manager.execute(DEPS_DB, DEPS_DDL)
    yield DEPS_DB
    await db_manager.drop_db(DEPS_DB)


async def dump_deps(pg_anon_runner, db_params, deps_db, name: str, options: list[str], mode: str = "dump"):
    return await dump(
        pg_anon_runner,
        db_params,
        deps_db,
        out_dir=output_path(name),
        dict_file=input_dict("empty.py"),
        mode=mode,
        extra=options,
    )


@pytest.mark.parametrize("mode", ["dump", "sync-struct-dump"])
async def test_dump_stops_when_kept_tables_need_an_excluded_schema(deps_db, db_params, pg_anon_runner, mode):
    res = await dump_deps(pg_anon_runner, db_params, deps_db, f"deps_{mode}", ["--exclude-schema-name=lib"], mode)

    assert res.result_code == ResultCode.FAIL
    assert res.exception.code == ErrorCode.EXCLUDED_SCHEMA_DEPENDENCY
    message = str(res.exception)
    assert "Excluded schemas are still needed by 2 tables" in message
    assert "keep.uses_type" in message
    assert "keep.events_2024" in message
    assert "keep.plain" not in message


async def test_dump_passes_when_dependent_schema_is_excluded_too(deps_db, db_params, pg_anon_runner):
    out = output_path("deps_both")
    res = await dump_deps(pg_anon_runner, db_params, deps_db, "deps_both", ["--exclude-schema-name=lib,keep"])

    assert res.result_code == ResultCode.DONE, f"dump failed: {res.error_message}"
    metadata = json.loads((Path(out) / "metadata.json").read_text())
    assert set(metadata["schemas"]) == {ANON_UTILS_DB_SCHEMA_NAME, "other", "public"}


async def test_data_only_dump_does_not_check_structure_dependencies(deps_db, db_params, pg_anon_runner):
    res = await dump_deps(
        pg_anon_runner, db_params, deps_db, "deps_data", ["--exclude-schema-name=lib"], "sync-data-dump"
    )

    assert res.result_code == ResultCode.DONE
