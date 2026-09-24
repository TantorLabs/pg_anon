from __future__ import annotations

import re
import zlib
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from pg_anon.common.enums import ResultCode

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

SUITE = Path(__file__).resolve().parent

SOURCE_DB = "pg_anon_exclude_filters_source"


def input_dict(name: str) -> str:
    return str(SUITE / "input_dict" / name)


def output_path(name: str) -> str:
    # a dump holds .py files, so mypy needs the directory name to be a valid module name
    out = SUITE / "output" / re.sub(r"\W", "_", name)
    out.mkdir(parents=True, exist_ok=True)
    return str(out)


# names with a space and capitals check quoting; crm refers to profile from another schema
SOURCE_DDL = """
CREATE SCHEMA s_excl;
CREATE TYPE s_excl.color AS ENUM ('red', 'green');
CREATE TABLE s_excl.a (id int PRIMARY KEY, tone s_excl.color DEFAULT 'red');
INSERT INTO s_excl.a (id) SELECT generate_series(1, 5);
COMMENT ON SCHEMA s_excl IS 'excluded';
GRANT USAGE ON SCHEMA s_excl TO PUBLIC;

CREATE SCHEMA s_keep;
CREATE TABLE s_keep.b (id int PRIMARY KEY);
INSERT INTO s_keep.b SELECT generate_series(1, 5);
CREATE TABLE s_keep."my table" (id int PRIMARY KEY);
INSERT INTO s_keep."my table" SELECT generate_series(1, 3);
CREATE TABLE s_keep."MixedCase" (id int PRIMARY KEY);
INSERT INTO s_keep."MixedCase" SELECT generate_series(1, 3);

-- only the "pg_" prefix is reserved, so "pgq" is a normal user schema
CREATE SCHEMA pgq;
CREATE TABLE pgq.queue (id int PRIMARY KEY);
INSERT INTO pgq.queue SELECT generate_series(1, 2);

CREATE SCHEMA "my schema";
CREATE TABLE "my schema".t (id int PRIMARY KEY);
INSERT INTO "my schema".t SELECT generate_series(1, 4);

CREATE SCHEMA profile;
CREATE TABLE profile.owners (id int PRIMARY KEY);
CREATE TABLE profile.items (id int PRIMARY KEY, owner_id int REFERENCES profile.owners(id));
INSERT INTO profile.owners SELECT generate_series(1, 5);
INSERT INTO profile.items SELECT g, g FROM generate_series(1, 5) g;

-- no tables, so an old dump knows this schema only from the TOC
CREATE SCHEMA only_types;
CREATE TYPE only_types.tone AS ENUM ('a');

CREATE SCHEMA crm;
CREATE TABLE crm.orders (id int PRIMARY KEY, owner_id int REFERENCES profile.owners(id));
INSERT INTO crm.orders SELECT g, g FROM generate_series(1, 3) g;
CREATE VIEW crm.owner_orders AS SELECT o.id, o.owner_id FROM crm.orders o JOIN profile.owners w ON w.id = o.owner_id;
"""


async def dump(pg_anon_runner, db_params, source_db, *, out_dir, dict_file, mode="dump", extra=None):
    args = [
        f"--prepared-sens-dict-file={dict_file}",
        f"--output-dir={out_dir}",
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        "--clear-output-dir",
    ]
    if extra:
        args.extend(extra)
    return await pg_anon_runner.run(mode, source_db, args)


async def restore(pg_anon_runner, db_params, target_db, *, in_dir, mode="restore", extra=None):
    args = [
        f"--db-connections-per-process={db_params.db_connections_per_process}",
        f"--input-dir={in_dir}",
    ]
    if extra:
        args.extend(extra)
    return await pg_anon_runner.run(mode, target_db, args)


@pytest.fixture(scope="module")
async def source_db(db_manager, pg_anon_runner) -> AsyncIterator[str]:
    await db_manager.create_db(SOURCE_DB)
    res = await pg_anon_runner.run("init", SOURCE_DB)
    assert res.result_code == ResultCode.DONE

    await db_manager.execute(SOURCE_DB, SOURCE_DDL)

    yield SOURCE_DB
    await db_manager.drop_db(SOURCE_DB)


@pytest.fixture
async def target_db(db_manager, request) -> AsyncIterator[str]:
    name = f"pg_anon_exclude_filters_tgt_{zlib.crc32(request.node.name.encode())}"
    await db_manager.create_db(name)
    yield name
    await db_manager.drop_db(name)
