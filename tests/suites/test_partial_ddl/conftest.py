from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from pg_anon.common.db_utils import create_connection
from pg_anon.common.enums import ResultCode

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

SUITE = Path(__file__).resolve().parent

SOURCE_DB = "pg_anon_partial_ddl_source"


def input_dict(name: str) -> str:
    return str(SUITE / "input_dict" / name)


def output_path(name: str) -> str:
    out = SUITE / "output" / name
    out.mkdir(parents=True, exist_ok=True)
    return str(out)


SOURCE_DDL = """
CREATE SCHEMA IF NOT EXISTS app;
CREATE SCHEMA IF NOT EXISTS lib;

CREATE TABLE IF NOT EXISTS app.users (
    id serial PRIMARY KEY,
    name text NOT NULL
);
INSERT INTO app.users (name) VALUES ('Alice'), ('Bob') ON CONFLICT DO NOTHING;

CREATE TYPE app.currency AS ENUM ('RUB', 'KZT');
CREATE TYPE lib.currency AS ENUM ('USD', 'EUR');

CREATE TYPE lib.money_pair AS (a integer, b integer);

CREATE DOMAIN lib.positive_amount AS numeric CHECK (VALUE > 0);

CREATE TYPE public.doc_kind AS ENUM ('passport', 'visa');
CREATE FUNCTION lib.describe(x public.doc_kind) RETURNS text
    AS $$ SELECT 'doc:' || x::text $$ LANGUAGE sql IMMUTABLE;

CREATE FUNCTION public.amount_eq(lib.positive_amount, lib.positive_amount) RETURNS boolean
    AS $$ SELECT $1::numeric = $2::numeric $$ LANGUAGE sql IMMUTABLE;
CREATE OPERATOR public.=== (
    PROCEDURE = public.amount_eq,
    LEFTARG = lib.positive_amount,
    RIGHTARG = lib.positive_amount
);

CREATE FUNCTION public.amount_lt(lib.positive_amount, lib.positive_amount) RETURNS boolean
    AS $$ SELECT $1::numeric < $2::numeric $$ LANGUAGE sql IMMUTABLE;
CREATE FUNCTION public.amount_gt(lib.positive_amount, lib.positive_amount) RETURNS boolean
    AS $$ SELECT $1::numeric > $2::numeric $$ LANGUAGE sql IMMUTABLE;
CREATE OPERATOR public.<<< (
    PROCEDURE = public.amount_lt,
    LEFTARG = lib.positive_amount,
    RIGHTARG = lib.positive_amount,
    COMMUTATOR = OPERATOR(public.>>>)
);
CREATE OPERATOR public.>>> (
    PROCEDURE = public.amount_gt,
    LEFTARG = lib.positive_amount,
    RIGHTARG = lib.positive_amount,
    COMMUTATOR = OPERATOR(public.<<<)
);

CREATE FUNCTION public.amount_max(lib.positive_amount, lib.positive_amount)
    RETURNS lib.positive_amount
    AS $$ SELECT greatest($1::numeric, $2::numeric)::lib.positive_amount $$ LANGUAGE sql IMMUTABLE;
CREATE AGGREGATE public.max_amount(lib.positive_amount) (
    SFUNC = public.amount_max,
    STYPE = lib.positive_amount
);

CREATE FUNCTION public.doc_kind_to_text(public.doc_kind) RETURNS text
    AS $$ SELECT 'k:' || $1::text $$ LANGUAGE sql IMMUTABLE;
CREATE CAST (public.doc_kind AS text) WITH FUNCTION public.doc_kind_to_text(public.doc_kind);
"""


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
    name = f"pg_anon_partial_ddl_tgt_{request.node.name}"[:60]
    await db_manager.create_db(name)
    yield name
    await db_manager.drop_db(name)


@pytest.fixture
async def source_connection(source_db, db_manager) -> AsyncIterator[object]:
    connection = await create_connection(db_manager._connection_params(source_db))  # noqa: SLF001
    try:
        yield connection
    finally:
        await connection.close()
