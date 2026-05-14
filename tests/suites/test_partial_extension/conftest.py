from __future__ import annotations

from pathlib import Path

import pytest

from pg_anon.common.enums import ResultCode

SUITE = Path(__file__).resolve().parent

SOURCE_DB = "pg_anon_partial_ext_source"


def input_dict(name: str) -> str:
    return str(SUITE / "input_dict" / name)


def output_path(name: str) -> str:
    out = SUITE / "output" / name
    out.mkdir(parents=True, exist_ok=True)
    return str(out)


@pytest.fixture(scope="module")
async def source_db(db_manager, pg_anon_runner):
    await db_manager.create_db(SOURCE_DB)
    res = await pg_anon_runner.run("init", SOURCE_DB)
    assert res.result_code == ResultCode.DONE

    await db_manager.execute(SOURCE_DB, """
        CREATE SCHEMA IF NOT EXISTS trgm_home;
        CREATE EXTENSION IF NOT EXISTS pg_trgm SCHEMA trgm_home;

        CREATE SCHEMA IF NOT EXISTS app;
        CREATE TABLE IF NOT EXISTS app.users (
            id serial PRIMARY KEY,
            name text NOT NULL,
            email text NOT NULL
        );
        INSERT INTO app.users (name, email) VALUES
            ('Alice', 'alice@example.com'),
            ('Bob',   'bob@example.com'),
            ('Carol', 'carol@example.com')
        ON CONFLICT DO NOTHING;

        CREATE SCHEMA IF NOT EXISTS other;
        CREATE TABLE IF NOT EXISTS other.unrelated (id serial PRIMARY KEY, val text);
        INSERT INTO other.unrelated (val) VALUES ('x') ON CONFLICT DO NOTHING;
    """)

    yield SOURCE_DB
    await db_manager.drop_db(SOURCE_DB)


@pytest.fixture
async def target_db(db_manager, request):
    name = f"pg_anon_partial_ext_tgt_{request.node.name}"[:60]
    await db_manager.create_db(name)
    yield name
    await db_manager.drop_db(name)
