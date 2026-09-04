from __future__ import annotations

from pathlib import Path

import pytest

from pg_anon.common.enums import ResultCode

SUITE = Path(__file__).resolve().parent
SOURCE_DB = "pg_anon_fdwsec_source"

SERVER_NAME = "fdwsec_server"


def input_dict(name: str) -> str:
    return str(SUITE / "input_dict" / name)


def output_path(name: str) -> str:
    out = SUITE / "output" / name
    out.mkdir(parents=True, exist_ok=True)
    return str(out)


async def _postgres_fdw_available(db_manager, db_name: str) -> bool:
    rows = await db_manager.fetch(
        db_name,
        "SELECT 1 FROM pg_available_extensions WHERE name = 'postgres_fdw'",
    )
    return bool(rows)


async def create_server(db_manager, db_name: str) -> None:
    """Create the postgres_fdw extension and a foreign server owned by the test role."""
    await db_manager.execute(
        db_name,
        f"""
        CREATE EXTENSION IF NOT EXISTS postgres_fdw;
        CREATE SERVER IF NOT EXISTS {SERVER_NAME}
            FOREIGN DATA WRAPPER postgres_fdw
            OPTIONS (host 'localhost', dbname 'postgres', port '5432');
        """,
    )


async def add_user_mapping(db_manager, db_name: str, *, with_options: bool) -> None:
    options = "OPTIONS (user 'remote_user', password 'remote_secret')" if with_options else ""
    await db_manager.execute(
        db_name,
        f"CREATE USER MAPPING FOR CURRENT_USER SERVER {SERVER_NAME} {options};",
    )


async def drop_user_mapping(db_manager, db_name: str) -> None:
    await db_manager.execute(
        db_name,
        f"DROP USER MAPPING IF EXISTS FOR CURRENT_USER SERVER {SERVER_NAME};",
    )


async def count_user_mappings(db_manager, db_name: str) -> int:
    rows = await db_manager.fetch(db_name, "SELECT count(*) AS c FROM pg_user_mappings")
    return int(rows[0]["c"])


@pytest.fixture(scope="module")
async def source_db(db_manager, pg_anon_runner, fixtures):
    # Check availability before create_db so a skip doesn't leave an undropped test DB.
    if not await _postgres_fdw_available(db_manager, "postgres"):
        pytest.skip("postgres_fdw extension is not available in the test PostgreSQL")
    await db_manager.create_db(SOURCE_DB)
    res = await pg_anon_runner.run("init", SOURCE_DB)
    assert res.result_code == ResultCode.DONE
    await fixtures.build_minimal_env(SOURCE_DB)
    await create_server(db_manager, SOURCE_DB)
    yield SOURCE_DB
    await db_manager.drop_db(SOURCE_DB)


@pytest.fixture
async def target_db(db_manager, request):
    name = f"pg_anon_fdwsec_tgt_{request.node.name}"[:60]
    await db_manager.create_db(name)
    yield name
    await db_manager.drop_db(name)
