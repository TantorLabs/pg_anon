from __future__ import annotations

import pytest

from pg_anon.common.db_utils import (
    create_connection,
    get_indexes_data,
    get_partition_ancestors_map,
    get_partitioned_ancestors,
    get_views_related_to_tables,
)
from pg_anon.common.dto import ConnectionParams

DB_NAME = "pg_anon_param_limit"

# Comfortably above the 32767 / 2 = 16383 table ceiling of the old code.
TABLE_COUNT = 20000


@pytest.fixture(scope="module")
async def database(db_manager):
    await db_manager.create_db(DB_NAME)
    yield DB_NAME
    await db_manager.drop_db(DB_NAME)


@pytest.fixture
async def connection(database, db_params):
    conn = await create_connection(
        ConnectionParams(
            host=db_params.test_db_host,
            port=int(db_params.test_db_port),
            database=database,
            user=db_params.test_db_user,
            password=db_params.test_db_user_password,
        )
    )
    try:
        yield conn
    finally:
        await conn.close()


@pytest.fixture
def many_tables() -> list[tuple[str, str]]:
    return [("public", f"t_{i}") for i in range(TABLE_COUNT)]


async def test_get_partition_ancestors_map_over_param_limit(connection, many_tables):
    assert await get_partition_ancestors_map(connection, many_tables) == {}


async def test_get_partitioned_ancestors_over_param_limit(connection, many_tables):
    assert await get_partitioned_ancestors(connection, many_tables) == set()


async def test_get_indexes_data_over_param_limit(connection, many_tables):
    assert await get_indexes_data(connection, many_tables) == []


async def test_get_views_related_to_tables_over_param_limit(connection, many_tables):
    assert await get_views_related_to_tables(connection, many_tables) == []


async def test_get_partition_ancestors_map_resolves_real_parent(connection):
    await connection.execute("CREATE SCHEMA param_limit_pos")
    try:
        await connection.execute(
            """
            CREATE TABLE param_limit_pos.p (id int) PARTITION BY RANGE (id);
            CREATE TABLE param_limit_pos.p_0_100
                PARTITION OF param_limit_pos.p FOR VALUES FROM (0) TO (100);
            """
        )
        result = await get_partition_ancestors_map(connection, [("param_limit_pos", "p_0_100")])
        assert result == {("param_limit_pos", "p_0_100"): [("param_limit_pos", "p")]}
    finally:
        await connection.execute("DROP SCHEMA param_limit_pos CASCADE")
