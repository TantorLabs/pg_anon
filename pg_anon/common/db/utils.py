from typing import Dict, List

import asyncpg
from asyncpg import Connection

from pg_anon.common.db.queries import query_get_scan_fields


async def get_count_query_result_set(connection_params: Dict, query: str) -> int:
    db_conn: Connection = await asyncpg.connect(**connection_params)
    count = await db_conn.fetchval(f"SELECT COUNT(*) FROM ({query}) as tbl")
    await db_conn.close()

    return count


async def get_scan_fields_list(connection_params: Dict) -> List:
    db_conn = await asyncpg.connect(**connection_params)
    query = query_get_scan_fields
    fields_list = await db_conn.fetch(query)
    await db_conn.close()
    return fields_list


async def get_scan_fields_count(connection_params: Dict) -> int:
    return await get_count_query_result_set(
        connection_params=connection_params,
        query=query_get_scan_fields
    )
