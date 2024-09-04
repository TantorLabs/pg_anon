from typing import Dict, List

import asyncpg
from asyncpg import Connection

from pg_anon.common.db_queries import get_query_get_scan_fields
from pg_anon.common.dto import FieldInfo


async def get_scan_fields_list(connection_params: Dict, limit: int = None) -> List:
    db_conn = await asyncpg.connect(**connection_params)
    query = get_query_get_scan_fields(limit=limit)
    fields_list = await db_conn.fetch(query)
    await db_conn.close()
    return fields_list


async def get_scan_fields_count(connection_params: Dict) -> int:
    db_conn = await asyncpg.connect(**connection_params)
    query = get_query_get_scan_fields(count_only=True)
    count = await db_conn.fetchval(query)
    await db_conn.close()
    return count


async def get_fields_list(connection_params: Dict, table_schema: str, table_name: str) -> List:
    db_conn = await asyncpg.connect(**connection_params)
    fields_list = await db_conn.fetch(
        """
            SELECT column_name, udt_name FROM information_schema.columns
            WHERE table_schema = '%s' AND table_name='%s'
            ORDER BY ordinal_position ASC
        """
        % (table_schema.replace("'", "''"), table_name.replace("'", "''"))
    )
    await db_conn.close()
    return fields_list


async def exec_data_scan_func_query(connection: Connection, scan_func: str, value, field_info: FieldInfo) -> bool:
    """
    Execute scan in data by custom DB function
    :param connection: Active connection to db
    :param scan_func: DB function name which can call with "(value, schema, table, column_name)" and returns boolean value
    :param value: Data value from field
    :param field_info: Field info
    :return: If it sensitive by scan func then return **True**, otherwise **False**
    """

    query = f"""SELECT {scan_func}($1, $2, $3, $4)"""
    statement = await connection.prepare(query)
    res = await statement.fetchval(
        value, field_info.nspname, field_info.relname, field_info.column_name
    )

    return res
