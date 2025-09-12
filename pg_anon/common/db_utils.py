import re
from typing import Dict, List

import asyncpg
from asyncpg import Connection, Pool

from pg_anon.common.constants import ANON_UTILS_DB_SCHEMA_NAME, SERVER_SETTINGS, DEFAULT_EXCLUDED_SCHEMAS
from pg_anon.common.db_queries import get_scan_fields_query, get_count_query, get_database_size_query
from pg_anon.common.dto import FieldInfo, ConnectionParams
from pg_anon.logger import get_logger

logger = get_logger()


async def create_connection(connection_params: ConnectionParams, server_settings: Dict = SERVER_SETTINGS) -> Connection:
    return await asyncpg.connect(
        **connection_params.as_dict(),
        server_settings=server_settings,
    )


async def create_pool(connection_params: ConnectionParams, server_settings: Dict = SERVER_SETTINGS, min_size: int = 10, max_size: int = 10) -> Pool:
    return await asyncpg.create_pool(
        **connection_params.as_dict(),
        server_settings=server_settings,
        min_size=min_size,
        max_size=max_size,
    )


async def check_db_connection(connection_params: ConnectionParams, server_settings: Dict = SERVER_SETTINGS) -> bool:
    try:
        db_conn = await create_connection(connection_params, server_settings=server_settings)
        await db_conn.close()
    except Exception as ex:
        return False

    return True


async def check_anon_utils_db_schema_exists(connection_params: ConnectionParams, server_settings: Dict = SERVER_SETTINGS) -> bool:
    """
    Checks exists db schema what consists predefined anonymization utils
    :param connection_params: Required connection parameters such as host, login, password and etc.
    :param server_settings: Optional server settings for new connection. Can consists of timeout settings, application name and etc.
    :return: Exists schema or not
    """
    query = f"""
    select exists (select schema_name FROM information_schema.schemata where "schema_name" = '{ANON_UTILS_DB_SCHEMA_NAME}');
    """

    db_conn = await create_connection(connection_params, server_settings=server_settings)
    exists = await db_conn.fetchval(query)
    await db_conn.close()
    return exists


async def get_scan_fields_list(connection_params: ConnectionParams, server_settings: Dict = SERVER_SETTINGS, limit: int = None) -> List:
    """
    Get fields list for scan sensitive data
    :param connection_params: Required connection parameters such as host, login, password and etc.
    :param server_settings: Optional server settings for new connection. Can consists of timeout settings, application name and etc.
    :param limit: Limit the number of results to return.
    :return: resulted fields list for processing
    """
    query = get_scan_fields_query(limit=limit)

    db_conn = await create_connection(connection_params, server_settings=server_settings)
    fields_list = await db_conn.fetch(query)
    await db_conn.close()
    return fields_list


async def get_scan_fields_count(connection_params: ConnectionParams, server_settings: Dict = SERVER_SETTINGS) -> int:
    """
    Get count of fields for scan sensitive data
    :param connection_params: Required connection parameters such as host, login, password and etc.
    :param server_settings: Optional server settings for new connection. Can consists of timeout settings, application name and etc.
    :return: count of resulted fields list for processing
    """
    query = get_scan_fields_query(count_only=True)

    db_conn = await create_connection(connection_params, server_settings=server_settings)
    count = await db_conn.fetchval(query)
    await db_conn.close()
    return count


async def get_fields_list(connection_params: ConnectionParams, table_schema: str, table_name: str, server_settings: Dict = SERVER_SETTINGS) -> List:
    """
    Get fields list for dump
    :param connection_params: Required connection parameters such as host, login, password and etc.
    :param table_schema: Table schema name
    :param table_name: Table name
    :param server_settings: Optional server settings for new connection. Can consists of timeout settings, application name and etc.
    :return: fields list for dump
    """
    db_conn = await create_connection(connection_params, server_settings=server_settings)
    fields_list = await db_conn.fetch(
        """
            SELECT column_name, udt_name, is_nullable FROM information_schema.columns
            WHERE table_schema = '%s' AND table_name='%s'
            ORDER BY ordinal_position ASC
        """
        % (table_schema.replace("'", "''"), table_name.replace("'", "''"))
    )
    await db_conn.close()
    return fields_list


async def get_rows_count(connection_params: ConnectionParams, schema_name: str, table_name: str, server_settings: Dict = SERVER_SETTINGS) -> int:
    """
    Get rows count in table
    :param connection_params: Required connection parameters such as host, login, password and etc.
    :param schema_name: Schema name
    :param table_name: Table name
    :param server_settings: Optional server settings for new connection. Can consists of timeout settings, application name and etc.
    :return: rows count in table
    """
    query = get_count_query(schema_name=schema_name, table_name=table_name)
    db_conn = await create_connection(connection_params, server_settings=server_settings)
    count = await db_conn.fetchval(query)
    await db_conn.close()
    return count


async def get_db_size(connection_params: ConnectionParams, db_name: str, server_settings: Dict = SERVER_SETTINGS) -> int:
    """
    Get db size count in table
    :param connection_params: Required connection parameters such as host, login, password and etc.
    :param db_name: Database name
    :param server_settings: Optional server settings for new connection. Can consists of timeout settings, application name and etc.
    :return: database size in bytes
    """
    query = get_database_size_query(db_name=db_name)
    db_conn = await create_connection(connection_params, server_settings=server_settings)
    result = await db_conn.fetchval(query)
    await db_conn.close()
    return result


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


async def get_tables_to_dump(connection: Connection, excluded_schemas: List[str]) -> List:
    excluded_schemas_str = ", ".join(
        [f"'{v}'" for v in [*excluded_schemas, *DEFAULT_EXCLUDED_SCHEMAS]]
    )

    query_db_obj = f"""
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE
                table_schema not in ({excluded_schemas_str}) and
                table_type = 'BASE TABLE'
        """

    db_objs = await connection.fetch(query_db_obj)
    return db_objs


async def check_db_is_empty(connection: Connection) -> bool:
    return await connection.fetchval(
            f"""
            SELECT NOT EXISTS(
                SELECT table_schema, table_name
                FROM information_schema.tables
                WHERE table_schema not in (
                        'pg_catalog',
                        'information_schema',
                        '{ANON_UTILS_DB_SCHEMA_NAME}'
                    ) AND table_type = 'BASE TABLE'
            )"""
        )


async def run_query_in_pool(pool: Pool, query: str):
    from pg_anon.common.utils import exception_helper

    logger.info(f"================> Started query {query}")

    try:
        async with pool.acquire() as connection:
            await connection.execute(query)
            logger.info(f"Execute query: {query}")
    except Exception as e:
        logger.error("Exception in run_query_in_pool:\n" + exception_helper())
        raise RuntimeError(f"Can't execute query: {query}")

    logger.info(f"<================ Finished query {query}")


async def get_pg_version(connection_params: ConnectionParams, server_settings: Dict = SERVER_SETTINGS):
    db_conn = await create_connection(connection_params, server_settings=server_settings)
    pg_version = await db_conn.fetchval("select version()")
    await db_conn.close()
    return re.findall(r"(\d+\.\d+)", str(pg_version))[0]
