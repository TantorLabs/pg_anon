import hashlib
import re
from typing import Dict, List, Optional, Tuple

import asyncpg
from asyncpg import Connection, Pool

from pg_anon.common.constants import ANON_UTILS_DB_SCHEMA_NAME, SERVER_SETTINGS, DEFAULT_EXCLUDED_SCHEMAS
from pg_anon.common.db_queries import get_scan_fields_query, get_count_query, get_database_size_query
from pg_anon.common.dto import FieldInfo, ConnectionParams
from pg_anon.common.utils import get_dict_rule_for_table
from pg_anon.context import Context
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


async def get_db_tables(
        connection_params: ConnectionParams,
        excluded_schemas: Optional[List[str]] = None,
        server_settings: Dict = SERVER_SETTINGS
) -> List[Tuple[str, str]]:
    if not excluded_schemas:
        excluded_schemas = []

    excluded_schemas_str = ", ".join(
        [f"'{v}'" for v in [*excluded_schemas, *DEFAULT_EXCLUDED_SCHEMAS]]
    )

    query = f"""
            SELECT t.table_schema, t.table_name
            FROM information_schema.tables t
            JOIN pg_class c ON c.relname = t.table_name
            JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = t.table_schema
            LEFT JOIN pg_partitioned_table pt ON pt.partrelid = c.oid
            WHERE
                t.table_schema NOT IN ({excluded_schemas_str})
                AND t.table_type = 'BASE TABLE'
                AND pt.partrelid IS NULL;
        """

    connection = await create_connection(connection_params, server_settings=server_settings)
    tables = await connection.fetch(query)
    await connection.close()
    return list(map(tuple, tables))


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


async def get_dump_query(
        ctx: Context,
        table_schema: str,
        table_name: str,
        table_rule: Optional[Dict] = None,
        nulls_last: bool = False,
        files: Optional[Dict] = None
):
    table_name_full = f'"{table_schema}"."{table_name}"'

    # black list has the highest priority for pg_dump / pg_restore
    if ctx.black_listed_tables and (table_schema, table_name) in ctx.black_listed_tables:
        ctx.logger.info("Skipping: " + str(table_name_full))
        return None

    # white list has the second priority for pg_dump / pg_restore
    if ctx.white_listed_tables and (table_schema, table_name) not in ctx.white_listed_tables:
        ctx.logger.info("Skipping: " + str(table_name_full))
        return None

    # dictionary_exclude has third priority
    if "dictionary_exclude" in ctx.prepared_dictionary_obj:
        exclude_rule = get_dict_rule_for_table(
            dictionary_rules=ctx.prepared_dictionary_obj["dictionary_exclude"],
            schema=table_schema,
            table=table_name,
        )

        if exclude_rule is not None and table_rule is None:
            ctx.logger.info("Skipping: " + str(table_name_full))
            return None

    hashed_name = hashlib.md5(
        (table_schema + "_" + table_name).encode()
    ).hexdigest()

    if files is not None:
        files[f"{hashed_name}.bin.gz"] = {"schema": table_schema, "table": table_name}

    if table_rule is None:
        # there is no table in the dictionary, so it will be transferred "as is"
        if (ctx.options.dbg_stage_1_validate_dict
                or ctx.options.dbg_stage_2_validate_data
                or ctx.options.dbg_stage_3_validate_full):
            query = "SELECT * FROM %s %s" % (table_name_full, ctx.validate_limit)
            return query
        else:
            query = f"SELECT * FROM {table_name_full}"
            return query
    else:
        # table found in dictionary
        if "raw_sql" in table_rule:
            # the table is transferred using "raw_sql"
            if (ctx.options.dbg_stage_1_validate_dict
                    or ctx.options.dbg_stage_2_validate_data
                    or ctx.options.dbg_stage_3_validate_full):
                query = table_rule["raw_sql"] + " " + ctx.validate_limit
                ctx.logger.info(str(query))
                return query
            else:
                query = table_rule["raw_sql"]
                return query
        else:
            # the table is transferred with the specific fields for anonymization
            fields_list = await get_fields_list(
                connection_params=ctx.connection_params,
                server_settings=ctx.server_settings,
                table_schema=table_schema,
                table_name=table_name
            )

            sql_expr = ""

            for cnt, column_info in enumerate(fields_list):
                column_name = column_info["column_name"]
                udt_name = column_info["udt_name"]
                field_anon_rule = table_rule["fields"].get(column_name)

                if field_anon_rule:
                    if field_anon_rule.find("SQL:") == 0:
                        sql_expr += f'({field_anon_rule[4:]}) as "{column_name}"'
                    else:
                        sql_expr += f'{field_anon_rule}::{udt_name} as "{column_name}"'
                else:
                    # field "as is"
                    sql_expr += f'"{column_name}" as "{column_name}"'

                if cnt != len(fields_list) - 1:
                    sql_expr += ",\n"

            query = f"SELECT {sql_expr} FROM {table_name_full}"
            if (ctx.options.dbg_stage_1_validate_dict
                    or ctx.options.dbg_stage_2_validate_data
                    or ctx.options.dbg_stage_3_validate_full):
                query += f" {ctx.validate_limit}"

            if nulls_last:
                ordering = ", ".join([
                    field["column_name"] + ' NULLS LAST' for field in fields_list
                    if field["is_nullable"].lower() == "yes"
                ])
                query += f" ORDER BY {ordering}"

            return query
