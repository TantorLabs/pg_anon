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
            SELECT column_name, udt_name, is_nullable, is_generated FROM information_schema.columns
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
        connection: Connection,
        excluded_schemas: Optional[List[str]] = None,
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

    tables = await connection.fetch(query)
    return list(map(tuple, tables))


async def get_schemas(connection: Connection) -> List[str]:
    query = f"""
    SELECT nspname AS schema_name
    FROM pg_namespace
    WHERE nspname NOT LIKE 'pg_%' AND nspname NOT IN ('information_schema')
    ORDER BY nspname;
    """

    result = await connection.fetch(query)
    return [row[0] for row in result]


async def get_custom_functions_ddl(connection: Connection) -> List[str]:
    query = f"""
    SELECT pg_get_functiondef(p.oid) AS ddl
    FROM pg_proc p
    JOIN pg_namespace n ON n.oid = p.pronamespace
    WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'anon_funcs')
    """

    result = await connection.fetch(query)
    return [row[0] for row in result]


async def get_custom_domains_ddl(connection: Connection) -> List[str]:
    query = f"""
    SELECT
        'CREATE DOMAIN ' || quote_ident(n.nspname) || '.' || quote_ident(t.typname) ||
        ' AS ' || pg_catalog.format_type(t.typbasetype, t.typtypmod) ||
        COALESCE(' DEFAULT ' || t.typdefault, '') ||
        COALESCE(' ' || pg_get_constraintdef(c.oid), '') || ';' AS ddl
    FROM pg_type t
    JOIN pg_namespace n ON n.oid = t.typnamespace
    LEFT JOIN pg_constraint c ON c.contypid = t.oid
    WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
      AND t.typtype = 'd'
    ORDER BY n.nspname, t.typname;
    """

    result = await connection.fetch(query)
    return [row[0] for row in result]


async def get_custom_types_ddl(connection: Connection) -> List[str]:
    query = f"""
    WITH user_types AS (
        SELECT
            n.nspname AS schema_name,
            t.typname AS type_name,
            t.typtype,
            t.typrelid,
            t.oid,
            t.typbasetype
        FROM pg_type t
        JOIN pg_namespace n ON n.oid = t.typnamespace
        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
          AND t.typtype IN ('c', 'e') -- composite, enum
          -- excluding row-types
          AND (t.typtype != 'c' OR t.typrelid = 0)
    )
    SELECT
        'DO $$' || E'\n' ||
        'BEGIN' || E'\n' ||
        '    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = ' || quote_literal(type_name) || ') THEN' || E'\n' ||
        '        ' ||
        CASE
            WHEN typtype = 'e' THEN
                'CREATE TYPE ' || quote_ident(schema_name) || '.' || quote_ident(type_name) ||
                ' AS ENUM (' ||
                string_agg(quote_literal(e.enumlabel), ', ') || ');'
            WHEN typtype = 'c' THEN
                'CREATE TYPE ' || quote_ident(schema_name) || '.' || quote_ident(type_name) || ' AS (' ||
                string_agg(
                    quote_ident(a.attname) || ' ' || pg_catalog.format_type(a.atttypid, a.atttypmod),
                    ', '
                ) || ');'
            END || E'\n' ||
        '    END IF;' || E'\n' ||
        'END;' || E'\n' ||
        '$$;' as ddl
    FROM user_types t
    LEFT JOIN pg_enum e ON e.enumtypid = t.oid
    LEFT JOIN pg_attribute a ON a.attrelid = t.typrelid AND a.attnum > 0
    GROUP BY schema_name, type_name, typtype, t.oid, t.typbasetype
    ORDER BY schema_name, type_name;
    """

    result = await connection.fetch(query)
    return [row[0] for row in result]


async def get_indexes_data(connection: Connection, tables: List[Tuple[str, str]]) -> List[str]:
    values_placeholders = ", ".join(f"($%s, $%s)" % (i * 2 + 1, i * 2 + 2) for i in range(len(tables)))
    args = [item for table_data in tables for item in table_data]
    query = f"""
    WITH tables_to_check AS (
        VALUES {values_placeholders}
    )
    SELECT
        n.nspname as "schema"
        ,t.relname as "table"
        ,i.relname AS "index_name"
        ,tt.column1 IS null as "is_excluded"
    FROM pg_index ix
    JOIN pg_class i ON i.oid = ix.indexrelid
    JOIN pg_class t ON t.oid = ix.indrelid
    JOIN pg_namespace n ON n.oid = t.relnamespace
    LEFT JOIN tables_to_check tt ON tt.column1 = n.nspname AND tt.column2 = t.relname
    WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast');
    """

    return await connection.fetch(query, *args)


async def get_views_related_to_tables(connection: Connection, tables: List[Tuple[str, str]]) -> List[str]:
    values_placeholders = ", ".join(f"($%s, $%s)" % (i * 2 + 1, i * 2 + 2) for i in range(len(tables)))
    args = [item for table_data in tables for item in table_data]
    query = f"""
    WITH tables_to_check AS (
        VALUES {values_placeholders}
    ),
    all_views AS (
        SELECT 
            schemaname AS view_schema,
            viewname AS view_name,
            definition AS view_definition,
            'view' AS view_type
        FROM pg_views
        WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
        UNION ALL
        SELECT 
            schemaname AS view_schema,
            matviewname AS view_name,
            definition AS view_definition,
            'materialized_view' AS view_type
        FROM pg_matviews
        WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
    ),
    all_tables AS (
        SELECT 
            n.nspname AS table_schema,
            c.relname AS table_name
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind = 'r'  -- only tables
          AND n.nspname NOT IN ('pg_catalog', 'information_schema')
    )
    select DISTINCT
        v.view_schema
        ,v.view_name
        ,v.view_type
        ,t.table_schema
        ,t.table_name
        ,tt.column1 is null as "is_excluded"
    FROM all_views v
    JOIN all_tables t
      ON v.view_definition ILIKE '%' || t.table_schema || '.' || t.table_name || '%'
       OR v.view_definition ILIKE '%' || t.table_name || '%'
    LEFT JOIN tables_to_check tt ON tt.column1 = t.table_schema AND tt.column2 = t.table_name
    ORDER BY v.view_schema, v.view_name, t.table_schema, t.table_name;
    """

    return await connection.fetch(query, *args)


async def get_constraints_to_excluded_tables(connection: Connection, tables: List[Tuple[str, str]]) -> List[str]:
    values_placeholders = ", ".join(f"($%s, $%s)" % (i * 2 + 1, i * 2 + 2) for i in range(len(tables)))
    args = [item for table_data in tables for item in table_data]
    query = f"""
    WITH tables_to_check AS (
        VALUES {values_placeholders}
    )
    select
        n_from.nspname AS "table_schema_from"
        ,c_from.relname AS "table_name_from"
        ,conname AS "constraint_name"
        ,n_to.nspname AS "table_schema_to"
        ,c_to.relname AS "table_name_to"
        ,t_to.column1 IS NULL AS "is_excluded"
    FROM pg_constraint con
    JOIN pg_class c_to ON c_to.oid = con.confrelid
    JOIN pg_namespace n_to ON n_to.oid = c_to.relnamespace
    LEFT JOIN tables_to_check t_to   ON t_to.column1 = n_to.nspname   AND t_to.column2 = c_to.relname
    JOIN pg_class c_from ON c_from.oid = con.conrelid
    JOIN pg_namespace n_from ON n_from.oid = c_from.relnamespace
    LEFT JOIN tables_to_check t_from ON t_from.column1 = n_from.nspname AND t_from.column2 = c_from.relname
    WHERE con.contype IN ('p','f')
      AND n_to.nspname NOT IN ('pg_catalog', 'information_schema') 
    """

    return await connection.fetch(query, *args)


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
        ctx.logger.info("Skipping dump data of table: " + str(table_name_full))
        return None

    # white list has the second priority for pg_dump / pg_restore
    if ctx.white_listed_tables and (table_schema, table_name) not in ctx.white_listed_tables:
        ctx.logger.info("Skipping dump data of table: " + str(table_name_full))
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

    if table_rule and "raw_sql" in table_rule:
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
        # the table is transferred with the specific fields for anonymization or transferred "as is"
        fields_list = await get_fields_list(
            connection_params=ctx.connection_params,
            server_settings=ctx.server_settings,
            table_schema=table_schema,
            table_name=table_name
        )

        fields = []

        for cnt, column_info in enumerate(fields_list):
            column_name = column_info["column_name"]
            udt_name = column_info["udt_name"]
            field_anon_rule = table_rule["fields"].get(column_name) if table_rule else None

            if column_info["is_generated"] == 'ALWAYS':
                continue

            if field_anon_rule:
                if field_anon_rule.find("SQL:") == 0:
                    fields.append(f'({field_anon_rule[4:]}) as "{column_name}"')
                else:
                    fields.append(f'{field_anon_rule}::{udt_name} as "{column_name}"')
            else:
                # field "as is"
                fields.append(f'"{column_name}" as "{column_name}"')

        fields_expr = ',\n'.join(fields)
        query = f"SELECT {fields_expr}\nFROM {table_name_full}"
        if sql_condition := table_rule and table_rule.get('sql_condition'):
            condition = re.sub(r'^\s*where\b\s*', '', sql_condition, flags=re.IGNORECASE)
            query += f"\nWHERE {condition}"

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
