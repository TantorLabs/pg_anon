import hashlib
import re
from collections import defaultdict
from typing import Any

import asyncpg
from asyncpg import Connection, Pool

from pg_anon.common.constants import ANON_UTILS_DB_SCHEMA_NAME, DEFAULT_EXCLUDED_SCHEMAS, SERVER_SETTINGS
from pg_anon.common.db_queries import (
    get_count_query,
    get_database_size_query,
    get_scan_fields_query,
    get_tables_with_fields_query,
)
from pg_anon.common.dto import ConnectionParams, FieldInfo
from pg_anon.common.errors import ErrorCode, PgAnonError
from pg_anon.common.utils import get_dict_rule_for_table
from pg_anon.context import Context
from pg_anon.logger import get_logger

logger = get_logger()


async def create_connection(connection_params: ConnectionParams, server_settings: dict = SERVER_SETTINGS) -> Connection:
    """Create a new asyncpg database connection."""
    return await asyncpg.connect(
        **connection_params.as_dict(),
        server_settings=server_settings,
    )


async def create_pool(
    connection_params: ConnectionParams, server_settings: dict = SERVER_SETTINGS, min_size: int = 10, max_size: int = 10
) -> Pool:
    """Create a new asyncpg connection pool."""
    return await asyncpg.create_pool(
        **connection_params.as_dict(),
        server_settings=server_settings,
        min_size=min_size,
        max_size=max_size,
    )


async def check_db_connection(connection_params: ConnectionParams, server_settings: dict = SERVER_SETTINGS) -> bool:
    """Check whether a database connection can be established."""
    try:
        db_conn = await create_connection(connection_params, server_settings=server_settings)
        await db_conn.close()
    except Exception:
        return False

    return True


async def check_anon_utils_db_schema_exists(
    connection_params: ConnectionParams, server_settings: dict = SERVER_SETTINGS
) -> bool:
    """Check whether the anonymization utils schema exists in the database."""
    query = f"""
    select exists (select schema_name FROM information_schema.schemata where "schema_name" = '{ANON_UTILS_DB_SCHEMA_NAME}');
    """

    db_conn = await create_connection(connection_params, server_settings=server_settings)
    exists = await db_conn.fetchval(query)
    await db_conn.close()
    return exists


async def get_scan_fields_list(
    connection_params: ConnectionParams, server_settings: dict = SERVER_SETTINGS, limit: int | None = None
) -> list:
    """Get the list of fields available for scanning sensitive data."""
    query = get_scan_fields_query(limit=limit)

    db_conn = await create_connection(connection_params, server_settings=server_settings)
    fields_list = await db_conn.fetch(query)
    await db_conn.close()
    return fields_list


async def get_tables_with_fields(
    schema: str,
    connection_params: ConnectionParams,
    server_settings: dict = SERVER_SETTINGS,
    limit: int = 10,
    offset: int = 0,
    table_filter: str | None = None,
) -> list:
    """Get tables with their column definitions for a given schema."""
    query = get_tables_with_fields_query(schema, limit, offset, table_filter=table_filter)

    db_conn = await create_connection(connection_params, server_settings=server_settings)
    try:
        data = await db_conn.fetch(query)
    finally:
        await db_conn.close()

    return data


async def get_scan_fields_count(connection_params: ConnectionParams, server_settings: dict = SERVER_SETTINGS) -> int:
    """Get the count of fields available for scanning sensitive data."""
    query = get_scan_fields_query(count_only=True)

    db_conn = await create_connection(connection_params, server_settings=server_settings)
    count = await db_conn.fetchval(query)
    await db_conn.close()
    return count


async def get_fields_list(
    connection_params: ConnectionParams, table_schema: str, table_name: str, server_settings: dict = SERVER_SETTINGS
) -> list:
    """Get the list of fields for a table dump."""
    db_conn = await create_connection(connection_params, server_settings=server_settings)
    fields_list = await db_conn.fetch(
        f"""
            SELECT column_name, udt_name, is_nullable, is_generated FROM information_schema.columns
            WHERE table_schema = '{table_schema.replace("'", "''")}' AND table_name='{table_name.replace("'", "''")}'
            ORDER BY ordinal_position ASC
        """
    )
    await db_conn.close()
    return fields_list

async def get_all_fields_list(connection_params: ConnectionParams, exclude_schemas: List[str], server_settings: Dict = SERVER_SETTINGS) -> Dict[Tuple[str, str], List]:
    """
    Get fields for all tables in one query.
    """
    db_conn = await create_connection(connection_params, server_settings=server_settings)
    try:
        excluded = list(DEFAULT_EXCLUDED_SCHEMAS) + (exclude_schemas or [])
        placeholders = ', '.join(f"'{s}'" for s in excluded)

        rows = await db_conn.fetch(f"""
            SELECT table_schema, table_name, column_name, udt_name, is_nullable, is_generated
            FROM information_schema.columns
            WHERE table_schema NOT IN ({placeholders})
            ORDER BY table_schema, table_name, ordinal_position ASC
        """)
    finally:
        await db_conn.close()

    result: Dict[Tuple[str, str], List] = {}
    for row in rows:
        key = (row['table_schema'], row['table_name'])
        if key not in result:
            result[key] = []
        result[key].append(row)

    return result


async def get_rows_count(
        connection_params: ConnectionParams, schema_name: str, table_name: str, server_settings: dict = SERVER_SETTINGS
) -> int:
    """Get the row count for a table."""
    query = get_count_query(schema_name=schema_name, table_name=table_name)
    db_conn = await create_connection(connection_params, server_settings=server_settings)
    count = await db_conn.fetchval(query)
    await db_conn.close()
    return count


async def get_db_size(
    connection_params: ConnectionParams, db_name: str, server_settings: dict = SERVER_SETTINGS
) -> int:
    """Get the database size in bytes."""
    query = get_database_size_query(db_name=db_name)
    db_conn = await create_connection(connection_params, server_settings=server_settings)
    result = await db_conn.fetchval(query)
    await db_conn.close()
    return result


async def exec_data_scan_func_query(connection: Connection, scan_func: str, value: Any, field_info: FieldInfo) -> bool:  # noqa: ANN401
    """Execute a row-level scan using a custom database function."""
    query = f"""SELECT {scan_func}($1, $2, $3, $4)"""
    statement = await connection.prepare(query)
    return await statement.fetchval(value, field_info.nspname, field_info.relname, field_info.column_name)


async def exec_data_scan_func_per_field_query(
    connection: Connection, scan_func_per_field: str, field_info: FieldInfo
) -> bool:
    """Execute a field-level scan using a custom database function."""
    query = f"""SELECT {scan_func_per_field}($1, $2, $3, $4)"""
    statement = await connection.prepare(query)
    return await statement.fetchval(field_info.nspname, field_info.relname, field_info.column_name, field_info.type)


async def get_db_tables(
    connection: Connection,
    excluded_schemas: list[str] | None = None,
) -> list[tuple[str, str]]:
    """Get the list of non-partitioned base tables in the database."""
    if not excluded_schemas:
        excluded_schemas = []
    excluded_schemas_str = ", ".join([f"'{v}'" for v in [*excluded_schemas, *DEFAULT_EXCLUDED_SCHEMAS]])

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

    return [(row[0], row[1]) for row in await connection.fetch(query)]


async def get_schemas(connection: Connection, schema_filter: str | None = None) -> list[str]:
    """Get the list of user-defined schemas in the database."""
    schema_filter_clause = f"AND nspname like '%{schema_filter}%'" if schema_filter else ""
    query = f"""
    SELECT nspname AS schema_name
    FROM pg_namespace
    WHERE nspname NOT LIKE 'pg_%' AND nspname != 'information_schema' {schema_filter_clause}
    ORDER BY nspname;
    """

    result = await connection.fetch(query)
    return [row[0] for row in result]


async def get_extensions(connection: Connection) -> list:
    """Get the list of installed extensions in the database."""
    query = """
    SELECT
        n.nspname as "schema"
        , e.extname as "name"
        , e.extversion "version"
        , e.extrelocatable as "relocatable"
    FROM pg_extension e
    JOIN pg_namespace n ON n.oid = e.extnamespace
    ORDER BY extname;
    """

    return await connection.fetch(query)


async def get_available_extensions_map(connection: Connection) -> dict[str, list[dict[str, Any]]]:
    """Get a map of available extensions with their version details."""
    query = """
    SELECT ev.name, ev.version, ev.installed, ev.requires, e.default_version
    FROM pg_available_extension_versions as ev
    LEFT JOIN pg_available_extensions as e on e."name" = ev."name"
    ORDER BY name, installed DESC, version DESC;
    """
    rows = await connection.fetch(query)

    extensions_map = defaultdict(list)

    for row in rows:
        extensions_map[row["name"]].append(
            {
                "version": row["version"],
                "installed": row["installed"],
                "requires": row["requires"],
                "default_version": row["default_version"],
            }
        )

    return dict(extensions_map)


async def get_available_schemas(connection: Connection) -> list[str]:
    """Get all schema names from the database."""
    query = "SELECT nspname FROM pg_namespace"
    result = await connection.fetch(query)
    return [row[0] for row in result]


async def get_custom_functions_ddl(connection: Connection, excluded_schemas: list[str] | None = None) -> list[str]:
    """Get DDL statements for user-defined functions not owned by extensions."""
    if not excluded_schemas:
        excluded_schemas = []
    excluded_schemas_str = ", ".join([f"'{v}'" for v in [*excluded_schemas, *DEFAULT_EXCLUDED_SCHEMAS]])

    query = f"""
    SELECT pg_get_functiondef(p.oid) AS ddl
    FROM pg_proc p
    JOIN pg_namespace n ON n.oid = p.pronamespace
    WHERE n.nspname NOT IN ({excluded_schemas_str})
    AND p.prokind != 'a'
    AND NOT EXISTS (
        SELECT 1
        FROM pg_depend d
        WHERE d.objid = p.oid
          AND d.deptype = 'e'
    );
    """

    result = await connection.fetch(query)
    return [row[0] for row in result]


async def get_custom_domains_ddl(connection: Connection, excluded_schemas: list[str] | None = None) -> list[str]:
    """Get DDL statements for user-defined domains not owned by extensions."""
    if not excluded_schemas:
        excluded_schemas = []
    excluded_schemas_str = ", ".join([f"'{v}'" for v in ["pg_catalog", "information_schema", *excluded_schemas]])

    query = f"""
    SELECT
        'CREATE DOMAIN ' || quote_ident(n.nspname) || '.' || quote_ident(t.typname) ||
        ' AS ' || pg_catalog.format_type(t.typbasetype, t.typtypmod) ||
        COALESCE(' DEFAULT ' || t.typdefault, '') ||
        COALESCE(' ' || pg_get_constraintdef(c.oid), '') || ';' AS ddl
    FROM pg_type t
    JOIN pg_namespace n ON n.oid = t.typnamespace
    LEFT JOIN pg_constraint c ON c.contypid = t.oid
    WHERE n.nspname NOT IN ({excluded_schemas_str})
        AND t.typtype = 'd'
        AND NOT EXISTS (
            SELECT 1
            FROM pg_depend d
            WHERE d.classid = 'pg_type'::regclass
            AND d.objid = t.oid
            AND d.deptype = 'e'
        )
    ORDER BY n.nspname, t.typname;
    """

    result = await connection.fetch(query)
    return [row[0] for row in result]


async def get_custom_types_ddl(connection: Connection, excluded_schemas: list[str] | None = None) -> list[str]:
    """Get DDL statements for user-defined composite and enum types."""
    if not excluded_schemas:
        excluded_schemas = []
    excluded_schemas_str = ", ".join([f"'{v}'" for v in ["pg_catalog", "information_schema", *excluded_schemas]])

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
            WHERE n.nspname NOT IN ({excluded_schemas_str})
            AND t.typtype IN ('c', 'e') -- composite, enum
            -- excluding row-types
            AND (t.typtype != 'c' OR t.typrelid = 0)
            AND NOT EXISTS (
                SELECT 1
                FROM pg_depend d
                WHERE d.objid = t.oid
                  AND d.deptype = 'e'
            )
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


async def get_custom_casts_ddl(connection: Connection, excluded_schemas: list[str] | None = None) -> list[str]:
    """Get DDL statements for user-defined casts not owned by extensions."""
    if not excluded_schemas:
        excluded_schemas = []
    excluded_schemas_str = ", ".join([f"'{v}'" for v in ["information_schema", *excluded_schemas]])

    query = f"""
    SELECT
        'CREATE CAST (' ||
        format_type(c.castsource, NULL) ||
        ' AS ' ||
        format_type(c.casttarget, NULL) ||
        ') ' ||
        CASE
            WHEN c.castmethod = 'i' THEN 'WITH INOUT'
            WHEN c.castfunc = 0 THEN 'WITHOUT FUNCTION'
            ELSE 'WITH FUNCTION ' || c.castfunc::regprocedure
        END ||
        CASE
            WHEN c.castcontext = 'i' THEN ' AS IMPLICIT'
            WHEN c.castcontext = 'a' THEN ' AS ASSIGNMENT'
            ELSE ''
        END || ';' AS ddl
    FROM pg_cast c
    JOIN pg_type ts ON ts.oid = c.castsource
    JOIN pg_type tt ON tt.oid = c.casttarget
    JOIN pg_namespace ns ON ns.oid = ts.typnamespace
    JOIN pg_namespace nt ON nt.oid = tt.typnamespace
    LEFT JOIN pg_proc f ON f.oid = c.castfunc
    LEFT JOIN pg_namespace nf ON nf.oid = f.pronamespace
    WHERE
        -- Exclude built-in casts
        (ns.nspname != 'pg_catalog' or nt.nspname != 'pg_catalog')
        -- Exclude user-defined types by excluded schemas
        and ns.nspname NOT IN ({excluded_schemas_str}) and nt.nspname NOT IN ({excluded_schemas_str})
        -- Exclude user-defined function by excluded schemas
        and (c.castfunc = 0 OR nf.nspname NOT IN ({excluded_schemas_str}))
        -- CAST not owned by extension
        AND NOT EXISTS (
        --NOT EXISTS (
            SELECT 1 FROM pg_depend d
            WHERE d.classid = 'pg_cast'::regclass
              AND d.objid = c.oid
              AND d.deptype = 'e'
        )
        -- source type not owned by extension
        AND NOT EXISTS (
            SELECT 1 FROM pg_depend d
            WHERE d.classid = 'pg_type'::regclass
              AND d.objid = ts.oid
              AND d.deptype = 'e'
        )
        -- target type not owned by extension
        AND NOT EXISTS (
            SELECT 1 FROM pg_depend d
            WHERE d.classid = 'pg_type'::regclass
              AND d.objid = tt.oid
              AND d.deptype = 'e'
        )
        -- function not owned by extension
        AND (
            c.castfunc = 0
            OR NOT EXISTS (
                SELECT 1 FROM pg_depend d
                WHERE d.classid = 'pg_proc'::regclass
                  AND d.objid = c.castfunc
                  AND d.deptype = 'e'
            )
        );
    """

    result = await connection.fetch(query)
    return [row[0] for row in result]


async def get_custom_operators_ddl(connection: Connection, excluded_schemas: list[str] | None = None) -> list[str]:
    """Get DDL statements for user-defined operators not owned by extensions."""
    excluded_schemas_filter = ""
    excluded_schemas_str = ""
    if excluded_schemas:
        excluded_schemas_str = ", ".join([f"'{v}'" for v in excluded_schemas])
        excluded_schemas_filter = f"AND nf.nspname not in ({excluded_schemas_str})"

    query = f"""
    SELECT
        'CREATE OPERATOR ' || o.oid::regoperator || ' (' ||
        'PROCEDURE = ' || o.oprcode::regprocedure ||
        COALESCE(', LEFTARG = ' || format_type(o.oprleft, NULL), '') ||
        COALESCE(', RIGHTARG = ' || format_type(o.oprright, NULL), '') ||
        COALESCE(', COMMUTATOR = ' || o.oprcom::regoperator, '') ||
        COALESCE(', NEGATOR = ' || o.oprnegate::regoperator, '') ||
        COALESCE(', RESTRICT = ' || o.oprrest::regprocedure, '') ||
        COALESCE(', JOIN = ' || o.oprjoin::regprocedure, '') ||
        ');' AS ddl
    FROM pg_operator o
    JOIN pg_namespace n ON n.oid = o.oprnamespace
    JOIN pg_proc f ON f.oid = o.oprcode
    JOIN pg_namespace nf ON nf.oid = f.pronamespace
    WHERE n.nspname NOT IN ('pg_catalog', 'information_schema'{", " + excluded_schemas_str if excluded_schemas_str else ""})
        {excluded_schemas_filter}
        AND NOT EXISTS (
            SELECT 1 FROM pg_depend d
            WHERE d.objid = o.oid AND d.deptype = 'e'
        );
    """

    result = await connection.fetch(query)
    return [row[0] for row in result]


async def get_custom_aggregates_ddl(connection: Connection, excluded_schemas: list[str] | None = None) -> list[str]:
    """Get DDL statements for user-defined aggregates not owned by extensions."""
    excluded_schemas_filter = ""
    excluded_schemas_str = ""
    if excluded_schemas:
        excluded_schemas_str = ", ".join([f"'{v}'" for v in excluded_schemas])
        excluded_schemas_filter = f"AND ns.nspname not in ({excluded_schemas_str})"

    query = f"""
    SELECT
        'CREATE AGGREGATE ' || p.oid::regprocedure || ' (' ||
        'SFUNC = ' || a.aggtransfn::regprocedure ||
        ', STYPE = ' || format_type(a.aggtranstype, NULL) ||
        COALESCE(', FINALFUNC = ' || a.aggfinalfn::regprocedure, '') ||
        COALESCE(', INITCOND = ' || quote_literal(a.agginitval), '') ||
        ');' AS ddl
    FROM pg_aggregate a
    JOIN pg_proc p ON p.oid = a.aggfnoid
    JOIN pg_proc sf ON sf.oid = a.aggtransfn
    JOIN pg_namespace n ON n.oid = p.pronamespace
    JOIN pg_namespace ns ON ns.oid = sf.pronamespace
    WHERE n.nspname NOT IN ('pg_catalog', 'information_schema'{", " + excluded_schemas_str if excluded_schemas_str else ""})
        {excluded_schemas_filter}
        AND NOT EXISTS (
            SELECT 1 FROM pg_depend d
            WHERE d.objid = p.oid AND d.deptype = 'e'
        );
    """

    result = await connection.fetch(query)
    return [row[0] for row in result]


async def get_indexes_data(connection: Connection, tables: list[tuple[str, str]]) -> list:
    """Get index metadata for the given tables."""
    values_placeholders = ", ".join(f"(${i * 2 + 1}, ${i * 2 + 2})" for i in range(len(tables)))
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


async def get_views_related_to_tables(connection: Connection, tables: list[tuple[str, str]]) -> list:
    """Get views and materialized views that reference the given tables."""
    values_placeholders = ", ".join(f"(${i * 2 + 1}, ${i * 2 + 2})" for i in range(len(tables)))
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


async def get_constraints_to_excluded_tables(connection: Connection, tables: list[tuple[str, str]]) -> list:
    """Get foreign key constraints referencing the given tables."""
    values_placeholders = ", ".join(f"(${i * 2 + 1}, ${i * 2 + 2})" for i in range(len(tables)))
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
    """Check whether the database has no user-defined base tables."""
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


async def run_query_in_pool(pool: Pool, query: str) -> None:
    """Execute a SQL query using a connection from the pool."""
    logger.info("================> Started query %s", query)

    try:
        async with pool.acquire() as connection:
            await connection.execute(query)
            logger.info("Execute query: %s", query)
    except Exception as exc:
        logger.exception("Exception in run_query_in_pool")
        raise PgAnonError(ErrorCode.DB_QUERY_FAILED, f"Can't execute query: {query}") from exc

    logger.info("<================ Finished query %s", query)


async def get_pg_version(connection_params: ConnectionParams, server_settings: dict = SERVER_SETTINGS) -> str:
    """Get the PostgreSQL server version as a string."""
    db_conn = await create_connection(connection_params, server_settings=server_settings)
    pg_version = await db_conn.fetchval("select version()")
    await db_conn.close()
    return re.findall(r"(\d+\.\d+)", str(pg_version))[0]


async def get_available_connections(connection: Connection) -> int:
    """Get the number of available database connections."""
    query = """
    WITH max_conn AS (
        SELECT setting::int AS max_connections
        FROM pg_settings
        WHERE name = 'max_connections'
    ),
    superuser_reserved_conn AS (
        SELECT setting::int AS superuser_reserved_connections
        FROM pg_settings
        WHERE name = 'superuser_reserved_connections'
    ),
    used_conn AS (
        SELECT COUNT(*) AS used_connections
        FROM pg_stat_activity
        WHERE pid <> pg_backend_pid()
          AND datname IS NOT NULL
    )
    SELECT
        max_conn.max_connections - superuser_reserved_conn.superuser_reserved_connections - used_conn.used_connections AS available_connections
    FROM
        max_conn,
        superuser_reserved_conn,
        used_conn;
    """
    result = await connection.fetchrow(query)
    return result[0]


async def check_required_connections(
    connection: Connection,
    required_connections: int,
) -> None:
    """Verify that enough database connections are available."""
    available_connections = await get_available_connections(connection)

    if required_connections > available_connections:
        raise PgAnonError(
            ErrorCode.INSUFFICIENT_CONNECTIONS,
            f"Not enough database connections. Required: {required_connections}, available: ~{available_connections}",
        )


async def get_dump_query(  # noqa: C901, PLR0912
    ctx: Context,
    table_schema: str,
    table_name: str,
    table_rule: dict | None = None,
    nulls_last: bool = False,
    files: dict | None = None,
    fields_cache: dict | None = None,
) -> str | None:
    """Build the SELECT query used to dump a table with optional anonymization rules."""
    table_name_full = f'"{table_schema}"."{table_name}"'

    # black list has the highest priority for pg_dump / pg_restore
    if ctx.black_listed_tables and (table_schema, table_name) in ctx.black_listed_tables:
        ctx.logger.info("Skipping dump data of table: %s", table_name_full)
        return None

    # white list has the second priority for pg_dump / pg_restore
    if ctx.white_listed_tables and (table_schema, table_name) not in ctx.white_listed_tables:
        ctx.logger.info("Skipping dump data of table: %s", table_name_full)
        return None

    # dictionary_exclude has third priority
    if "dictionary_exclude" in ctx.prepared_dictionary_obj:
        exclude_rule = get_dict_rule_for_table(
            dictionary_rules=ctx.prepared_dictionary_obj["dictionary_exclude"],
            schema=table_schema,
            table=table_name,
        )

        if exclude_rule is not None and table_rule is None:
            ctx.logger.info("Skipping: %s", table_name_full)
            return None

    hashed_name = hashlib.md5(  # noqa: S324
        (table_schema + "_" + table_name).encode()
    ).hexdigest()

    if files is not None:
        files[f"{hashed_name}.bin.gz"] = {"schema": table_schema, "table": table_name}

    if table_rule and "raw_sql" in table_rule:
        # the table is transferred using "raw_sql"
        if (
            ctx.options.dbg_stage_1_validate_dict
            or ctx.options.dbg_stage_2_validate_data
            or ctx.options.dbg_stage_3_validate_full
        ):
            query = table_rule["raw_sql"] + " " + ctx.validate_limit
            ctx.logger.info(str(query))
            return query
        return table_rule["raw_sql"]
    # the table is transferred with the specific fields for anonymization or transferred "as is"
    if fields_cache is not None:
        fields_list = fields_cache.get((table_schema, table_name), [])
    else:
        fields_list = await get_fields_list(
            connection_params=ctx.connection_params,
            server_settings=ctx.server_settings,
            table_schema=table_schema,
            table_name=table_name,
        )

    fields = []

    for column_info in fields_list:
        column_name = column_info["column_name"]
        udt_name = column_info["udt_name"]
        field_anon_rule = table_rule["fields"].get(column_name) if table_rule else None

        if column_info["is_generated"] == "ALWAYS":
            continue

        if field_anon_rule:
            if field_anon_rule.find("SQL:") == 0:
                fields.append(f'({field_anon_rule[4:]}) as "{column_name}"')
            else:
                fields.append(f'{field_anon_rule}::{udt_name} as "{column_name}"')
        else:
            # field "as is"
            fields.append(f'"{column_name}" as "{column_name}"')

    fields_expr = ",\n".join(fields)
    query = f"SELECT {fields_expr}\nFROM {table_name_full}"
    if sql_condition := table_rule and table_rule.get("sql_condition"):
        condition = re.sub(r"^\s*where\b\s*", "", sql_condition, flags=re.IGNORECASE)
        query += f"\nWHERE {condition}"

    if (
        ctx.options.dbg_stage_1_validate_dict
        or ctx.options.dbg_stage_2_validate_data
        or ctx.options.dbg_stage_3_validate_full
    ):
        query += f" {ctx.validate_limit}"

    if nulls_last:
        ordering = ", ".join(
            [
                f'"{field["column_name"]}"' + " NULLS LAST"
                for field in fields_list
                if field["is_nullable"].lower() == "yes"
            ]
        )
        if ordering:
            query += f" ORDER BY {ordering}"

    return query
