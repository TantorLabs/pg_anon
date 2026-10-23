import hashlib
import re
from collections.abc import Iterable
from typing import Any

import asyncpg
from asyncpg import Connection, Pool

from pg_anon.common.constants import ANON_UTILS_DB_SCHEMA_NAME, SERVER_SETTINGS
from pg_anon.common.db_queries import (
    get_count_query,
    get_database_size_query,
    get_foreign_servers_count_query,
    get_scan_fields_query,
    get_tables_with_fields_query,
    get_user_routines_and_triggers_count_query,
    get_visible_user_mappings_query,
    schemas_filter,
)
from pg_anon.common.dependencies import DependencyGraph, PG_NAMESPACE, TABLE_KINDS
from pg_anon.common.dto import ConnectionParams, FieldInfo
from pg_anon.common.errors import ErrorCode, PgAnonError
from pg_anon.common.utils import get_dict_rule_for_table
from pg_anon.context import Context
from pg_anon.logger import get_logger

logger = get_logger()


def _tables_as_arrays(tables: list[tuple[str, str]]) -> list[list[str]]:
    return [[schema for schema, _ in tables], [table for _, table in tables]]


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


async def require_anon_utils_db_schema(
    connection_params: ConnectionParams, server_settings: dict = SERVER_SETTINGS
) -> None:
    """Raise a clear error when the anonymization utils schema is missing."""
    if not await check_anon_utils_db_schema_exists(connection_params, server_settings=server_settings):
        raise PgAnonError(
            ErrorCode.SCHEMA_NOT_INITIALIZED,
            f"Schema '{ANON_UTILS_DB_SCHEMA_NAME}' does not exist. Please run init mode first.",
        )


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


async def get_fields_list(
    connection_params: ConnectionParams, table_schema: str, table_name: str, server_settings: dict = SERVER_SETTINGS
) -> list:
    """Get the list of fields for a table dump."""
    db_conn = await create_connection(connection_params, server_settings=server_settings)
    fields_list = await db_conn.fetch(
        f"""
            SELECT column_name, udt_name, udt_schema, is_nullable, is_generated FROM information_schema.columns
            WHERE table_schema = '{table_schema.replace("'", "''")}' AND table_name='{table_name.replace("'", "''")}'
            ORDER BY ordinal_position ASC
        """
    )
    await db_conn.close()
    return fields_list


async def get_all_fields_list(
    connection_params: ConnectionParams, exclude_schemas: list[str], server_settings: dict = SERVER_SETTINGS
) -> dict[tuple[str, str], list]:
    """Get fields for all tables in one query."""
    db_conn = await create_connection(connection_params, server_settings=server_settings)
    try:
        rows = await db_conn.fetch(f"""
            SELECT table_schema, table_name, column_name, udt_name, udt_schema, is_nullable, is_generated
            FROM information_schema.columns
            WHERE {schemas_filter("table_schema", exclude_schemas)}
            ORDER BY table_schema, table_name, ordinal_position ASC
        """)
    finally:
        await db_conn.close()

    result: dict[tuple[str, str], list] = {}
    for row in rows:
        key = (row["table_schema"], row["table_name"])
        if key not in result:
            result[key] = []
        result[key].append(row)

    return result


async def get_event_triggers(connection: Connection) -> dict[str, str]:
    """Return EVENT TRIGGER names with the schema of their function."""
    rows = await connection.fetch(
        """
        SELECT et.evtname, n.nspname
        FROM pg_event_trigger et
        JOIN pg_proc p ON p.oid = et.evtfoid
        JOIN pg_namespace n ON n.oid = p.pronamespace
        """
    )
    return {row["evtname"]: row["nspname"] for row in rows}


async def get_legacy_inheritance_parents(
    connection_params: ConnectionParams,
    exclude_schemas: list[str] | None = None,
    server_settings: dict = SERVER_SETTINGS,
) -> set[tuple[str, str]]:
    """Return (schema, table) pairs that are legacy-INHERITS parents (not declarative partitions)."""
    db_conn = await create_connection(connection_params, server_settings=server_settings)
    try:
        rows = await db_conn.fetch(f"""
            SELECT DISTINCT
                np.nspname AS parent_schema,
                p.relname AS parent_table
            FROM pg_inherits inh
            JOIN pg_class p ON p.oid = inh.inhparent
            JOIN pg_namespace np ON np.oid = p.relnamespace
            WHERE p.relkind = 'r'
              AND {schemas_filter("np.nspname", exclude_schemas)}
        """)
    finally:
        await db_conn.close()

    return {(row["parent_schema"], row["parent_table"]) for row in rows}


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


async def get_partition_ancestors_map(
    connection: Connection,
    tables: list[tuple[str, str]],
) -> dict[tuple[str, str], list[tuple[str, str]]]:
    """Map each input table to its declarative-partitioning ancestors, closest first; INHERITS parents are skipped."""
    if not tables:
        return {}

    args = _tables_as_arrays(tables)

    rows = await connection.fetch(
        """
        WITH RECURSIVE
        input_oids AS (
            SELECT c.oid
              FROM unnest($1::text[], $2::text[]) AS v(s, t)
              JOIN pg_namespace n ON n.nspname = v.s
              JOIN pg_class c ON c.relname = v.t AND c.relnamespace = n.oid
        ),
        walk(start_oid, parent_oid, lvl) AS (
            SELECT inh.inhrelid, inh.inhparent, 1
              FROM pg_inherits inh
             WHERE inh.inhrelid IN (SELECT oid FROM input_oids)
            UNION ALL
            SELECT w.start_oid, inh.inhparent, w.lvl + 1
              FROM pg_inherits inh
              JOIN walk w ON w.parent_oid = inh.inhrelid
        )
        SELECT sn.nspname AS start_schema,
               sc.relname AS start_table,
               pn.nspname AS ancestor_schema,
               pc.relname AS ancestor_table
          FROM walk w
          JOIN pg_class sc ON sc.oid = w.start_oid
          JOIN pg_namespace sn ON sn.oid = sc.relnamespace
          JOIN pg_class pc ON pc.oid = w.parent_oid
          JOIN pg_namespace pn ON pn.oid = pc.relnamespace
         WHERE pc.relkind = 'p'
         ORDER BY w.start_oid, w.lvl
        """,
        *args,
    )

    result: dict[tuple[str, str], list[tuple[str, str]]] = {}
    for r in rows:
        key = (r["start_schema"], r["start_table"])
        result.setdefault(key, []).append((r["ancestor_schema"], r["ancestor_table"]))
    return result


async def get_table_ancestors_map(connection: Connection) -> dict[tuple[str, str], list[tuple[str, str]]]:
    """Map each partition and each INHERITS child to all the tables above it, closest first."""
    rows = await connection.fetch(
        """
        SELECT cn.nspname AS child_schema,
               c.relname  AS child_table,
               pn.nspname AS parent_schema,
               p.relname  AS parent_table
          FROM pg_inherits inh
          JOIN pg_class c ON c.oid = inh.inhrelid
          JOIN pg_namespace cn ON cn.oid = c.relnamespace
          JOIN pg_class p ON p.oid = inh.inhparent
          JOIN pg_namespace pn ON pn.oid = p.relnamespace
         WHERE c.relkind IN ('r', 'p', 'f')
         ORDER BY inh.inhrelid, inh.inhseqno
        """
    )

    parents: dict[tuple[str, str], list[tuple[str, str]]] = {}
    for r in rows:
        parents.setdefault((r["child_schema"], r["child_table"]), []).append((r["parent_schema"], r["parent_table"]))

    result: dict[tuple[str, str], list[tuple[str, str]]] = {}
    for table in parents:
        ancestors: list[tuple[str, str]] = []
        level = parents[table]
        while level:
            ancestors.extend(parent for parent in dict.fromkeys(level) if parent not in ancestors)
            level = [grandparent for parent in level for grandparent in parents.get(parent, [])]
        result[table] = ancestors
    return result


async def get_foreign_descendants_map(connection: Connection) -> dict[tuple[str, str], set[tuple[str, str]]]:
    """Map each table that has foreign tables below it, at any depth, to these foreign tables."""
    # the walk starts from the foreign tables, so its cost does not depend on the size of the database
    rows = await connection.fetch(
        """
        WITH RECURSIVE walk(foreign_oid, parent_oid) AS (
            SELECT c.oid, inh.inhparent
              FROM pg_class c
              JOIN pg_inherits inh ON inh.inhrelid = c.oid
             WHERE c.relkind = 'f'
            UNION
            SELECT w.foreign_oid, inh.inhparent
              FROM walk w
              JOIN pg_inherits inh ON inh.inhrelid = w.parent_oid
        )
        SELECT pn.nspname AS parent_schema,
               p.relname  AS parent_table,
               fn.nspname AS foreign_schema,
               f.relname  AS foreign_table
          FROM walk w
          JOIN pg_class p ON p.oid = w.parent_oid
          JOIN pg_namespace pn ON pn.oid = p.relnamespace
          JOIN pg_class f ON f.oid = w.foreign_oid
          JOIN pg_namespace fn ON fn.oid = f.relnamespace
        """
    )

    result: dict[tuple[str, str], set[tuple[str, str]]] = {}
    for r in rows:
        result.setdefault((r["parent_schema"], r["parent_table"]), set()).add((r["foreign_schema"], r["foreign_table"]))
    return result


async def get_db_tables(
    connection: Connection,
    excluded_schemas: list[str] | None = None,
) -> list[tuple[str, str]]:
    """Get the list of non-partitioned base tables in the database."""
    if not excluded_schemas:
        excluded_schemas = []
    query = f"""
            SELECT t.table_schema, t.table_name
            FROM information_schema.tables t
            JOIN pg_class c ON c.relname = t.table_name
            JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = t.table_schema
            WHERE
                {schemas_filter("t.table_schema", excluded_schemas)}
                AND t.table_type = 'BASE TABLE'
                -- relkind 'p' marks partitioned parents; it simply never matches before PG 10
                AND c.relkind <> 'p';
        """

    return [(row[0], row[1]) for row in await connection.fetch(query)]


async def get_all_tables(connection: Connection) -> set[tuple[str, str]]:
    """Return the tables that the dependency graph has: all kinds, all user schemas, except the tables of extensions."""
    kinds = ", ".join(f"'{kind}'" for kind in TABLE_KINDS)
    rows = await connection.fetch(
        f"""
        SELECT n.nspname, c.relname
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind IN ({kinds})
          AND {schemas_filter("n.nspname")}
          AND NOT EXISTS (
              SELECT 1 FROM pg_depend d
              WHERE d.classid = 'pg_class'::regclass AND d.objid = c.oid AND d.deptype = 'e'
          )
        """  # noqa: S608
    )
    return {(row[0], row[1]) for row in rows}


async def get_schemas(connection: Connection, schema_filter: str | None = None) -> list[str]:
    """Get the list of user-defined schemas in the database."""
    schema_filter_clause = f"AND nspname like '%{schema_filter}%'" if schema_filter else ""
    query = f"""
    SELECT nspname AS schema_name
    FROM pg_namespace
    WHERE {schemas_filter("nspname")} {schema_filter_clause}
    ORDER BY nspname;
    """

    result = await connection.fetch(query)
    return [row[0] for row in result]


async def get_extension_schemas(connection: Connection) -> set[str]:
    """Get the schemas that belong to extensions."""
    rows = await connection.fetch(
        """
        SELECT n.nspname
        FROM pg_namespace n
        JOIN pg_depend d ON d.classid = 'pg_namespace'::regclass AND d.objid = n.oid AND d.deptype = 'e'
        """
    )
    return {row[0] for row in rows}


async def get_dependency_graph(connection: Connection) -> DependencyGraph:
    """Read the links between user objects from pg_depend and the objects that hold other objects."""
    graph = DependencyGraph()

    # 16384 is the first OID of a user object; schemas are kept because objects of "public" refer to it
    # 'I' exists only in PostgreSQL 11: it links the index of a partition to the columns of the partition
    edges = await connection.fetch(
        """
        SELECT DISTINCT d.classid, d.objid, d.refclassid, d.refobjid, d.deptype::text AS deptype
        FROM pg_depend d
        WHERE d.deptype IN ('n', 'a', 'i', 'I')
          AND d.objid >= 16384
          AND (d.refobjid >= 16384 OR d.refclassid = 'pg_namespace'::regclass)
        """
    )
    for row in edges:
        node, referenced = (row["classid"], row["objid"]), (row["refclassid"], row["refobjid"])
        if node == referenced:
            continue
        graph.dependents.setdefault(referenced, set()).add(node)
        if row["deptype"] == "i" and referenced[0] != PG_NAMESPACE:
            graph.direct_owners[node] = referenced
        elif row["deptype"] == "a" and referenced[0] != PG_NAMESPACE:
            graph.auto_owners.setdefault(node, set()).add(referenced)

    # pg_dump writes these objects inside the CREATE command of another object
    owners = await connection.fetch(
        """
        SELECT 'pg_attrdef'::regclass::oid AS classid, oid AS objid,
               'pg_class'::regclass::oid AS owner_classid, adrelid AS owner_objid
        FROM pg_attrdef WHERE oid >= 16384
        UNION ALL
        SELECT 'pg_constraint'::regclass::oid, oid,
               CASE WHEN contypid <> 0 THEN 'pg_type'::regclass::oid ELSE 'pg_class'::regclass::oid END,
               CASE WHEN contypid <> 0 THEN contypid ELSE conrelid END
        FROM pg_constraint WHERE oid >= 16384 AND (contypid <> 0 OR contype IN ('c', 'n'))
        UNION ALL
        SELECT 'pg_amop'::regclass::oid, oid, 'pg_opfamily'::regclass::oid, amopfamily
        FROM pg_amop WHERE oid >= 16384
        UNION ALL
        SELECT 'pg_amproc'::regclass::oid, oid, 'pg_opfamily'::regclass::oid, amprocfamily
        FROM pg_amproc WHERE oid >= 16384
        """
    )
    for row in owners:
        graph.direct_owners.setdefault((row["classid"], row["objid"]), (row["owner_classid"], row["owner_objid"]))

    relations = await connection.fetch(
        f"""
        SELECT c.oid, c.relkind::text AS relkind, n.nspname, c.relname
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE {schemas_filter("n.nspname")}
        """  # noqa: S608
    )
    graph.relations = {row["oid"]: (row["relkind"], row["nspname"], row["relname"]) for row in relations}

    members = await connection.fetch("SELECT classid, objid FROM pg_depend WHERE deptype = 'e'")
    graph.extension_members = {(row["classid"], row["objid"]) for row in members}

    graph.schemas = {
        row["nspname"]: row["oid"] for row in await connection.fetch("SELECT oid, nspname FROM pg_namespace")
    }

    return graph


async def describe_db_objects(connection: Connection, nodes: Iterable[tuple[int, int]]) -> list[str]:
    """Return readable names of objects, like "function app.audit_rows()"."""
    nodes = list(nodes)
    # pg_describe_object is translated to the language of the server, pg_identify_object is not
    rows = await connection.fetch(
        """
        SELECT i.type || ' ' || i.identity
        FROM unnest($1::oid[], $2::oid[]) AS t(c, o), pg_identify_object(t.c, t.o, 0) AS i
        """,
        [node[0] for node in nodes],
        [node[1] for node in nodes],
    )
    return [row[0] for row in rows]


async def get_extensions(connection: Connection) -> list:
    """Get installed extensions with their dependencies from pg_depend."""
    query = """
    SELECT
        n.nspname as "schema"
        , e.extname as "name"
        , e.extversion "version"
        , e.extrelocatable as "relocatable"
        , COALESCE(
            array_agg(r.extname ORDER BY r.extname) FILTER (WHERE r.extname IS NOT NULL)
            , '{}'::name[]
          ) as "requires"
    FROM pg_extension e
    JOIN pg_namespace n ON n.oid = e.extnamespace
    LEFT JOIN pg_depend d
        ON d.classid = 'pg_extension'::regclass
        AND d.objid = e.oid
        AND d.refclassid = 'pg_extension'::regclass
    LEFT JOIN pg_extension r ON r.oid = d.refobjid
    GROUP BY n.nspname, e.extname, e.extversion, e.extrelocatable
    ORDER BY e.extname;
    """

    return await connection.fetch(query)


async def get_visible_user_mappings(connection: Connection) -> list:
    """FDW user mappings whose OPTIONS are visible to the current role (credentials leak)."""
    return await connection.fetch(get_visible_user_mappings_query())


async def get_foreign_servers_count(connection: Connection) -> int:
    """Number of foreign servers defined in the database (FDW presence check)."""
    return await connection.fetchval(get_foreign_servers_count_query())


async def get_user_routines_and_triggers_count(
    connection: Connection, excluded_schemas: list[str] | None = None
) -> int:
    """Number of user-defined functions/procedures and non-internal triggers."""
    return await connection.fetchval(get_user_routines_and_triggers_count_query(excluded_schemas))


async def get_extension_tables(connection: Connection) -> dict[tuple[str, str], str | None]:
    """Get tables owned by extensions: config tables have a dump condition, the others have None."""
    query = """
    SELECT
        n.nspname AS schema_name
        , c.relname AS table_name
        , cfg.condition
    FROM pg_depend d
    JOIN pg_class c ON c.oid = d.objid AND d.classid = 'pg_class'::regclass
    JOIN pg_namespace n ON n.oid = c.relnamespace
    LEFT JOIN LATERAL (
        SELECT cond AS condition
        FROM pg_extension e, unnest(e.extconfig, e.extcondition) AS u(objid, cond)
        WHERE u.objid = c.oid
    ) cfg ON true
    WHERE
        d.refclassid = 'pg_extension'::regclass
        AND d.deptype = 'e'
        AND c.relkind IN ('r', 'p');
    """

    return {(row["schema_name"], row["table_name"]): row["condition"] for row in await connection.fetch(query)}


async def get_available_extensions_map(connection: Connection) -> dict[str, dict[str, Any]]:
    """Get extensions available on the server: default version and requires of every version."""
    query = """
    SELECT ev.name, ev.version, ev.requires, e.default_version
    FROM pg_available_extension_versions as ev
    LEFT JOIN pg_available_extensions as e on e."name" = ev."name";
    """

    extensions_map: dict[str, dict[str, Any]] = {}

    for row in await connection.fetch(query):
        extension = extensions_map.setdefault(row["name"], {"default_version": row["default_version"], "versions": {}})
        extension["versions"][row["version"]] = list(row["requires"] or [])

    return extensions_map


async def get_available_schemas(connection: Connection) -> list[str]:
    """Get all schema names from the database."""
    query = "SELECT nspname FROM pg_namespace"
    result = await connection.fetch(query)
    return [row[0] for row in result]


async def get_indexes_data(connection: Connection, tables: list[tuple[str, str]]) -> list:
    """Get index metadata for the given tables."""
    args = _tables_as_arrays(tables)
    query = f"""
    WITH RECURSIVE tables_to_check(schema_name, table_name) AS (
        SELECT * FROM unnest($1::text[], $2::text[]) AS v(schema_name, table_name)
    ),
    expanded_tables(schema_name, table_name) AS (
        SELECT
            (schema_name::text COLLATE "C") AS schema_name,
            (table_name::text COLLATE "C") AS table_name
        FROM tables_to_check
        UNION
        SELECT an.nspname::text, a.relname::text
        FROM expanded_tables et
        JOIN pg_namespace n ON n.nspname = et.schema_name
        JOIN pg_class c ON c.relname = et.table_name AND c.relnamespace = n.oid
        JOIN pg_inherits inh ON inh.inhrelid = c.oid
        JOIN pg_class a ON a.oid = inh.inhparent
        JOIN pg_namespace an ON an.oid = a.relnamespace
    )
    SELECT
        n.nspname as "schema"
        ,t.relname as "table"
        ,i.relname AS "index_name"
        ,tt.schema_name IS null as "is_excluded"
        ,parent_n.nspname AS "parent_index_schema"
        ,parent_i.relname AS "parent_index_name"
    FROM pg_index ix
    JOIN pg_class i ON i.oid = ix.indexrelid
    JOIN pg_class t ON t.oid = ix.indrelid
    JOIN pg_namespace n ON n.oid = t.relnamespace
    LEFT JOIN expanded_tables tt ON tt.schema_name = n.nspname AND tt.table_name = t.relname
    LEFT JOIN pg_inherits idx_inh ON idx_inh.inhrelid = i.oid
    LEFT JOIN pg_class parent_i ON parent_i.oid = idx_inh.inhparent AND parent_i.relkind = 'I'
    LEFT JOIN pg_namespace parent_n ON parent_n.oid = parent_i.relnamespace
    WHERE {schemas_filter("n.nspname")};
    """

    return await connection.fetch(query, *args)


async def get_constraints_to_excluded_tables(connection: Connection, tables: list[tuple[str, str]]) -> list[dict]:
    """Get foreign key and primary key constraints with partition-aware metadata."""
    table_set = {(s, t) for s, t in tables}

    rows = await connection.fetch(
        f"""
        SELECT
            n_from.nspname AS table_schema_from,
            c_from.relname AS table_name_from,
            con.conname    AS constraint_name,
            n_to.nspname   AS table_schema_to,
            c_to.relname   AS table_name_to,
            c_to.relkind::text   AS referenced_relkind,
            c_from.relkind::text AS referrer_relkind,
            con.contype::text    AS contype,
            pg_get_constraintdef(con.oid) AS definition,
            c_to.oid   AS confrelid,
            c_from.oid AS conrelid
        FROM pg_constraint con
        JOIN pg_class c_to ON c_to.oid = con.confrelid
        JOIN pg_namespace n_to ON n_to.oid = c_to.relnamespace
        JOIN pg_class c_from ON c_from.oid = con.conrelid
        JOIN pg_namespace n_from ON n_from.oid = c_from.relnamespace
        WHERE con.contype IN ('p', 'f')
          AND {schemas_filter("n_to.nspname")}
          -- a key of a partitioned table has copies for partitions; conparentid is read as JSON, PostgreSQL 10 has no such column
          AND (con.contype = 'p' OR coalesce((to_jsonb(con) ->> 'conparentid')::bigint, 0) = 0)
        """
    )

    if not rows:
        return []

    partitioned_oids: set[int] = set()
    for r in rows:
        if r["referenced_relkind"] == "p":
            partitioned_oids.add(r["confrelid"])
        if r["referrer_relkind"] == "p":
            partitioned_oids.add(r["conrelid"])

    leaves_by_oid: dict[int, list[tuple[str, str]]] = {}
    if partitioned_oids:
        leaf_rows = await connection.fetch(
            """
            WITH RECURSIVE descendants(root_oid, desc_oid, desc_relkind) AS (
                SELECT c.oid, c.oid, c.relkind
                  FROM pg_class c
                 WHERE c.oid = ANY($1::oid[])
                UNION ALL
                SELECT d.root_oid, ch.oid, ch.relkind
                  FROM descendants d
                  JOIN pg_inherits inh ON inh.inhparent = d.desc_oid
                  JOIN pg_class ch ON ch.oid = inh.inhrelid
                 WHERE d.desc_relkind = 'p'
            )
            SELECT
                d.root_oid,
                n.nspname AS leaf_schema,
                c.relname AS leaf_name
            FROM descendants d
            JOIN pg_class c ON c.oid = d.desc_oid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE d.desc_relkind <> 'p'
            """,
            list(partitioned_oids),
        )
        for r in leaf_rows:
            leaves_by_oid.setdefault(r["root_oid"], []).append((r["leaf_schema"], r["leaf_name"]))

    result: list[dict] = []
    for r in rows:
        is_pk = r["contype"] == "p"
        ref_table = (r["table_schema_to"], r["table_name_to"])
        from_table = (r["table_schema_from"], r["table_name_from"])

        ref_leaves = leaves_by_oid.get(r["confrelid"], []) if r["referenced_relkind"] == "p" else [ref_table]
        from_leaves = leaves_by_oid.get(r["conrelid"], []) if r["referrer_relkind"] == "p" else [from_table]

        if is_pk:
            is_excluded = ref_table not in table_set
        else:
            is_excluded = any(leaf not in table_set for leaf in (ref_leaves + from_leaves))

        result.append(
            {
                "table_schema_from": r["table_schema_from"],
                "table_name_from": r["table_name_from"],
                "constraint_name": r["constraint_name"],
                "table_schema_to": r["table_schema_to"],
                "table_name_to": r["table_name_to"],
                "is_excluded": is_excluded,
                "contype": r["contype"],
                # only foreign keys are recreated from metadata
                "definition": None if is_pk else r["definition"],
                "referenced_relkind": r["referenced_relkind"],
                "referrer_relkind": r["referrer_relkind"],
                "referenced_partition_leaves": [[s, t] for s, t in ref_leaves]
                if r["referenced_relkind"] == "p"
                else None,
                "referrer_partition_leaves": [[s, t] for s, t in from_leaves] if r["referrer_relkind"] == "p" else None,
            }
        )

    return result


async def check_db_is_empty(connection: Connection) -> bool:
    """Check whether the database has no user-defined base tables."""
    return await connection.fetchval(
        f"""
            SELECT NOT EXISTS(
                SELECT table_schema, table_name
                FROM information_schema.tables
                WHERE {schemas_filter("table_schema", [ANON_UTILS_DB_SCHEMA_NAME])}
                    AND table_type = 'BASE TABLE'
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


async def count_foreign_keys_of_tables(connection: Connection, tables: list[tuple[str, str]]) -> int:
    """Count the foreign keys that start from the given tables or refer to them, also through partitioned parents."""
    # a key of a partitioned table has copies for partitions; conparentid is read as JSON, PostgreSQL 10 has no such column
    query = """
    WITH RECURSIVE touched(oid) AS (
        SELECT c.oid
        FROM unnest($1::text[], $2::text[]) AS t(schema_name, table_name)
        JOIN pg_namespace n ON n.nspname = t.schema_name
        JOIN pg_class c ON c.relnamespace = n.oid AND c.relname = t.table_name
        UNION
        SELECT p.oid
        FROM touched
        JOIN pg_inherits inh ON inh.inhrelid = touched.oid
        JOIN pg_class p ON p.oid = inh.inhparent AND p.relkind = 'p'
    )
    SELECT count(*)
    FROM pg_constraint con
    WHERE con.contype = 'f'
      AND coalesce((to_jsonb(con) ->> 'conparentid')::bigint, 0) = 0
      AND (con.conrelid IN (SELECT oid FROM touched) OR con.confrelid IN (SELECT oid FROM touched))
    """
    return await connection.fetchval(query, [table[0] for table in tables], [table[1] for table in tables])


async def get_invalid_partitioned_indexes(connection: Connection) -> list[str]:
    """Get partitioned indexes left invalid, which the planner ignores and foreign keys cannot reference."""
    query = """
    SELECT n.nspname || '.' || c.relname AS index_name
    FROM pg_index i
    JOIN pg_class c ON c.oid = i.indexrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE NOT i.indisvalid
      -- relkind 'I' marks partitioned indexes; it simply never matches before PG 11
      AND c.relkind = 'I'
    ORDER BY 1;
    """
    return [row[0] for row in await connection.fetch(query)]


async def get_available_connections(connection: Connection) -> int:
    """Get the number of available database connections."""
    query = """
    WITH max_conn AS (
        SELECT setting::int AS max_connections
        FROM pg_settings
        WHERE name = 'max_connections'
    ),
    reserved_conn AS (
        SELECT
            current_setting('superuser_reserved_connections')::int
            + COALESCE(current_setting('reserved_connections', true)::int, 0) AS reserved_connections
    ),
    used_conn AS (
        SELECT COUNT(*) AS used_connections
        FROM pg_stat_activity
        WHERE datname IS NOT NULL
    )
    SELECT
        max_conn.max_connections - reserved_conn.reserved_connections - used_conn.used_connections AS available_connections
    FROM
        max_conn,
        reserved_conn,
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


def _strip_leading_where(condition: str) -> str:
    return re.sub(r"^\s*where\b\s*", "", condition, flags=re.IGNORECASE)


async def get_dump_query(  # noqa: C901, PLR0912
    ctx: Context,
    table_schema: str,
    table_name: str,
    table_rule: dict | None = None,
    nulls_last: bool = False,
    files: dict | None = None,
    fields_cache: dict | None = None,
    legacy_inherits_parents: set[tuple[str, str]] | None = None,
) -> str | None:
    """Build the SELECT query used to dump a table with optional masking rules."""
    table_name_full = f'"{table_schema}"."{table_name}"'
    from_clause_target = (
        f"ONLY {table_name_full}"
        if legacy_inherits_parents and (table_schema, table_name) in legacy_inherits_parents
        else table_name_full
    )

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

    is_limited = (
        ctx.options.dbg_stage_1_validate_dict
        or ctx.options.dbg_stage_2_validate_data
        or ctx.options.dbg_stage_3_validate_full
    )

    if files is not None:
        files[f"{hashed_name}.bin.gz"] = {"schema": table_schema, "table": table_name}
        # a condition, a custom query or a debug limit can leave rows out, so foreign keys may not match
        if is_limited or (table_rule and ("raw_sql" in table_rule or table_rule.get("sql_condition"))):
            files[f"{hashed_name}.bin.gz"]["partial_data"] = True

    if table_rule and "raw_sql" in table_rule:
        # the table is transferred using "raw_sql"
        if is_limited:
            query = table_rule["raw_sql"] + " " + ctx.validate_limit
            ctx.logger.info(str(query))
            return query
        return table_rule["raw_sql"]
    # the table is transferred with the specific fields for masking or transferred "as is"
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
        # Schema-qualify the cast target type so user-defined types (e.g. bank.passport) resolve regardless of search_path.
        udt_schema = column_info["udt_schema"]
        field_anon_rule = table_rule["fields"].get(column_name) if table_rule else None

        if column_info["is_generated"] == "ALWAYS":
            continue

        if field_anon_rule:
            if field_anon_rule.find("SQL:") == 0:
                fields.append(f'({field_anon_rule[4:]}) as "{column_name}"')
            else:
                fields.append(f'{field_anon_rule}::"{udt_schema}"."{udt_name}" as "{column_name}"')
        else:
            # field "as is"
            fields.append(f'"{column_name}" as "{column_name}"')

    fields_expr = ",\n".join(fields)
    query = f"SELECT {fields_expr}\nFROM {from_clause_target}"

    conditions = []
    if sql_condition := table_rule and table_rule.get("sql_condition"):
        conditions.append(_strip_leading_where(sql_condition))
    # extension config table: only the rows its condition selects
    if extension_condition := ctx.extension_table_conditions.get((table_schema, table_name)):
        conditions.append(_strip_leading_where(extension_condition))

    if len(conditions) == 1:
        query += f"\nWHERE {conditions[0]}"
    elif conditions:
        query += "\nWHERE " + " AND ".join(f"({condition})" for condition in conditions)

    if is_limited:
        query += f" {ctx.validate_limit}"

    if nulls_last:
        # Order by raw table columns, not output aliases: sorting by masked columns makes PostgreSQL collapse identical masking expressions to one value.
        ordering = ", ".join(
            [
                f'"{table_schema}"."{table_name}"."{field["column_name"]}" NULLS LAST'
                for field in fields_list
                if field["is_nullable"].lower() == "yes"
            ]
        )
        if ordering:
            query += f" ORDER BY {ordering}"

    return query
