import re

from pg_anon.common.constants import ANON_UTILS_DB_SCHEMA_NAME
from pg_anon.common.dto import FieldInfo


def get_limit_query(limit: int) -> str:
    return f"LIMIT {limit}" if limit is not None and limit > 0 else ""


def get_count_query(schema_name: str, table_name: str) -> str:
    return f"""
        SELECT count(*)
        FROM \"{schema_name}\".\"{table_name}\"
    """


def get_database_size_query(db_name: str) -> str:
    return f"SELECT pg_database_size('{db_name}')"


def get_relation_size_query(schema: str, table: str) -> str:
    return f"""select pg_total_relation_size('"{schema}"."{table}"')"""


def get_scan_fields_query(limit: int = None, count_only: bool = False):
    if not count_only:
        fields = f"""
            SELECT DISTINCT
            n.nspname,
            c.relname,
            a.attname AS column_name,
            format_type(a.atttypid, a.atttypmod) as type,
            c.oid, a.attnum,
            {ANON_UTILS_DB_SCHEMA_NAME}.digest(n.nspname || '.' || c.relname || '.' || a.attname, '', 'md5') as obj_id,
            {ANON_UTILS_DB_SCHEMA_NAME}.digest(n.nspname || '.' || c.relname, '', 'md5') as tbl_id
        """
        order_by = 'ORDER BY 1, 2, a.attnum' if count_only else ''
    else:
        fields = "SELECT COUNT(*)"
        order_by = ''

    query_limit = get_limit_query(limit)

    return f"""
    {fields}
    FROM pg_class c
    JOIN pg_namespace n on c.relnamespace = n.oid
    JOIN pg_attribute a ON a.attrelid = c.oid
    JOIN pg_type t ON a.atttypid = t.oid
    LEFT JOIN pg_index i ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
    WHERE
        a.attnum > 0
        AND c.relkind IN ('r', 'p')
        AND a.atttypid = t.oid
        AND n.nspname not in ('pg_catalog', 'information_schema', 'pg_toast')
        AND coalesce(i.indisprimary, false) = false
        AND row(c.oid, a.attnum) not in (
            SELECT
                t.oid,
                a.attnum
            FROM pg_class AS t
            JOIN pg_attribute AS a ON a.attrelid = t.oid
            JOIN pg_depend AS d ON d.refobjid = t.oid AND d.refobjsubid = a.attnum
            JOIN pg_class AS s ON s.oid = d.objid
            JOIN pg_namespace AS pn_t ON pn_t.oid = t.relnamespace
            WHERE
                t.relkind IN ('r', 'p')
                AND s.relkind = 'S'
                AND d.deptype = 'a'
                AND d.classid = 'pg_catalog.pg_class'::regclass
                AND d.refclassid = 'pg_catalog.pg_class'::regclass
        )
    {order_by}
    {query_limit}
    """


def get_data_from_field_query(field_info: FieldInfo, limit: int = None, condition: str = None, not_null: bool = True) -> str:
    """
    Build query for receiving data from table
    :param field_info: Field info
    :param limit: batch size
    :param condition: specific WHERE condition for receiving data
    :param not_null: filter for receiving only not null values
    :return: Returns raw SQL query
    """

    conditions = []
    query_condition = ''
    need_where = True

    if condition:
        condition_without_special_characters = re.sub('[^A-Z0-9]+', '', condition.upper())
        if condition_without_special_characters.startswith('WHERE'):
            need_where = False
        conditions.append(condition)

    if not_null:
        conditions.append(f'\"{field_info.column_name}\" is NOT NULL')

    if conditions:
        query_condition = 'WHERE ' if need_where else ''
        query_condition += ' and '.join(conditions)

    query_limit = get_limit_query(limit)

    query = f"""
    SELECT distinct t1._field 
    FROM (
        SELECT (substring(\"{field_info.column_name}\"::text, 1, 8196)) as _field
        FROM \"{field_info.nspname}\".\"{field_info.relname}\"
        {query_condition}
        {query_limit}
    ) as t1
    """

    return query


def get_sequences_query():
    return """
        SELECT
            pn_t.nspname,
            t.relname AS table_name,
            a.attname AS column_name,
            pn_s.nspname,
            s.relname AS sequence_name
        FROM pg_class AS t
        JOIN pg_attribute AS a ON a.attrelid = t.oid
        JOIN pg_depend AS d ON d.refobjid = t.oid AND d.refobjsubid = a.attnum
        JOIN pg_class AS s ON s.oid = d.objid
        JOIN pg_namespace AS pn_t ON pn_t.oid = t.relnamespace
        JOIN pg_namespace AS pn_s ON pn_s.oid = s.relnamespace
        WHERE
            t.relkind IN ('r', 'p')
            AND s.relkind = 'S'
            AND d.deptype = 'a'
            AND d.classid = 'pg_catalog.pg_class'::regclass
            AND d.refclassid = 'pg_catalog.pg_class'::regclass
            """


def get_check_constraint_query():
    return """
        SELECT nsp.nspname,  cl.relname, pc.conname, pg_get_constraintdef(pc.oid)
        FROM (
            SELECT substring(T.v FROM position(' ' in T.v) + 1 for length(T.v) )::bigint as func_oid, t.conoid
            from (
                SELECT T.v as v, t.conoid
                FROM (
                        SELECT ((SELECT regexp_matches(t.v, '(:funcid\s\d+)', 'g'))::text[])[1] as v, t.conoid
                        FROM (
                            SELECT conbin::text as v, oid as conoid
                            FROM pg_constraint
                            WHERE contype = 'c'
                        ) T
                ) T WHERE length(T.v) > 0
            ) T
        ) T
        INNER JOIN pg_constraint pc on T.conoid = pc.oid
        INNER JOIN pg_class cl on cl.oid = pc.conrelid
        INNER JOIN pg_namespace nsp on cl.relnamespace = nsp.oid
        WHERE T.func_oid in (
            SELECT  p.oid
            FROM    pg_namespace n
            INNER JOIN pg_proc p ON p.pronamespace = n.oid
            WHERE   n.nspname not in ( 'pg_catalog', 'information_schema' )
        )
    """


def get_sequences_max_value_init_query():
    return """
    DO $$
    DECLARE
        cmd text;
        schema text;
    BEGIN
        FOR cmd, schema IN (
            select
               ('SELECT setval(''' || T.seq_name || ''', max("' || T.column_name || '") + 1) FROM "' || T.table_name || '"') as cmd,
               T.table_schema as schema
            FROM (
                    select
                       substring(t.column_default from 10 for length(t.column_default) - 21) as seq_name,
                       t.table_schema,
                       t.table_name,
                       t.column_name
                       FROM (
                           SELECT table_schema, table_name, column_name, column_default
                           FROM information_schema.columns
                           WHERE column_default LIKE 'nextval%'
                       ) T
            ) T
        ) LOOP
            EXECUTE 'SET search_path = ''' || schema || ''';';
            -- EXECUTE cmd;
            raise notice '%', cmd;
        END LOOP;
        SET search_path = 'public';
    END$$;"""
