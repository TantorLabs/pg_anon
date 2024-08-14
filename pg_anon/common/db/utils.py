import hashlib
from typing import Dict, List

import asyncpg
from asyncpg import Connection

from pg_anon.context import Context
from pg_anon.common.db.queries import query_get_scan_fields
from pg_anon.common.utils import get_dict_rule_for_table


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


async def get_dump_query(ctx: Context, table_schema: str, table_name: str, db_conn: asyncpg.Connection,
                         files: Dict = {}, excluded_objs: List = [], included_objs: List = [],
                         fields_for_view: List = []):
    table_name_full = f'"{table_schema}"."{table_name}"'

    table_rule = get_dict_rule_for_table(
        dictionary_rules=ctx.prepared_dictionary_obj["dictionary"],
        schema=table_schema,
        table=table_name,
    )
    found_white_list = table_rule is not None

    # dictionary_exclude has the highest priority
    if "dictionary_exclude" in ctx.prepared_dictionary_obj:
        exclude_rule = get_dict_rule_for_table(
            dictionary_rules=ctx.prepared_dictionary_obj["dictionary_exclude"],
            schema=table_schema,
            table=table_name,
        )
        found = exclude_rule is not None
        if found and not found_white_list:
            excluded_objs.append(
                [
                    exclude_rule,
                    table_schema,
                    table_name,
                    "if found and not found_white_list",
                ]
            )
            ctx.logger.info("Skipping: " + str(table_name_full))
            return

    hashed_name = hashlib.md5(
        (table_schema + "_" + table_name).encode()
    ).hexdigest()

    files["%s.bin.gz" % hashed_name] = {"schema": table_schema, "table": table_name}

    if not found_white_list:
        included_objs.append(
            [table_rule, table_schema, table_name, "if not found_white_list"]
        )
        # there is no table in the dictionary, so it will be transferred "as is"
        if (ctx.args.dbg_stage_1_validate_dict
                or ctx.args.dbg_stage_2_validate_data
                or ctx.args.dbg_stage_3_validate_full):
            query = "SELECT * FROM %s %s" % (table_name_full, ctx.validate_limit)
            return query
        else:
            query = f"SELECT * FROM {table_name_full}"
            return query
    else:
        included_objs.append(
            [table_rule, table_schema, table_name, "if found_white_list"]
        )
        # table found in dictionary
        if "raw_sql" in table_rule:
            # the table is transferred using "raw_sql"
            if (ctx.args.dbg_stage_1_validate_dict
                    or ctx.args.dbg_stage_2_validate_data
                    or ctx.args.dbg_stage_3_validate_full):
                query = table_rule["raw_sql"] + " " + ctx.validate_limit
                return query
            else:
                query = table_rule["raw_sql"]
                return query
        else:
            # the table is transferred with the specific fields for anonymization
            fields_list = await db_conn.fetch(
                """
                    SELECT column_name, udt_name FROM information_schema.columns
                    WHERE table_schema = '%s' AND table_name='%s'
                    ORDER BY ordinal_position ASC
                """
                % (table_schema.replace("'", "''"), table_name.replace("'", "''"))
            )

            sql_expr = ""

            def check_fld(fld_name):
                if fld_name in table_rule["fields"]:
                    return fld_name, table_rule["fields"][fld_name]
                return None, None

            for cnt, column_info in enumerate(fields_list):
                column_name = column_info["column_name"]
                udt_name = column_info["udt_name"]
                fld_name, fld_val = check_fld(column_name)
                if fld_name:
                    if fld_val.find("SQL:") == 0:
                        sql_expr += f'({fld_val[4:]}) as "{fld_name}"'
                    else:
                        sql_expr += f'{fld_val}::{udt_name} as "{fld_name}"'
                    fields_for_view.append(fld_name + '*')
                else:
                    # field "as is"
                    if (
                            not column_name.islower() and not column_name.isupper()
                    ) or column_name.isupper():
                        sql_expr += f'"{column_name}" as "{column_name}"'
                    else:
                        sql_expr += f'"{column_name}" as "{column_name}"'
                    fields_for_view.append(column_name)
                if cnt != len(fields_list) - 1:
                    sql_expr += ",\n"

            if (ctx.args.dbg_stage_1_validate_dict
                    or ctx.args.dbg_stage_2_validate_data
                    or ctx.args.dbg_stage_3_validate_full):
                query = "SELECT %s FROM %s %s" % (
                    sql_expr,
                    table_name_full,
                    ctx.validate_limit,
                )
                return query
            else:
                query = "SELECT %s FROM %s" % (
                    sql_expr,
                    table_name_full,
                )
                return query

