from prettytable import PrettyTable
import asyncpg

from pg_anon.common import (
    PgAnonResult,
    ResultCode,
    exception_helper,
    get_dict_rule_for_table,
)
from pg_anon.context import Context


async def view_data(ctx: Context):
    result = PgAnonResult()
    ctx.logger.info("-------------> Started view-data")
    result.result_code = ResultCode.DONE

    try:
        ctx.read_prepared_dict()
    except:
        ctx.logger.error("<------------- view-data failed\n" + exception_helper())
        result.result_code = ResultCode.FAIL
        return result

    db_conn = await asyncpg.connect(**ctx.conn_params)
    table = PrettyTable()
    table_fields_names = []

    table_name_full = f'"{ctx.args.schema_name}"."{ctx.args.table_name}"'
    table_rule = get_dict_rule_for_table(
        ctx.prepared_dictionary_obj["dictionary"], ctx.args.schema_name, ctx.args.table_name
    )
    found_white_list = table_rule is not None

    fields_list = await db_conn.fetch(
        """
            SELECT column_name, udt_name FROM information_schema.columns
            WHERE table_schema = '%s' AND table_name='%s'
            ORDER BY ordinal_position ASC
        """
        % (ctx.args.schema_name.replace("'", "''"), ctx.args.table_name.replace("'", "''"))
    )

    if not found_white_list:
        query = "SELECT * FROM %s LIMIT %s OFFSET %s" % (
            table_name_full,
            ctx.args.limit,
            ctx.args.offset,
        )
    else:
        sql_expr = str()

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
                table_fields_names.append(fld_name + '*')
            else:
                # field "as is"
                if (
                        not column_name.islower() and not column_name.isupper()
                ) or column_name.isupper():
                    sql_expr += f'"{column_name}" as "{column_name}"'
                else:
                    sql_expr += f'"{column_name}" as "{column_name}"'
                table_fields_names.append(column_name)
            if cnt != len(fields_list) - 1:
                sql_expr += ",\n"

        query = "SELECT %s FROM %s LIMIT %s OFFSET %s" % (
            sql_expr,
            table_name_full,
            ctx.args.limit,
            ctx.args.offset,
        )

    ctx.logger.info(str(query))

    table_result = await db_conn.fetch(query)
    await db_conn.close()

    table.field_names = table_fields_names

    try:
        table.add_rows([[record[field["column_name"]] for field in fields_list] for record in table_result])
        print(table)
    except:
        ctx.logger.error("<------------- view-data failed\n" + exception_helper())
        result.result_code = ResultCode.FAIL
        return result

    if result.result_code == ResultCode.DONE:
        ctx.logger.info("<------------- Finished view-data")
    return result
