from prettytable import PrettyTable
import asyncpg

from pg_anon.common.dto import PgAnonResult
from pg_anon.common.enums import ResultCode
from pg_anon.common.utils import get_dict_rule_for_table
from pg_anon.common.db.utils import get_dump_query
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

    query = await get_dump_query(
        ctx=ctx,
        table_schema=ctx.args.schema_name,
        table_name=ctx.args.table_name,
        db_conn=db_conn,
        fields_for_view=table_fields_names
    )

    ctx.logger.info(str(query))

    table_result = await db_conn.fetch(query)
    fields_list = await db_conn.fetch(
        """
            SELECT column_name, udt_name FROM information_schema.columns
            WHERE table_schema = '%s' AND table_name='%s'
            ORDER BY ordinal_position ASC
        """
        % (ctx.args.schema_name.replace("'", "''"), ctx.args.table_name.replace("'", "''"))
    )
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
