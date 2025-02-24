import os

from pg_anon.common.db_utils import create_connection
from pg_anon.common.dto import PgAnonResult
from pg_anon.common.enums import ResultCode
from pg_anon.common.utils import exception_helper


async def make_init(ctx):
    result = PgAnonResult()
    ctx.logger.info("-------------> Started init mode")

    async def handle_notice(connection, message):
        ctx.logger.info("NOTICE: %s" % message)

    db_conn = await create_connection(ctx.connection_params, server_settings=ctx.server_settings)
    db_conn.add_log_listener(handle_notice)

    tr = db_conn.transaction()
    await tr.start()
    try:
        with open(os.path.join(ctx.current_dir, "init.sql"), "r") as f:
            data = f.read()
        await db_conn.execute(data)
        await tr.commit()
        result.result_code = ResultCode.DONE
    except:
        await tr.rollback()
        ctx.logger.error(exception_helper(show_traceback=True))
        result.result_code = ResultCode.FAIL
    finally:
        await db_conn.close()
    ctx.logger.info("<------------- Finished init mode")
    return result
