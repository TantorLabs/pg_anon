from pg_anon.common.constants import BASE_DIR
from pg_anon.common.db_utils import create_connection
from pg_anon.context import Context


class InitMode:
    def __init__(self, context: Context):
        self.context = context

    async def run(self) -> None:
        self.context.logger.info("-------------> Started init mode")

        async def handle_notice(connection, message):
            self.context.logger.info("NOTICE: %s" % message)

        db_conn = await create_connection(self.context.connection_params, server_settings=self.context.server_settings)
        db_conn.add_log_listener(handle_notice)

        tr = db_conn.transaction()
        await tr.start()

        try:
            with open(BASE_DIR / "init.sql", "r") as f:
                data = f.read()
            await db_conn.execute(data)
            await tr.commit()

            self.context.logger.info("<------------- Finished init mode")
        except Exception as ex:
            await tr.rollback()
            raise ex
        finally:
            await db_conn.close()
