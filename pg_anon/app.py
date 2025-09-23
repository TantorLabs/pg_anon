from pg_anon.common.constants import ANON_UTILS_DB_SCHEMA_NAME
from pg_anon.common.db_utils import check_anon_utils_db_schema_exists, get_pg_version
from pg_anon.common.dto import PgAnonResult, RunOptions
from pg_anon.common.enums import AnonMode
from pg_anon.common.utils import check_pg_util, exception_helper
from pg_anon.context import Context
from pg_anon.modes.create_dict import CreateDictMode
from pg_anon.modes.dump import DumpMode
from pg_anon.modes.initialization import InitMode
from pg_anon.modes.restore import RestoreMode
from pg_anon.modes.view_data import ViewDataMode
from pg_anon.modes.view_fields import ViewFieldsMode
from pg_anon.version import __version__


class PgAnonApp:

    def __init__(self, options: RunOptions):
        self.context = Context(options)
        self.result = PgAnonResult()
        self._skip_check_postgres_utils = self.context.options.mode in (
            AnonMode.INIT,
            AnonMode.CREATE_DICT,
            AnonMode.VIEW_FIELDS,
            AnonMode.VIEW_DATA,
        )

    def _bootstrap(self):
        self.context.logger.info(
            "============> Started pg_anon (v%s) in mode: %s"
            % (__version__, self.context.options.mode.value)
        )
        if self.context.options.debug:
            params_info = "#--------------- Run options\n"
            params_info += self.context.options.to_json()
            params_info += "\n#-----------------------------------"
            self.context.logger.debug(params_info)

    async def _set_postgres_utils(self):
        pg_version = await get_pg_version(self.context.connection_params, server_settings=self.context.server_settings)
        self.context.set_postgres_version(pg_version)
        self.context.logger.info(f"Target DB version: {pg_version}")
        self.context.logger.info(f"pg_dump path: {self.context.pg_dump}")
        self.context.logger.info(f"pg_restore path: {self.context.pg_restore}")

    def _check_postgres_utils(self):
        if self._skip_check_postgres_utils:
            self.context.logger.info(f"Skip postgres utils exists check")
            return

        self.context.logger.info(f"Postgres utils exists checking")

        pg_dump_exists = check_pg_util(self.context, self.context.pg_dump, "pg_dump")
        pg_restore_exists = check_pg_util(self.context, self.context.pg_restore, "pg_restore")

        if not pg_dump_exists or not pg_restore_exists:
            raise RuntimeError('pg_dump or pg_restore not found')

    async def _check_initialization(self):
        if self.context.options.mode in (
                AnonMode.CREATE_DICT,
                AnonMode.DUMP,
                AnonMode.SYNC_DATA_DUMP,
                AnonMode.SYNC_STRUCT_DUMP,
        ):
            anon_utils_schema_exists = await check_anon_utils_db_schema_exists(
                connection_params=self.context.connection_params,
                server_settings=self.context.server_settings
            )
            if not anon_utils_schema_exists:
                raise ValueError(
                    f"Schema '{ANON_UTILS_DB_SCHEMA_NAME}' does not exist. First you need execute init, by run '--mode=init'"
                )

    def _get_mode(self):
        if self.context.options.mode in (AnonMode.DUMP, AnonMode.SYNC_DATA_DUMP, AnonMode.SYNC_STRUCT_DUMP):
            return DumpMode(self.context)

        if self.context.options.mode in (AnonMode.RESTORE, AnonMode.SYNC_DATA_RESTORE, AnonMode.SYNC_STRUCT_RESTORE):
            return RestoreMode(self.context)

        if self.context.options.mode == AnonMode.INIT:
            return InitMode(self.context)

        if self.context.options.mode == AnonMode.CREATE_DICT:
            return CreateDictMode(self.context)

        if self.context.options.mode == AnonMode.VIEW_FIELDS:
            return ViewFieldsMode(self.context)

        if self.context.options.mode == AnonMode.VIEW_DATA:
            return ViewDataMode(self.context)

        raise RuntimeError("Unknown mode: " + self.context.options.mode.value)

    async def run(self) -> PgAnonResult:
        self._bootstrap()
        self.result.start(self.context.options)
        try:
            await self._set_postgres_utils()
            self._check_postgres_utils()
            await self._check_initialization()

            mode = self._get_mode()
            self.result.result_data = await mode.run()
            self.result.complete()
        except Exception as exc:
            self.context.logger.error(exception_helper(show_traceback=True))
            self.result.fail(exc)
        finally:
            self.context.logger.info(
                f"<============ Finished pg_anon in mode: {self.context.options.mode.value}, "
                f"result_code = {self.result.result_code.value}, "
                f"elapsed: {self.result.elapsed} sec"
            )

            return self.result

    async def validate_target_tables(self) -> PgAnonResult:
        result = PgAnonResult()
        result.start(self.context.options)

        try:
            await RestoreMode.validate_restore(self.context)
            result.complete()
        except:
            self.context.logger.error(exception_helper(show_traceback=True))
            result.fail()
        finally:
            return result
