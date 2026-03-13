from pathlib import Path

from pg_anon.common.constants import ANON_UTILS_DB_SCHEMA_NAME, SAVED_RUN_OPTIONS_FILE_NAME, SAVED_RUN_STATUS_FILE_NAME
from pg_anon.common.db_utils import check_anon_utils_db_schema_exists, get_pg_version
from pg_anon.common.dto import PgAnonResult, RunOptions
from pg_anon.common.enums import AnonMode
from pg_anon.common.errors import ErrorCode, PgAnonError
from pg_anon.common.utils import check_pg_util, save_json_file
from pg_anon.context import Context
from pg_anon.modes.create_dict import CreateDictMode
from pg_anon.modes.dump import DumpMode
from pg_anon.modes.initialization import InitMode
from pg_anon.modes.restore import RestoreMode
from pg_anon.modes.view_data import ViewDataMode
from pg_anon.modes.view_fields import ViewFieldsMode
from pg_anon.version import __version__


class PgAnonApp:
    def __init__(self, options: RunOptions) -> None:
        run_dir = Path(options.run_dir)
        run_dir.mkdir(parents=True, exist_ok=True)
        save_json_file(run_dir / SAVED_RUN_OPTIONS_FILE_NAME, options.to_dict())

        self.context = Context(options)
        self.result = PgAnonResult()
        self._skip_check_postgres_utils = self.context.options.mode in (
            AnonMode.INIT,
            AnonMode.CREATE_DICT,
            AnonMode.VIEW_FIELDS,
            AnonMode.VIEW_DATA,
        )

    def _bootstrap(self) -> None:
        self.context.logger.info(
            "============> Started pg_anon (v%s) in mode: %s", __version__, self.context.options.mode.value
        )
        if self.context.options.debug:
            params_info = "#--------------- Run options\n"
            params_info += self.context.options.to_json()
            params_info += "\n#-----------------------------------"
            self.context.logger.debug(params_info)

    async def _set_postgres_utils(self) -> None:
        pg_version = await get_pg_version(self.context.connection_params, server_settings=self.context.server_settings)
        self.context.set_postgres_version(pg_version)
        self.context.logger.info("Target DB version: %s", pg_version)
        self.context.logger.info("pg_dump path: %s", self.context.pg_dump)
        self.context.logger.info("pg_restore path: %s", self.context.pg_restore)

    def _check_postgres_utils(self) -> None:
        if self._skip_check_postgres_utils:
            self.context.logger.info("Skip postgres utils exists check")
            return

        self.context.logger.info("Postgres utils exists checking")

        pg_dump_exists = check_pg_util(self.context, self.context.pg_dump, "pg_dump")
        pg_restore_exists = check_pg_util(self.context, self.context.pg_restore, "pg_restore")

        if not pg_dump_exists or not pg_restore_exists:
            raise PgAnonError(ErrorCode.PG_TOOLS_NOT_FOUND, "pg_dump or pg_restore not found")

    async def _check_initialization(self) -> None:
        if self.context.options.mode in (
            AnonMode.CREATE_DICT,
            AnonMode.DUMP,
            AnonMode.SYNC_DATA_DUMP,
            AnonMode.SYNC_STRUCT_DUMP,
        ):
            anon_utils_schema_exists = await check_anon_utils_db_schema_exists(
                connection_params=self.context.connection_params, server_settings=self.context.server_settings
            )
            if not anon_utils_schema_exists:
                raise PgAnonError(
                    ErrorCode.SCHEMA_NOT_INITIALIZED,
                    f"Schema '{ANON_UTILS_DB_SCHEMA_NAME}' does not exist. First you need execute init, by run '--mode=init'",
                )

    def _get_mode(self) -> DumpMode | RestoreMode | InitMode | CreateDictMode | ViewFieldsMode | ViewDataMode:
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

        raise PgAnonError(ErrorCode.UNKNOWN_MODE, "Unknown mode: " + self.context.options.mode.value)

    async def run(self) -> PgAnonResult:
        """Execute the anonymization pipeline and return the result."""
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
            self.context.logger.exception(
                "<============ %s failed", self.context.options.mode.value
            )
            self.result.fail(exc)

        self.context.logger.info(
            "<============ Finished pg_anon in mode: %s, result_code = %s, elapsed: %s sec",
            self.context.options.mode.value,
            self.result.result_code.value,
            self.result.elapsed,
        )
        save_json_file(Path(self.context.options.run_dir) / SAVED_RUN_STATUS_FILE_NAME, self.result.to_dict())

        return self.result

    async def validate_target_tables(self) -> PgAnonResult:
        """Validate that target tables exist and are ready for restore."""
        result = PgAnonResult()
        result.start(self.context.options)

        try:
            await RestoreMode.validate_restore(self.context)
            result.complete()
        except Exception:
            self.context.logger.exception("Validate target tables failed")
            result.fail()

        return result
