import asyncio
import logging
import os
import sys
from datetime import datetime
from typing import Optional, List

from pg_anon.common.constants import ANON_UTILS_DB_SCHEMA_NAME
from pg_anon.common.db_utils import check_anon_utils_db_schema_exists, get_pg_version
from pg_anon.common.dto import PgAnonResult
from pg_anon.common.enums import ResultCode, VerboseOptions, AnonMode
from pg_anon.common.utils import check_pg_util, exception_helper, simple_slugify
from pg_anon.context import Context
from pg_anon.logger import get_logger, logger_add_file_handler, logger_set_log_level
from pg_anon.modes.create_dict import CreateDictMode
from pg_anon.modes.dump import DumpMode
from pg_anon.modes.initialization import InitMode
from pg_anon.modes.restore import RestoreMode
from pg_anon.modes.view_data import ViewDataMode
from pg_anon.modes.view_fields import ViewFieldsMode
from pg_anon.version import __version__


async def run_pg_anon(cli_run_params: Optional[List[str]] = None) -> None:
    """
    Run pg_anon
    :param cli_run_params: list of params in command line format
    :return: result of pg_anon
    """
    parser = Context.get_arg_parser()
    args = parser.parse_args(cli_run_params)
    result = await MainRoutine(args).run()
    if result.result_code == ResultCode.FAIL:
        exit(1)


class MainRoutine:

    def __init__(self, external_args=None):
        file_dir = os.path.dirname(os.path.realpath(__file__))
        self.current_dir = os.path.dirname(file_dir)
        self.start_time = datetime.now().strftime('%Y_%m_%d__%H_%M')
        if external_args is not None:
            self.args = external_args
        else:
            self.args = Context.get_arg_parser().parse_args()
        self.setup_logger()
        self.context = Context(self.args)
        self.context.logger = self.logger
        self.result = PgAnonResult()

        self._skip_check_postgres_utils = self.context.args.mode in (
            AnonMode.INIT,
            AnonMode.CREATE_DICT,
            AnonMode.VIEW_FIELDS,
            AnonMode.VIEW_DATA,
        )

    def setup_logger(self):
        log_level = logging.NOTSET

        if self.args.mode not in (AnonMode.VIEW_FIELDS, AnonMode.VIEW_DATA):
            if self.args.verbose == VerboseOptions.INFO:
                log_level = logging.INFO
            elif self.args.verbose == VerboseOptions.DEBUG:
                log_level = logging.DEBUG
            elif self.args.verbose == VerboseOptions.ERROR:
                log_level = logging.ERROR

        additional_file_name = None
        if self.args.mode == AnonMode.CREATE_DICT and self.args.meta_dict_files:
            additional_file_name = os.path.splitext(os.path.basename(self.args.meta_dict_files[0]))[0]
        elif self.args.prepared_sens_dict_files:
            additional_file_name = os.path.splitext(os.path.basename(self.args.prepared_sens_dict_files[0]))[0]
        elif self.args.input_dir:
            additional_file_name = os.path.basename(self.args.input_dir)

        log_file_name_parts = [self.start_time, self.args.mode.value]
        if additional_file_name:
            log_file_name_parts.append(simple_slugify(additional_file_name))

        log_file_name = "__".join(log_file_name_parts) + ".log"
        log_dir = os.path.join(self.current_dir, "log")

        self.logger = get_logger()
        logger_add_file_handler(
            log_dir=log_dir,
            log_file_name=log_file_name
        )
        logger_set_log_level(log_level=log_level)

    def _bootstrap(self):
        if self.args.version:
            self.context.logger.info("Version %s" % __version__)
            sys.exit(0)

        self.context.logger.info(
            "============ %s version %s ============"
            % (os.path.basename(__file__), __version__)
        )
        self.context.logger.info(
            "============> Started MainRoutine.run in mode: %s" % self.context.args.mode
        )
        if self.context.args.debug:
            params_info = "#--------------- Incoming parameters\n"
            for arg in vars(self.args):
                if arg not in ("db_user_password"):
                    params_info += "#   %s = %s\n" % (arg, getattr(self.args, arg))
            params_info += "#-----------------------------------"
            self.context.logger.info(params_info)

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
        if self.context.args.mode in (
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
        if self.context.args.mode in (AnonMode.DUMP, AnonMode.SYNC_DATA_DUMP, AnonMode.SYNC_STRUCT_DUMP):
            return DumpMode(self.context)

        if self.context.args.mode in (AnonMode.RESTORE, AnonMode.SYNC_DATA_RESTORE, AnonMode.SYNC_STRUCT_RESTORE):
            return RestoreMode(self.context)

        if self.context.args.mode == AnonMode.INIT:
            return InitMode(self.context)

        if self.context.args.mode == AnonMode.CREATE_DICT:
            return CreateDictMode(self.context)

        if self.context.args.mode == AnonMode.VIEW_FIELDS:
            return ViewFieldsMode(self.context)

        if self.context.args.mode == AnonMode.VIEW_DATA:
            return ViewDataMode(self.context)

        raise RuntimeError("Unknown mode: " + self.context.args.mode)

    async def run(self) -> PgAnonResult:
        self._bootstrap()
        self.result.start()
        try:
            await self._set_postgres_utils()
            self._check_postgres_utils()
            await self._check_initialization()

            mode = self._get_mode()
            self.result.result_data = await mode.run()
            self.result.complete()
        except:
            self.context.logger.error(exception_helper(show_traceback=True))
            self.result.fail()
        finally:
            self.context.logger.info(
                f"<============ Finished MainRoutine.run in mode: {self.context.args.mode.value}, "
                f"result_code = {self.result.result_code.value}, "
                f"elapsed: {self.result.elapsed} sec"
            )

            return self.result

    async def validate_target_tables(self) -> PgAnonResult:
        result = PgAnonResult()
        result.start()

        try:
            await RestoreMode.validate_restore(self.context)
            result.complete()
        except:
            self.context.logger.error(exception_helper(show_traceback=True))
            result.fail()
        finally:
            return result


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run_pg_anon())
