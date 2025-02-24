import asyncio
import logging
import os
import sys
import time
from datetime import datetime
from typing import Optional, List

from pg_anon.common.constants import ANON_UTILS_DB_SCHEMA_NAME
from pg_anon.common.db_utils import check_anon_utils_db_schema_exists, get_pg_version
from pg_anon.common.dto import PgAnonResult
from pg_anon.common.enums import ResultCode, VerboseOptions, AnonMode
from pg_anon.common.utils import check_pg_util, exception_helper, simple_slugify
from pg_anon.context import Context
from pg_anon.modes.create_dict import create_dict
from pg_anon.modes.dump import DumpMode
from pg_anon.logger import get_logger, logger_add_file_handler, logger_set_log_level
from pg_anon.modes.initialization import make_init
from pg_anon.modes.restore import RestoreMode
from pg_anon.version import __version__
from pg_anon.modes.view_data import ViewDataMode
from pg_anon.modes.view_fields import ViewFieldsMode


async def run_pg_anon(cli_run_params: Optional[List[str]] = None) -> PgAnonResult:
    """
    Run pg_anon
    :param cli_run_params: list of params in command line format
    :return: result of pg_anon
    """
    parser = Context.get_arg_parser()
    args = parser.parse_args(cli_run_params)
    return await MainRoutine(args).run()


class MainRoutine:
    external_args = None
    logger = None
    args = None

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

    async def run(self) -> PgAnonResult:
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

        result = PgAnonResult()
        try:
            pg_version = await get_pg_version(self.context.connection_params, server_settings=self.context.server_settings)
            self.context.set_postgres_version(pg_version)
            self.context.logger.debug(f"Target DB version: {pg_version}")
            self.context.logger.debug(f"pg_dump path: {self.context.pg_dump}")
            self.context.logger.debug(f"pg_restore path: {self.context.pg_restore}")
        except:
            self.context.logger.error(exception_helper(show_traceback=True))
            result.result_code = ResultCode.FAIL
            return result

        pg_dump_exists = check_pg_util(self.context, self.context.pg_dump, "pg_dump")
        pg_restore_exists = check_pg_util(self.context, self.context.pg_restore, "pg_restore")
        if not pg_dump_exists or not pg_restore_exists:
            result.result_code = ResultCode.FAIL
            return result

        if self.context.args.mode in (
                AnonMode.CREATE_DICT,
                AnonMode.DUMP,
                AnonMode.SYNC_DATA_DUMP,
                AnonMode.SYNC_STRUCT_DUMP,
            ):
            try:
                anon_utils_schema_exists = await check_anon_utils_db_schema_exists(
                    connection_params=self.context.connection_params,
                    server_settings=self.context.server_settings
                )
                if not anon_utils_schema_exists:
                    raise ValueError(
                        f"Schema '{ANON_UTILS_DB_SCHEMA_NAME}' does not exist. First you need execute init, by run '--mode=init'"
                    )
            except:
                self.context.logger.error(exception_helper(show_traceback=True))
                result.result_code = ResultCode.FAIL
                return result

        start_t = time.time()
        try:
            if self.context.args.mode in (
                AnonMode.DUMP,
                AnonMode.SYNC_DATA_DUMP,
                AnonMode.SYNC_STRUCT_DUMP,
            ):
                result = await DumpMode(self.context).run()
            elif self.context.args.mode in (
                AnonMode.RESTORE,
                AnonMode.SYNC_DATA_RESTORE,
                AnonMode.SYNC_STRUCT_RESTORE,
            ):
                restore_mode = RestoreMode(self.context)
                result = await restore_mode.run()
                await restore_mode.run_analyze()
            elif self.context.args.mode == AnonMode.INIT:
                result = await make_init(self.context)
            elif self.context.args.mode == AnonMode.CREATE_DICT:
                result = await create_dict(self.context)
            elif self.context.args.mode == AnonMode.VIEW_FIELDS:
                result = await ViewFieldsMode(self.context).run()
            elif self.context.args.mode == AnonMode.VIEW_DATA:
                result = await ViewDataMode(self.context).run()
            else:
                raise Exception("Unknown mode: " + self.context.args.mode)

            self.context.logger.info(f"MainRoutine.run result_code = {result.result_code}")
        except:
            self.context.logger.error(exception_helper(show_traceback=True))
        finally:
            end_t = time.time()
            result.elapsed = round(end_t - start_t, 2)
            self.context.logger.info(
                f"<============ Finished MainRoutine.run in mode: {self.context.args.mode}, elapsed: {result.elapsed} sec"
            )

            return result

    async def validate_target_tables(self) -> PgAnonResult:
        result = PgAnonResult()
        try:
            result = await RestoreMode.validate_restore(self.context)
        except:
            self.context.logger.error(exception_helper(show_traceback=True))
        finally:
            return result


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run_pg_anon())
