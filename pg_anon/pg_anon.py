import asyncio
import logging
import os
import re
import sys
import time

from logging.handlers import RotatingFileHandler

import asyncpg


from pg_anon.common import (
    AnonMode,
    PgAnonResult,
    ResultCode,
    VerboseOptions,
    check_pg_util,
    exception_helper,
)
from pg_anon.create_dict import create_dict
from pg_anon.context import Context
from pg_anon.dump import make_dump
from pg_anon.restore import make_restore, run_analyze, validate_restore
from pg_anon.version import __version__


async def make_init(ctx):
    result = PgAnonResult()
    ctx.logger.info("-------------> Started init mode")

    async def handle_notice(connection, message):
        ctx.logger.info("NOTICE: %s" % message)

    db_conn = await asyncpg.connect(**ctx.conn_params)
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


class MainRoutine:
    external_args = None
    logger = None
    args = None

    def __init__(self, external_args=None):
        file_dir = os.path.dirname(os.path.realpath(__file__))
        self.current_dir = os.path.dirname(file_dir)
        if external_args is not None:
            self.args = external_args
        else:
            self.args = Context.get_arg_parser().parse_args()
        self.setup_logger()
        self.ctx = Context(self.args)
        self.ctx.logger = self.logger

    def __del__(self):
        self.close_logger_handlers()

    def setup_logger(self):
        if self.args.verbose == VerboseOptions.INFO:
            log_level = logging.INFO
        if self.args.verbose == VerboseOptions.DEBUG:
            log_level = logging.DEBUG
        if self.args.verbose == VerboseOptions.ERROR:
            log_level = logging.ERROR

        self.logger = logging.getLogger(os.path.basename(__file__))
        # if len(self.logger.handlers):
        #    self.close_previous_logger_handlers()

        if not len(self.logger.handlers):
            formatter = logging.Formatter(
                datefmt="%Y-%m-%d %H:%M:%S",
                fmt="%(asctime)s,%(msecs)03d %(levelname)8s %(lineno)3d - %(message)s",
            )

            handler = logging.StreamHandler(sys.stdout)
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

            if not os.path.exists(os.path.join(self.current_dir, "log")):
                os.makedirs(os.path.join(self.current_dir, "log"))

            if self.args.mode == AnonMode.INIT:
                log_file = str(self.args.mode) + ".log"
            elif self.args.mode == AnonMode.CREATE_DICT:
                log_file = "%s_%s.log" % (
                    str(self.args.mode),
                    str(
                        os.path.splitext(os.path.basename(self.args.meta_dict_files[0]))[0]
                        if self.args.meta_dict_files
                        else os.path.basename(self.args.input_dir)
                    ),
                )
            else:
                log_file = "%s_%s.log" % (
                    str(self.args.mode),
                    str(
                        os.path.splitext(os.path.basename(self.args.prepared_sens_dict_files[0]))[0]
                        if self.args.prepared_sens_dict_files
                        else os.path.basename(self.args.input_dir)
                    ),
                )

            f_handler = RotatingFileHandler(
                os.path.join(self.current_dir, "log", log_file),
                maxBytes=1024 * 10000,
                backupCount=10,
            )
            f_handler.setFormatter(formatter)

            self.logger.addHandler(f_handler)
            self.logger.setLevel(log_level)

    def close_logger_handlers(self):
        if not self.logger:  # FIXME: Return an exception for --help command
            return
        for handler in self.logger.handlers[
            :
        ]:  # iterate over a copy of the handlers list
            try:
                handler.acquire()
                handler.flush()
                handler.close()
            except Exception as e:
                print(f"Error closing log handler: {e}")
            finally:
                handler.release()
                self.logger.removeHandler(handler)

    async def run(self) -> PgAnonResult:
        if self.args.version:
            self.ctx.logger.info("Version %s" % __version__)
            sys.exit(0)

        self.ctx.logger.info(
            "============ %s version %s ============"
            % (os.path.basename(__file__), __version__)
        )
        self.ctx.logger.info(
            "============> Started MainRoutine.run in mode: %s" % self.ctx.args.mode
        )
        if self.ctx.args.debug:
            params_info = "#--------------- Incoming parameters\n"
            for arg in vars(self.args):
                if arg not in ("db_user_password"):
                    params_info += "#   %s = %s\n" % (arg, getattr(self.args, arg))
            params_info += "#-----------------------------------"
            self.ctx.logger.info(params_info)

        result = PgAnonResult()
        try:
            db_conn = await asyncpg.connect(**self.ctx.conn_params)
            self.ctx.pg_version = await db_conn.fetchval("select version()")
            self.ctx.pg_version = re.findall(r"(\d+\.\d+)", str(self.ctx.pg_version))[0]
            await db_conn.close()
        except:
            self.ctx.logger.error(exception_helper(show_traceback=True))
            result.result_code = ResultCode.FAIL
            return result

        if not check_pg_util(
            self.ctx, self.ctx.args.pg_dump, "pg_dump"
        ) or not check_pg_util(self.ctx, self.ctx.args.pg_restore, "pg_restore"):
            result.result_code = ResultCode.FAIL
            return result

        start_t = time.time()
        try:
            if self.ctx.args.mode in (
                AnonMode.DUMP,
                AnonMode.SYNC_DATA_DUMP,
                AnonMode.SYNC_STRUCT_DUMP,
            ):
                result = await make_dump(self.ctx)
            elif self.ctx.args.mode in (
                AnonMode.RESTORE,
                AnonMode.SYNC_DATA_RESTORE,
                AnonMode.SYNC_STRUCT_RESTORE,
            ):
                result = await make_restore(self.ctx)
                if (self.ctx.args.mode in (AnonMode.SYNC_DATA_RESTORE, AnonMode.RESTORE)
                        and not self.ctx.metadata["dbg_stage_2_validate_data"]
                        and not self.ctx.metadata["dbg_stage_3_validate_full"]):
                    await run_analyze(self.ctx)
            elif self.ctx.args.mode == AnonMode.INIT:
                result = await make_init(self.ctx)
            elif self.ctx.args.mode == AnonMode.CREATE_DICT:
                result = await create_dict(self.ctx)
            else:
                raise Exception("Unknown mode: " + self.ctx.args.mode)

            self.ctx.logger.info(
                "MainRoutine.run result_code = %s" % result.result_code
            )
        except:
            self.ctx.logger.error(exception_helper(show_traceback=True))
        finally:
            end_t = time.time()
            self.ctx.logger.info(
                "<============ Finished MainRoutine.run in mode: %s, elapsed: %s sec"
                % (self.ctx.args.mode, str(round(end_t - start_t, 2)))
            )
            if result.result_data is None:
                result.result_data = {"elapsed": str(round(end_t - start_t, 2))}
            else:
                result.result_data["elapsed"] = str(round(end_t - start_t, 2))

            return result

    async def validate_target_tables(self) -> PgAnonResult:
        result = PgAnonResult()
        try:
            result = await validate_restore(self.ctx)
        except:
            self.ctx.logger.error(exception_helper(show_traceback=True))
        finally:
            return result


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(MainRoutine().run())
