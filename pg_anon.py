import argparse
import logging
from logging.handlers import RotatingFileHandler
from dump import *
from restore import *
from create_dict import *

PG_ANON_VERSION = "0.1.0"


class BasicEnum:
    def __str__(self):
        return self.value


class OutputFormat(BasicEnum, Enum):
    BINARY = "binary"
    TEXT = "text"


class Context:
    @exception_handler
    def __init__(self, args):
        self.current_dir = os.path.dirname(os.path.realpath(__file__))
        self.args = args
        self.pg_version = None
        self.validate_limit = "LIMIT 100"
        self.dictionary_content = None  # for dump process
        self.dictionary_obj = {}
        self.metadata = None  # for restore process
        self.task_results = {}  # for dump process (key is hash() of SQL query)
        self.total_rows = 0
        self.create_dict_matches = {}  # for create-dict mode
        self.exclude_schemas = ["anon_funcs", "columnar_internal"]

        if args.db_user_password == "" and os.environ.get("PGPASSWORD") is not None:
            args.db_user_password = os.environ["PGPASSWORD"]

        self.conn_params = {
            "host": args.db_host,
            "database": args.db_name,
            "port": args.db_port,
            "user": args.db_user,
        }

        if args.db_passfile != "":
            self.conn_params.update({"passfile": args.db_passfile})

        if args.db_user_password != "":
            self.conn_params.update({"password": args.db_user_password})

        if (
            args.db_ssl_cert_file != ""
            or args.db_ssl_key_file != ""
            or args.db_ssl_ca_file != ""
        ):
            self.conn_params.update(
                {
                    "ssl": "on",
                    "ssl_min_protocol_version": "TLSv1.2",
                    "ssl_cert_file": args.db_ssl_cert_file,
                    "ssl_key_file": args.db_ssl_key_file,
                    "ssl_ca_file": args.db_ssl_ca_file,
                }
            )

    @staticmethod
    def get_arg_parser():
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--version",
            help="Show the version number and exit",
            action="store_true",
            default=False,
        )
        parser.add_argument(
            "--debug",
            help="Enable debug mode, (default: %(default)s)",
            action="store_true",
            default=False,
        )
        parser.add_argument(
            "--db-host",
            type=str,
        )
        parser.add_argument("--db-port", type=str, default="5432")
        parser.add_argument("--db-name", type=str, default="default")
        parser.add_argument("--db-user", type=str, default="default")
        parser.add_argument("--db-user-password", type=str, default="")
        parser.add_argument("--db-passfile", type=str, default="")
        parser.add_argument("--db-ssl-key-file", type=str, default="")
        parser.add_argument("--db-ssl-cert-file", type=str, default="")
        parser.add_argument("--db-ssl-ca-file", type=str, default="")
        parser.add_argument(
            "--mode", type=AnonMode, choices=list(AnonMode), default=AnonMode.INIT
        )
        parser.add_argument(
            "--copy-options",
            dest="copy_options",
            default="",
            help='Options for COPY command like "with binary".',
        )
        parser.add_argument(
            "--format",
            dest="format",
            type=OutputFormat,
            choices=list(OutputFormat),
            default=OutputFormat.BINARY.value,
            help="COPY data format, can be overwritten by --copy-options. Selects the data format to be read or written: text, csv or binary.",
        )
        parser.add_argument(
            "--verbose",
            dest="verbose",
            type=VerboseOptions,
            choices=list(VerboseOptions),
            default=VerboseOptions.INFO,
            help="Enable verbose output",
        )
        parser.add_argument("--dict-file", type=str, default="")
        parser.add_argument("--threads", type=int, default=4)
        parser.add_argument("--pg-dump", type=str, default="/usr/bin/pg_dump")
        parser.add_argument("--pg-restore", type=str, default="/usr/bin/pg_restore")
        parser.add_argument("--output-dir", type=str, default="")
        parser.add_argument("--input-dir", type=str, default="")
        parser.add_argument(
            "--validate-dict",
            action="store_true",
            default=False,
            help="""Validate dictionary, show the tables and run SQL queries without data export""",
        )
        parser.add_argument(
            "--validate-full",
            action="store_true",
            default=False,
            help="""Same as "--validate-dict" + data export with limit""",
        )
        parser.add_argument("--clear-output-dir", action="store_true", default=False)
        parser.add_argument(
            "--drop-custom-check-constr", action="store_true", default=False
        )
        parser.add_argument(
            "--seq-init-by-max-value",
            action="store_true",
            default=False,
            help="""Initialize sequences based on maximum values. Otherwise, the sequences will be initialized
                based on the values of the source database.""",
        )
        parser.add_argument(
            "--disable-checks",
            action="store_true",
            default=False,
            help="""Disable checks of disk space and PostgreSQL version""",
        )
        parser.add_argument(
            "--skip-data",
            action="store_true",
            default=False,
            help="""Don't copy data. Only the database structure will be dumped""",
        )
        parser.add_argument(
            "--scan-mode",
            type=ScanMode,
            choices=list(ScanMode),
            default=ScanMode.PARTIAL.value,
            help="In '--create-dict' mode defines whether to scan all data or only part of it",
        )
        parser.add_argument(
            "--output-dict-file",
            type=str,
            default="output-dict-file.py",
            help="In '--create-dict' mode output file will be saved to this value",
        )
        parser.add_argument(
            "--scan-partial-rows",
            type=int,
            default=10000,
            help="In '--create-dict=partial' mode how much rows to scan",
        )
        return parser


async def make_init(ctx):
    result = PgAnonResult()
    ctx.logger.info("-------------> Started init mode")
    db_conn = await asyncpg.connect(**ctx.conn_params)

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
            else:
                log_file = "%s_%s.log" % (
                    str(self.args.mode),
                    str(
                        os.path.splitext(os.path.basename(self.args.dict_file))[0]
                        if self.args.dict_file != ""
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

    def __del__(self):
        self.close_logger_handlers()

    def __init__(self, external_args=None):
        self.current_dir = os.path.dirname(os.path.realpath(__file__))
        if external_args is not None:
            self.args = external_args
        else:
            self.args = Context.get_arg_parser().parse_args()
        self.setup_logger()
        self.ctx = Context(self.args)
        self.ctx.logger = self.logger

    async def run(self) -> PgAnonResult:
        if self.args.version:
            self.ctx.logger.info("Version %s" % PG_ANON_VERSION)
            sys.exit(0)

        self.ctx.logger.info(
            "============ %s version %s ============"
            % (os.path.basename(__file__), PG_ANON_VERSION)
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
                if self.ctx.args.mode != AnonMode.SYNC_STRUCT_RESTORE:
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
