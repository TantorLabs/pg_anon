import argparse
import logging
from logging.handlers import RotatingFileHandler
from dump import *
from restore import *
from create_dict import *


PG_ANON_VERSION = '22.9.16'     # year month day


class BasicEnum():
    def __str__(self):
        return self.value


class OutputFormat(BasicEnum, Enum):
    BINARY = 'binary'
    TEXT = 'text'


class Context:
    def close_logger(self):
        if len(self.logger.handlers) > 0:
            for handler in self.logger.handlers:
                try:
                    handler.acquire()
                    handler.flush()
                    handler.close()
                except (OSError, ValueError):
                    pass
                finally:
                    handler.release()
                self.logger.removeHandler(handler)

    @exception_handler
    def __init__(self, args):
        self.current_dir = os.path.dirname(os.path.realpath(__file__))
        self.args = args
        self.pg_version = None
        self.validate_limit = "LIMIT 100"
        self.dictionary_content = None  # for dump process
        self.dictionary_obj = {}
        self.metadata = None            # for restore process
        self.task_results = {}          # for dump process (key is hash() of SQL query)
        self.total_rows = 0

        if args.verbose == VerboseOptions.INFO:
            log_level = logging.INFO
        if args.verbose == VerboseOptions.DEBUG:
            log_level = logging.DEBUG
        if args.verbose == VerboseOptions.ERROR:
            log_level = logging.ERROR

        self.logger = logging.getLogger(os.path.basename(__file__))
        self.close_logger()

        if not len(self.logger.handlers):
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                datefmt='%Y-%m-%d %H:%M:%S',
                fmt="%(asctime)s,%(msecs)03d %(levelname)8s %(lineno)3d - %(message)s"
            )
            handler.setFormatter(formatter)

            if not os.path.exists(os.path.join(self.current_dir, 'log')):
                os.makedirs(os.path.join(self.current_dir, 'log'))

            if args.mode == AnonMode.INIT:
                log_file = str(args.mode) + ".log"
            else:
                log_file = "%s_%s.log" % (
                        str(args.mode),
                        str(
                            os.path.splitext(os.path.basename(args.dict_file))[0]
                            if args.dict_file != '' else args.input_dir
                        )
                )

            f_handler = RotatingFileHandler(
                os.path.join(self.current_dir, 'log', log_file),
                maxBytes=1024 * 1000,
                backupCount=10
            )
            f_handler.setFormatter(formatter)

            self.logger.addHandler(handler)
            self.logger.addHandler(f_handler)
            self.logger.setLevel(log_level)

        if args.db_user_password == '' and os.environ.get('PGPASSWORD') is not None:
            args.db_user_password = os.environ["PGPASSWORD"]

        if args.db_certfile == '' and args.db_keyfile == '':
            self.conn_params = {
                "host": args.db_host,
                "database": args.db_name,
                "port": args.db_port,
                "user": args.db_user,
                "password": args.db_user_password
            }
        if args.db_certfile != '' and args.db_keyfile != '':
            self.conn_params = {
                "host": args.db_host,
                "database": args.db_name,
                "port": args.db_port,
                "user": args.db_user,
                "password": args.db_user_password,
                "keyfile": args.db_keyfile,
                "certfile": args.db_certfile
            }

    def __del__(self):
        self.close_logger()

    @staticmethod
    def get_arg_parser():
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--version",
            help="Show the version number and exit",
            action='store_true',
            default=False
        )
        parser.add_argument(
            "--debug",
            help="Enable debug mode, (default: %(default)s)",
            action='store_true',
            default=False
        )
        parser.add_argument(
            "--db-host",
            type=str,
        )
        parser.add_argument(
            "--db-port",
            type=str,
            default='5432'
        )
        parser.add_argument(
            "--db-name",
            type=str,
            default='default'
        )
        parser.add_argument(
            "--db-user",
            type=str,
            default='default'
        )
        parser.add_argument(
            "--db-user-password",
            type=str,
            default=''
        )
        parser.add_argument(
            "--db-keyfile",
            type=str,
            default=''
        )
        parser.add_argument(
            "--db-certfile",
            type=str,
            default=''
        )
        parser.add_argument(
            "--mode",
            type=AnonMode,
            choices=list(AnonMode),
            default=AnonMode.INIT
        )
        parser.add_argument(
            "--copy-options",
            dest="copy_options",
            default="",
            help="Options for COPY command like \"with binary\"."
        )
        parser.add_argument(
            "--format",
            dest="format",
            type=OutputFormat,
            choices=list(OutputFormat),
            default=OutputFormat.BINARY.value,
            help="COPY data format, can be overwritten by --copy-options. Selects the data format to be read or written: text, csv or binary."
        )
        parser.add_argument(
            "--verbose",
            dest="verbose",
            type=VerboseOptions,
            choices=list(VerboseOptions),
            default=VerboseOptions.INFO,
            help="Enable verbose output"
        )
        parser.add_argument(
            "--dict-file",
            type=str,
            default=''
        )
        parser.add_argument(
            "--threads",
            type=int,
            default=4
        )
        parser.add_argument(
            "--pg-dump",
            type=str,
            default='/usr/bin/pg_dump'
        )
        parser.add_argument(
            "--pg-restore",
            type=str,
            default='/usr/bin/pg_restore'
        )
        parser.add_argument(
            "--output-dir",
            type=str,
            default=''
        )
        parser.add_argument(
            "--input-dir",
            type=str,
            default=''
        )
        parser.add_argument(
            "--validate-dict",
            action='store_true',
            default=False,
            help="""Validate dictionary, show the tables and run SQL queries without data export"""
        )
        parser.add_argument(
            "--validate-full",
            action='store_true',
            default=False,
            help="""Same as "--validate-dict" + data export with limit"""
        )
        parser.add_argument(
            "--clear-output-dir",
            action='store_true',
            default=False
        )
        parser.add_argument(
            "--drop-custom-check-constr",
            action='store_true',
            default=False
        )
        parser.add_argument(
            "--seq-init-by-max-value",
            action='store_true',
            default=False,
            help="""Initialize sequences based on maximum values. Otherwise, the sequences will be initialized
                based on the values of the source database."""
        )
        parser.add_argument(
            "--disable-checks",
            action='store_true',
            default=False,
            help="""Disable checks of disk space and PostgreSQL version"""
        )
        parser.add_argument(
            "--skip-data",
            action='store_true',
            default=False,
            help="""Don't copy data. Only the database structure will be dumped"""
        )
        return parser


async def make_init(ctx):
    result = PgAnonResult()
    ctx.logger.info("-------------> Started init mode")
    db_conn = await asyncpg.connect(**ctx.conn_params)

    tr = db_conn.transaction()
    await tr.start()
    try:
        with open(os.path.join(ctx.current_dir, 'init.sql'), 'r') as f:
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

    def __init__(self, external_args=None):
        self.external_args = external_args

    async def run(self) -> PgAnonResult:
        if self.external_args is not None:
            args = self.external_args
        else:
            args = Context.get_arg_parser().parse_args()
        ctx = Context(args)

        if args.version:
            ctx.logger.info("Version %s" % PG_ANON_VERSION)
            sys.exit(0)

        ctx.logger.info("============ %s version %s ============" % (os.path.basename(__file__), PG_ANON_VERSION))
        ctx.logger.info("============> Started MainRoutine.run in mode: %s" % ctx.args.mode)
        if ctx.args.debug:
            params_info = "#--------------- Incoming parameters\n"
            for arg in vars(args):
                if arg not in ("db_user_password"):
                    params_info += "#   %s = %s\n" % (arg, getattr(args, arg))
            params_info += "#-----------------------------------"
            ctx.logger.info(params_info)

        db_conn = await asyncpg.connect(**ctx.conn_params)
        ctx.pg_version = await db_conn.fetchval("select version()")
        ctx.pg_version = re.findall(r"(\d+\.\d+)", str(ctx.pg_version))[0]
        await db_conn.close()

        result = PgAnonResult()
        try:
            if ctx.args.mode in (AnonMode.DUMP, AnonMode.SYNC_DATA_DUMP, AnonMode.SYNC_STRUCT_DUMP):
                result = await make_dump(ctx)
            elif ctx.args.mode in (AnonMode.RESTORE, AnonMode.SYNC_DATA_RESTORE, AnonMode.SYNC_STRUCT_RESTORE):
                result = await make_restore(ctx)
                if ctx.args.mode != AnonMode.SYNC_STRUCT_RESTORE:
                    await run_analyze(ctx)
            elif ctx.args.mode == AnonMode.INIT:
                result = await make_init(ctx)
            elif ctx.args.mode == AnonMode.CREATE_DICT:
                result = await create_dict(ctx)
            else:
                raise Exception("Unknown mode: " + ctx.args.mode)

            ctx.logger.info("MainRoutine.run result_code = %s" % result.result_code)
        except:
            ctx.logger.error(exception_helper(show_traceback=True))
        finally:
            ctx.logger.info("<============ Finished MainRoutine.run in mode: %s" % ctx.args.mode)
            return result

    async def validate_target_tables(self) -> PgAnonResult:
        if self.external_args is not None:
            args = self.external_args
        else:
            args = Context.get_arg_parser().parse_args()
        ctx = Context(args)

        result = PgAnonResult()
        try:
            result = await validate_restore(ctx)
        except:
            ctx.logger.error(exception_helper(show_traceback=True))
        finally:
            return result


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(MainRoutine().run())
