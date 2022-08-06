import argparse
from enum import Enum
import logging
from dump import *
from restore import *


PG_ANON_VERSION = '1.0'


class BasicEnum():
    def __str__(self):
        return self.value


class OutputFormat(BasicEnum, Enum):
    BINARY = 'binary'
    TEXT = 'text'


class AnonMode(BasicEnum, Enum):
    DUMP = 'dump'
    RESTORE = 'restore'
    INIT = 'init'


class VerboseOptions(BasicEnum, Enum):
    INFO = 'info'
    DEBUG = 'debug'
    ERROR = 'error'


class Context:
    def __init__(self, args):
        self.current_dir = os.path.dirname(os.path.realpath(__file__))
        self.args = args
        self.pg_version = None
        self.validate_limit = " limit 100 "
        self.dictionary_content = None  # for dump process
        self.metadata = None            # for restore process
        self.task_results = {}          # for dump process (key is hash() of SQL query)

        if args.verbose == VerboseOptions.INFO:
            log_level = logging.INFO
        if args.verbose == VerboseOptions.DEBUG:
            log_level = logging.DEBUG
        if args.verbose == VerboseOptions.ERROR:
            log_level = logging.ERROR

        self.logger = logging.getLogger()
        if not len(self.logger.handlers):
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                datefmt='%Y-%m-%d %H:%M:%S',
                fmt="%(asctime)s,%(msecs)03d %(levelname)8s %(lineno)3d - %(message)s"
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(log_level)

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
            default=AnonMode.DUMP
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
            default=False
        )
        parser.add_argument(
            "--validate-full",
            action='store_true',
            default=False
        )
        parser.add_argument(
            "--clear-output-dir",
            action='store_true',
            default=False
        )
        return parser


async def make_init(ctx):
    ctx.logger.info("-------------> Started init mode")
    db_conn = await asyncpg.connect(**ctx.conn_params)

    tr = db_conn.transaction()
    await tr.start()
    try:
        with open(os.path.join(ctx.current_dir, 'init.sql'), 'r') as f:
            data = f.read()
        await db_conn.execute(data)
    except:
        await tr.rollback()
        await db_conn.close()
        raise
    else:
        await tr.commit()
        await db_conn.close()
    ctx.logger.info("<------------- Finished init mode")


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

        dt = datetime.now().isoformat(' ')
        if ctx.args.debug:
            ctx.logger.debug('%s %s started' % (dt, os.path.basename(__file__)))
            ctx.logger.debug("#--------------- Incoming parameters")
            for arg in vars(args):
                ctx.logger.debug("#   %s = %s" % (arg, getattr(args, arg)))
            ctx.logger.debug("#-----------------------------------")

        if args.version:
            ctx.logger.debug("Version %s" % PG_ANON_VERSION)
            sys.exit(0)

        db_conn = await asyncpg.connect(**ctx.conn_params)
        ctx.pg_version = await db_conn.fetchval("select version()")
        ctx.pg_version = re.findall(r"(\d+\.\d+)", str(ctx.pg_version))[0]
        await db_conn.close()

        result = PgAnonResult()
        try:
            if ctx.args.mode == AnonMode.DUMP:
                await make_dump(ctx)
            elif ctx.args.mode == AnonMode.RESTORE:
                await make_restore(ctx)
            elif ctx.args.mode == AnonMode.INIT:
                await make_init(ctx)
            else:
                raise Exception("Unknown mode: " + ctx.args.mode)

            ctx.logger.info("==============================================================")
            ctx.logger.info("Done")
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            msg = ""
            for v in traceback.format_exception(exc_type, exc_value, exc_traceback):
                msg += str(v) + '\n'
            ctx.logger.error(msg)
            # sys.exit(-1)
        finally:
            return result


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(MainRoutine().run())
