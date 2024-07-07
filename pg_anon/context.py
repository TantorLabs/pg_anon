import argparse
import os

from pg_anon.common import (
    exception_handler,
    AnonMode,
    VerboseOptions,
    ScanMode,
)


class Context:
    @exception_handler
    def __init__(self, args):
        self.current_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
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
        self.logger = None

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
            "--verbose",
            dest="verbose",
            type=VerboseOptions,
            choices=list(VerboseOptions),
            default=VerboseOptions.INFO,
            help="Enable verbose output",
        )
        parser.add_argument("--dict-file", type=str, default="")
        parser.add_argument(
            "--threads",
            type=int,
            default=4,
            help="Amount of threads for IO operations.",
        )
        parser.add_argument(
            "--processes",
            type=int,
            default=4,
            help="Amount of processes for multiprocessing operations.",
        )
        parser.add_argument(
            "--pg-dump",
            type=str,
            default="/usr/bin/pg_dump",
            help="Path to the `pg_dump` Postgres tool",
        )
        parser.add_argument(
            "--pg-restore",
            type=str,
            default="/usr/bin/pg_restore",
            help="Path to the `pg_dump` Postgres tool.",
        )
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
            "--drop-custom-check-constr",
            action="store_true",
            default=False,
            help="Drop all CHECK constrains containing user-defined procedures to avoid performance "
            "degradation at the data loading stage.",
        )
        parser.add_argument(
            "--seq-init-by-max-value",
            action="store_true",
            default=False,
            help="Initialize sequences based on maximum values. Otherwise, the sequences "
            "will be initialized based on the values of the source database.",
        )
        parser.add_argument(
            "--disable-checks",
            action="store_true",
            default=False,
            help="Disable checks of disk space and PostgreSQL version.",
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
