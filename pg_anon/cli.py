import argparse
import asyncio
import sys
import uuid
from datetime import datetime
from typing import Optional, List

from pg_anon import PgAnonApp
from pg_anon.common.constants import RUNS_BASE_DIR
from pg_anon.common.dto import PgAnonResult, RunOptions
from pg_anon.common.enums import AnonMode, VerboseOptions, ScanMode, ResultCode
from pg_anon.common.utils import parse_comma_separated_list
from pg_anon.version import __version__


def get_arg_parser():
    parser = argparse.ArgumentParser()
    clean_db_args_group = parser.add_mutually_exclusive_group()

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
        "--config",
        help="Path to configuration file of pg_anon in YAML",
        default=None,
    )
    parser.add_argument("--db-host", type=str)
    parser.add_argument("--db-port", type=int, default=5432)
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
    parser.add_argument(
        "--meta-dict-file",
        dest='meta_dict_files',
        type=parse_comma_separated_list,
        default=None,
        help="In 'create-dict' mode input file or file list with scan rules of sensitive and not sensitive fields"
    )
    parser.add_argument(
        "--db-connections-per-process",
        type=int,
        default=4,
        help="Amount of db connections for each process for IO operations.",
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
        "--dbg-stage-1-validate-dict",
        action="store_true",
        default=False,
        help="""Validate dictionary, show the tables and run SQL queries without data export""",
    )
    parser.add_argument(
        "--dbg-stage-2-validate-data",
        action="store_true",
        default=False,
        help="""Validate data, show the tables and run SQL queries with data export in prepared database""",
    )
    parser.add_argument(
        "--dbg-stage-3-validate-full",
        action="store_true",
        default=False,
        help="""Makes all logic with "limit" in SQL queries""",
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
    clean_db_args_group.add_argument(
        "--clean-db",
        help="Clean database objects before restore (if they exist in dump). Mutually exclusive with --drop-db.",
        action="store_true",
        default=False,
    )
    clean_db_args_group.add_argument(
        "--drop-db",
        help="Drop target database before restore. Mutually exclusive with --clean-db.",
        action="store_true",
        default=False,
    )
    parser.add_argument(
        "--ignore-privileges",
        help="Ignore privileges from source db",
        action="store_true",
        default=False,
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
        help="In 'create-dict' mode defines whether to scan all data or only part of it",
    )
    parser.add_argument(
        "--output-sens-dict-file",
        type=str,
        default="output-sens-dict-file.py",
        help="In 'create-dict' mode output file with sensitive fields will be saved to this value",
    )
    parser.add_argument(
        "--output-no-sens-dict-file",
        type=str,
        help="In 'create-dict' mode output file with not sensitive fields will be saved to this value",
    )
    parser.add_argument(
        "--prepared-sens-dict-file",
        dest='prepared_sens_dict_files',
        type=parse_comma_separated_list,
        help="In 'create-dict' mode input file or file list with sensitive fields, which was obtained in previous use by option `--output-sens-dict-file` or prepared manually",
    )
    parser.add_argument(
        "--prepared-no-sens-dict-file",
        dest="prepared_no_sens_dict_files",
        type=parse_comma_separated_list,
        help="In 'create-dict' mode input file or file list with not sensitive fields, which was obtained in previous use by option `--output-no-sens-dict-file` or prepared manually",
    )
    parser.add_argument(
        "--partial-tables-dict-file",
        dest="partial_tables_dict_files",
        type=parse_comma_separated_list,
        help="In 'dump' or 'restore' mode input file or file list with tables. Only the tables specified in this dictionary will be included.",
    )
    parser.add_argument(
        "--partial-tables-exclude-dict-file",
        dest="partial_tables_exclude_dict_files",
        type=parse_comma_separated_list,
        help="In 'dump' or 'restore' mode input file or file list with tables. Only the tables specified in this dictionary will be excluded.",
    )
    parser.add_argument(
        "--scan-partial-rows",
        type=int,
        default=10000,
        help="In '--scan-mode=partial' sets how much rows to scan",
    )
    parser.add_argument(
        "--view-only-sensitive-fields",
        action="store_true",
        default=False,
        help="In 'view-fields' mode output only sensitive fields. By default output all db fields",
    )
    parser.add_argument(
        "--schema-name",
        type=str,
        help="In 'view-fields' and 'view-data' modes filter fields by schema name.",
    )
    parser.add_argument(
        "--schema-mask",
        type=str,
        help="In 'view-fields' mode filter fields by schema mask. By default output all db fields",
    )
    parser.add_argument(
        "--table-name",
        type=str,
        help="In 'view-fields' and 'view-data' modes filter fields by table name.",
    )
    parser.add_argument(
        "--table-mask",
        type=str,
        help="In 'view-fields' mode filter fields by table mask. By default output all db fields",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        default=False,
        help="In 'view-fields' mode output in JSON format. By default using table output",
    )
    parser.add_argument(
        "--save-dicts",
        action="store_true",
        default=False,
        help="Saves all input and output dictionaries to dir `runs` in modes: 'create-dict', 'dump', 'restore'",
    )
    parser.add_argument(
        "--fields-count",
        type=int,
        default=5000,
        help="In 'view-fields' mode specify how many fields will be processed for output. By default = 5000",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="In 'view-data' mode how much rows to display. By default = 100",
    )
    parser.add_argument(
        "--offset",
        type=int,
        default=0,
        help="In 'view-data' mode which part of --limit rows will be displayed. By default = 0",
    )
    parser.add_argument(
        "--application-name-suffix",
        type=str,
        default=None,
        required=False,
        help="Appends suffix for connection name. Just for comfortable automation",
    )

    return parser


def build_run_options(cli_run_params: Optional[List[str]] = None) -> RunOptions:
    args_parsed = get_arg_parser().parse_args(cli_run_params)
    args_dict = vars(args_parsed)

    if args_dict.get("debug") or args_dict.get("verbose") == VerboseOptions.DEBUG:
        args_dict["debug"] = True
        args_dict["verbose"] = VerboseOptions.DEBUG

    internal_operation_id = str(uuid.uuid4())
    start_date = datetime.today()
    run_dir = str(
        RUNS_BASE_DIR /
        str(start_date.year) /
        str(start_date.month) /
        str(start_date.day) /
        internal_operation_id
    )

    args_dict.update({
        'pg_anon_version': __version__,
        'internal_operation_id': internal_operation_id,
        'run_dir': run_dir,
    })
    return RunOptions(**args_dict)


async def run_pg_anon(cli_run_params: Optional[List[str]] = None) -> PgAnonResult:
    """
    Run pg_anon
    :param cli_run_params: list of params in command line format
    :return: result of pg_anon
    """
    options = build_run_options(cli_run_params)

    if options.version:
        print("Version %s" % options.pg_anon_version)
        sys.exit(0)

    result = await PgAnonApp(options).run()
    return result


def main(argv=None):
    result = asyncio.run(run_pg_anon(argv))
    if result.result_code == ResultCode.FAIL:
        sys.exit(1)
