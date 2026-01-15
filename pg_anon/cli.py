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


def common_parser():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument(
        "--db-host",
        type=str,
        required=True,
        help="""Database host""",
    )
    parser.add_argument(
        "--db-port",
        type=int,
        default=5432,
        help="""Database port""",
    )
    parser.add_argument(
        "--db-name",
        type=str,
        required=True,
        help="""Database name""",
    )
    parser.add_argument(
        "--db-user",
        type=str,
        required=True,
        help="""Database user""",
    )
    parser.add_argument(
        "--db-user-password",
        type=str,
        default="",
        help="""Database user password""",
    )
    parser.add_argument(
        "--db-passfile",
        type=str,
        default="",
        help="""Path to a file containing the password used for authentication""",
    )
    parser.add_argument(
        "--db-ssl-key-file",
        type=str,
        default="",
        help="""Path to the client SSL key file for secure connections""",
    )
    parser.add_argument(
        "--db-ssl-cert-file",
        type=str,
        default="",
        help="""Path to the client SSL certificate file""",
    )
    parser.add_argument(
        "--db-ssl-ca-file",
        type=str,
        default="",
        help="""Path to the CA certificate used to verify the server’s certificate""",
    )
    parser.add_argument(
        "--config",
        help="""Path to configuration file of pg_anon in YAML""",
        type=str,
        default="",
    )
    parser.add_argument(
        "--version",
        help="""Show the version number and exit""",
        action="store_true",
    )
    parser.add_argument(
        "--verbose",
        dest="verbose",
        choices=list(v.value for v in VerboseOptions),
        default=VerboseOptions.INFO.value,
        help="""Sets the log verbosity level: "info", "debug", "error". (default: %(default)s)""",
    )
    parser.add_argument(
        "--debug",
        help="""Enables debug mode (equivalent to "--verbose=debug") and adds extra debug logs.""",
        action="store_true",
    )
    parser.add_argument(
        "--application-name-suffix",
        type=str,
        default="",
        help="""Appends suffix for connection name. Just for comfortable automation.""",
    )
    return parser


# Scan, Dump, Restore, View-data, View-fields
def io_common_parser():
    p = argparse.ArgumentParser(add_help=False)
    p.add_argument(
        "--db-connections-per-process",
        type=int,
        default=4,
        help="""Number of database connections per process for I/O operations. (default: %(default)s)""",
    )
    p.add_argument(
        "--processes",
        type=int,
        default=4,
        help="""Number of processes used for multiprocessing operations. (default: %(default)s)""",
    )

    return p


def scan_parser():
    p = argparse.ArgumentParser(add_help=False)

    p.add_argument(
        "--meta-dict-file",
        dest="meta_dict_files",
        type=parse_comma_separated_list,
        required=True,
        help="Input file or file list contains meta-dictionary, which was prepared manually. In rules collision case, priority has rules in last file from the list."
    )
    p.add_argument(
        "--prepared-sens-dict-file",
        dest="prepared_sens_dict_files",
        type=parse_comma_separated_list,
        help="""Input file or file list contains sensitive dictionary, which was obtained in previous use by option "--output-sens-dict-file" or prepared manually. In rules collision case, priority has rules in last file from the list.""",
    )
    p.add_argument(
        "--prepared-no-sens-dict-file",
        dest="prepared_no_sens_dict_files",
        type=parse_comma_separated_list,
        help="""Input file or file list contains not sensitive dictionary, which was obtained in previous use by option "--output-no-sens-dict-file" or prepared manually. In rules collision case, priority has rules in last file from the list.""",
    )

    p.add_argument(
        "--output-sens-dict-file",
        type=str,
        required=True,
        help="""Output file path for saving sensitive dictionary.""",
    )
    p.add_argument(
        "--output-no-sens-dict-file",
        type=str,
        default="",
        help="""Output file path for saving not sensitive dictionary.""",
    )

    p.add_argument(
        "--scan-mode",
        choices=list(v.value for v in ScanMode),
        default=ScanMode.PARTIAL.value,
        help="""Defines whether to scan all data or only part of it ["full", "partial"] (default: %(default)s)""",
    )
    p.add_argument(
        "--scan-partial-rows",
        type=int,
        default=10000,
        help="""In "--scan-mode=partial" defines amount of rows to scan (default: %(default)s). Actual rows count can be smaller after getting unique values.""",
    )

    p.add_argument(
        "--save-dicts",
        action="store_true",
        help="""Duplicate all input and output dictionaries to dir "runs". It can be useful for debugging or integration purposes.""",
    )

    return p


def dump_parser():
    p = argparse.ArgumentParser(add_help=False)
    p.add_argument(
        "--prepared-sens-dict-file",
        dest="prepared_sens_dict_files",
        type=parse_comma_separated_list,
        required=True,
        help="""Input file or file list contains sensitive dictionary, which was generated by the create-dict (scan) mode or created manually. In rules collision case, priority has rules in last file from the list.""",
    )
    p.add_argument(
        "--partial-tables-dict-file",
        dest="partial_tables_dict_files",
        type=parse_comma_separated_list,
        help="""Input file or file list contains tables dictionary for include specific tables in the dump. All tables not listed in these files will be excluded. These files must be prepared manually (acts as a whitelist).""",
    )
    p.add_argument(
        "--partial-tables-exclude-dict-file",
        dest="partial_tables_exclude_dict_files",
        type=parse_comma_separated_list,
        help="""Input file or file list contains tables dictionary for exclude specific tables from the dump. All tables listed in these files will be excluded. These files must be prepared manually (acts as a blacklist).""",
    )
    p.add_argument(
        "--dbg-stage-1-validate-dict",
        action="store_true",
        help="""Validate dictionary, show the tables and run SQL queries without data export.""",
    )
    p.add_argument(
        "--dbg-stage-2-validate-data",
        action="store_true",
        help="""Validate data, show the tables and run SQL queries with data export in prepared database.""",
    )
    p.add_argument(
        "--dbg-stage-3-validate-full",
        action="store_true",
        help="""Makes all logic with "limit" in SQL queries.""",
    )
    p.add_argument(
        "--clear-output-dir",
        action="store_true",
        help="""Clears the output directory from previous dumps or other files.""",
    )
    p.add_argument(
        "--pg-dump",
        type=str,
        default="/usr/bin/pg_dump",
        help="""Path to the pg_dump Postgres tool (default: %(default)s).""",
    )
    p.add_argument(
        "--output-dir",
        type=str,
        default="",
        help="""Output directory for dump files.""",
    )
    p.add_argument(
        "--save-dicts",
        action="store_true",
        help="""Duplicate all input dictionaries to dir "runs". It can be useful for debugging or integration purposes.""",
    )

    return p


def restore_parser():
    p = argparse.ArgumentParser(add_help=False)

    p.add_argument(
        "--input-dir",
        type=str,
        required=True,
        help="""Path to the directory containing dump files created in dump mode. """,
    )
    p.add_argument(
        "--partial-tables-dict-file",
        dest="partial_tables_dict_files",
        type=parse_comma_separated_list,
        help="""Input file or file list contains tables dictionary for include specific tables in the dump. All tables not listed in these files will be excluded. These files must be prepared manually (acts as a whitelist).""",
    )
    p.add_argument(
        "--partial-tables-exclude-dict-file",
        dest="partial_tables_exclude_dict_files",
        type=parse_comma_separated_list,
        help="""Input file or file list contains tables dictionary for exclude specific tables from the dump. All tables listed in these files will be excluded. These files must be prepared manually (acts as a blacklist).""",
    )
    p.add_argument(
        "--disable-checks",
        action="store_true",
        help="""Disable checks of disk space and PostgreSQL version.""",
    )
    p.add_argument(
        "--seq-init-by-max-value",
        action="store_true",
        help="""Initialize sequences based on maximum values. Otherwise, the sequences will be initialized based on the values of the source database. """,
    )
    p.add_argument(
        "--drop-custom-check-constr",
        action="store_true",
        help="""Drops all CHECK constraints that contain user-defined procedures to avoid performance degradation during data loading. """,
    )
    p.add_argument(
        "--pg-restore",
        type=str,
        default="/usr/bin/pg_restore",
        help="""Path to the pg_restore Postgres tool. """,
    )
    group = p.add_mutually_exclusive_group()
    group.add_argument(
        "--clean-db",
        action="store_true",
        help="""Cleans the database objects before restoring (if they exist in the dump). Mutually exclusive with "--drop-db".""",
    )
    group.add_argument(
        "--drop-db",
        action="store_true",
        help="""Drop target database before restore. Mutually exclusive with "--clean-db".""",
    )
    p.add_argument(
        "--save-dicts",
        action="store_true",
        help="""Duplicate all input dictionaries to dir "runs". It can be useful for debugging or integration purposes. """,
    )

    # Hidden param for validate_target_tables() in tests
    p.add_argument(
        "--prepared-sens-dict-file",
        dest="prepared_sens_dict_files",
        type=parse_comma_separated_list,
        help=argparse.SUPPRESS,
    )

    return p


def view_fields_parser():
    p = argparse.ArgumentParser(add_help=False)

    p.add_argument(
        "--prepared-sens-dict-file",
        dest="prepared_sens_dict_files",
        type=parse_comma_separated_list,
        required=True,
        help="""Input file or file list contains sensitive dictionary, which was generated by the create-dict (scan) mode or created manually. In rules collision case, priority has rules in last file from the list.""",
    )
    p.add_argument(
        "--view-only-sensitive-fields",
        action="store_true",
        help="""Displays only sensitive fields.""",
    )
    p.add_argument(
        "--fields-count",
        type=int,
        default=5000,
        help="""Maximum number of fields to process for output. (default: %(default)s)""",
    )
    p.add_argument(
        "--schema-name",
        type=str,
        default="",
        help="""Filter by schema name.""",
    )
    p.add_argument(
        "--schema-mask",
        type=str,
        default="",
        help="""Filter by schema name using a regular expression.""",
    )
    p.add_argument(
        "--table-name",
        type=str,
        default="",
        help="""Filter by table name.""",
    )
    p.add_argument(
        "--table-mask",
        type=str,
        default="",
        help="""Filter by table name using a regular expression.""",
    )
    p.add_argument(
        "--json",
        action="store_true",
        help="""Outputs results in JSON format instead of a table.""",
    )

    return p


def view_data_parser():
    p = argparse.ArgumentParser(add_help=False)

    p.add_argument(
        "--prepared-sens-dict-file",
        dest="prepared_sens_dict_files",
        type=parse_comma_separated_list,
        required=True,
        help="""Input file or file list contains sensitive dictionary, which was generated by the create-dict (scan) mode or created manually. In rules collision case, priority has rules in last file from the list.""",
    )
    p.add_argument(
        "--schema-name",
        type=str,
        required=True,
        help="""Schema name.""",
    )
    p.add_argument(
        "--table-name",
        type=str,
        required=True,
        help="""Table name.""",
    )
    p.add_argument(
        "--limit",
        type=int,
        default=100,
        help="""Number of rows to display. (default: %(default)s)""",
    )
    p.add_argument(
        "--offset",
        type=int,
        default=0,
        help="""Row offset for pagination. (default: %(default)s)""",
    )
    p.add_argument(
        "--json",
        action="store_true",
        help="""Outputs results in JSON format instead of a table.""",
    )

    return p


def get_arg_parser():
    parser = argparse.ArgumentParser(
        prog="pg_anon",
        description="PostgreSQL database anonymization tool",
    )

    sub = parser.add_subparsers(dest="mode", help="Work mode", required=True)

    sub.add_parser(
        "init",
        parents=[common_parser()],
        help="""Creates the "anon_funcs" schema in the source database and loads the predefined SQL functions.""",
    )

    sub.add_parser(
        "create-dict",
        parents=[common_parser(), io_common_parser(), scan_parser()],
        help="""Analyzes PostgreSQL database to detect potentially sensitive data and generate dictionaries files""",
    )

    # Dump modes
    for mode_name, help_text in [
        ("dump", "Creates an anonymized backup using rules from the sensitive dictionary."),
        ("sync-struct-dump", "Creates a backup containing only the database structure without anonymized data."),
        ("sync-data-dump", "Create backup contains only anonymized data without database structure."),
    ]:
        sub.add_parser(
            mode_name,
            parents=[common_parser(), io_common_parser(), dump_parser()],
            help=help_text,
        )

    # Restore modes
    for mode_name, help_text in [
        ("restore", "Restores an anonymized backup created using pg_anon in the dump mode."),
        ("sync-struct-restore", "Restores only the database structure."),
        ("sync-data-restore", "Restores data only from anonymized backup."),
    ]:
        sub.add_parser(
            mode_name,
            parents=[common_parser(), io_common_parser(), restore_parser()],
            help=help_text,
        )

    sub.add_parser(
        "view-fields",
        parents=[common_parser(), view_fields_parser()],
        help="""Displays how database fields match the anonymization rules.""",
    )

    sub.add_parser(
        "view-data",
        parents=[common_parser(), view_data_parser()],
        help="""Displays anonymized table data without creating a dump.""",
    )

    # backward compatibility
    parser.add_argument("--mode", help=argparse.SUPPRESS)  # hidden in help
    return parser


def normalize_legacy_mode_args(argv: list[str]) -> list[str]:
    if "--mode" not in argv and not any(a.startswith("--mode=") for a in argv):
        return argv

    new_argv = []
    mode = None
    skip_next = False

    for i, arg in enumerate(argv):
        if skip_next:
            skip_next = False
            continue

        if arg == "--mode":
            mode = argv[i + 1]
            skip_next = True
        elif arg.startswith("--mode="):
            mode = arg.split("=", 1)[1]
        else:
            new_argv.append(arg)

    if mode:
        return [mode] + new_argv

    return argv


def build_run_options(cli_run_params: Optional[List[str]] = None) -> RunOptions:
    if cli_run_params is None:
        cli_run_params = sys.argv[1:]

    # Handle --version before subcommand parsing
    if "--version" in cli_run_params:
        print("Version %s" % __version__)
        sys.exit(0)

    cli_run_params = normalize_legacy_mode_args(cli_run_params)

    parser = get_arg_parser()
    args_parsed = parser.parse_args(cli_run_params)
    args_dict = vars(args_parsed)

    if args_dict.get("debug") or args_dict.get("verbose") == VerboseOptions.DEBUG.value:
        args_dict["debug"] = True
        args_dict["verbose"] = VerboseOptions.DEBUG.value

    if args_dict.get('scan_mode'):
        args_dict['scan_mode'] = ScanMode(args_dict['scan_mode'])

    if args_dict.get('verbose'):
        args_dict['verbose'] = VerboseOptions(args_dict['verbose'])

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
        'mode': AnonMode(args_dict['mode']),
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
