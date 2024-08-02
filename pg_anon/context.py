import argparse
import os
from typing import Dict, Optional

from pg_anon.common import (
    exception_handler,
    AnonMode,
    VerboseOptions,
    ScanMode, parse_comma_separated_list,
)


class Context:
    @exception_handler
    def __init__(self, args):
        self.current_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
        self.args = args
        self.pg_version = None
        self.validate_limit = "LIMIT 100"
        self.meta_dictionary_obj: Dict = {}
        self.prepared_dictionary_obj: Dict = {}
        self.prepared_dictionary_contents: Dict = {}  # for dump process
        self.metadata = None  # for restore process
        self.task_results = {}  # for dump process (key is hash() of SQL query)
        self.total_rows = 0
        self.create_dict_sens_matches = {}  # for create-dict mode
        self.create_dict_no_sens_matches = {}  # for create-dict mode
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

    def _check_meta_dict_types(self, meta_dict: Dict):
        """
        Checking expected types in meta dict fields
        """
        if not (
            isinstance(meta_dict["field"]["rules"], list) and
            isinstance(meta_dict["field"]["constants"], list) and
            isinstance(meta_dict["skip_rules"], list) and
            isinstance(meta_dict["data_regex"]["rules"], list) and
            isinstance(meta_dict["data_const"]["constants"], list) and
            isinstance(meta_dict["funcs"], dict) and
            isinstance(meta_dict["no_sens_dictionary"], list)
        ):
            raise ValueError('Meta dict does not have expected types')

    def _make_meta_dict(self, meta_dict_data: Optional[Dict] = None) -> dict:
        """
        Making meta dict in expected format, from meta dict data
        """
        result_dict = {
          "field": {
            "rules": meta_dict_data.get('field', {}).get('rules', []) if meta_dict_data else [],
            "constants": meta_dict_data.get('field', {}).get('constants', []) if meta_dict_data else []
          },
          "skip_rules": meta_dict_data.get('skip_rules', []) if meta_dict_data else [],
          "data_regex": {
            "rules": meta_dict_data.get('data_regex', {}).get('rules', []) if meta_dict_data else []
          },
          "data_const": {
            "constants": meta_dict_data.get('data_const', {}).get('constants', []) if meta_dict_data else []
          },
          "funcs": meta_dict_data.get('funcs', {}) if meta_dict_data else {},
          "no_sens_dictionary": meta_dict_data.get('no_sens_dictionary', []) if meta_dict_data else [],
        }

        return result_dict

    def _append_meta_dict(self, meta_dict):
        """
        Appending meta dict to existing meta dict
        """
        self._check_meta_dict_types(meta_dict)

        if meta_dict["field"]["rules"]:
            self.meta_dictionary_obj["field"]["rules"].extend(meta_dict["field"]["rules"])

        if meta_dict["field"]["constants"]:
            self.meta_dictionary_obj["field"]["constants"].extend(meta_dict["field"]["constants"])

        if meta_dict["skip_rules"]:
            self.meta_dictionary_obj["skip_rules"].extend(meta_dict["skip_rules"])

        if meta_dict["data_regex"]["rules"]:
            self.meta_dictionary_obj["data_regex"]["rules"].extend(meta_dict["data_regex"]["rules"])

        if meta_dict["data_const"]["constants"]:
            self.meta_dictionary_obj["data_const"]["constants"].extend(meta_dict["data_const"]["constants"])

        if meta_dict["funcs"]:
            self.meta_dictionary_obj["funcs"].update(meta_dict["funcs"])

        if meta_dict["no_sens_dictionary"]:
            self.meta_dictionary_obj["no_sens_dictionary"].extend(meta_dict["no_sens_dictionary"])

    def read_meta_dict(self):
        self.meta_dictionary_obj = self._make_meta_dict()

        dict_files_list = self.args.meta_dict_files

        if self.args.prepared_no_sens_dict_files:
            dict_files_list += self.args.prepared_no_sens_dict_files

        for meta_dict_file in dict_files_list:
            dictionary_file_name = os.path.join("dict", meta_dict_file)
            with open(os.path.join(self.current_dir, dictionary_file_name), "r") as dictionary_file:
                data = dictionary_file.read()

            if not data:
                continue

            meta_dict_data = eval(data)

            if not meta_dict_data:
                continue

            if not isinstance(meta_dict_data, dict):
                raise ValueError(f"Received non-dictionary structure from file: {dictionary_file_name}")

            prepared_meta_dict = self._make_meta_dict(meta_dict_data)
            self._append_meta_dict(prepared_meta_dict)

    def read_prepared_dict(self):
        self.prepared_dictionary_obj = {
            "dictionary": [],
            "dictionary_exclude": [],
            "validate_tables": [],
        }

        for dict_file in self.args.prepared_sens_dict_files:
            dictionary_file_name = os.path.join("dict", dict_file)
            with open(os.path.join(self.current_dir, dictionary_file_name), "r") as dictionary_file:
                data = dictionary_file.read()

            self.prepared_dictionary_contents = {dictionary_file_name: data}

            if not data:
                continue

            dict_data = eval(data)

            if not dict_data:
                continue

            if not isinstance(dict_data, dict):
                raise ValueError(f"Received non-dictionary structure from file: {dictionary_file_name}")

            if dictionary := dict_data.get("dictionary", []):
                self.prepared_dictionary_obj["dictionary"].extend(dictionary)

            if dictionary := dict_data.get("dictionary_exclude", []):
                self.prepared_dictionary_obj["dictionary_exclude"].extend(dictionary)

            if validate_tables := dict_data.get("validate_tables", []):
                self.prepared_dictionary_obj["validate_tables"].extend(validate_tables)

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
        parser.add_argument(
            "--meta-dict-file",
            dest='meta_dict_files',
            type=parse_comma_separated_list,
            default=None,
            help="In '--create-dict' mode input file or file list with scan rules of sensitive and not sensitive fields"
        )
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
            "--output-sens-dict-file",
            type=str,
            default="output-sens-dict-file.py",
            help="In '--create-dict' mode output file with sensitive fields will be saved to this value",
        )
        parser.add_argument(
            "--output-no-sens-dict-file",
            type=str,
            help="In '--create-dict' mode output file with not sensitive fields will be saved to this value",
        )
        parser.add_argument(
            "--prepared-sens-dict-file",
            dest='prepared_sens_dict_files',
            type=parse_comma_separated_list,
            help="In '--create-dict' mode input file or file list with sensitive fields, which was obtained in previous use by option `--output-sens-dict-file` or prepared manually",
        )
        parser.add_argument(
            "--prepared-no-sens-dict-file",
            dest="prepared_no_sens_dict_files",
            type=parse_comma_separated_list,
            help="In '--create-dict' mode input file or file list with not sensitive fields, which was obtained in previous use by option `--output-no-sens-dict-file` or prepared manually",
        )
        parser.add_argument(
            "--scan-partial-rows",
            type=int,
            default=10000,
            help="In '--create-dict=partial' mode how much rows to scan",
        )
        return parser
