import logging
import os
import re
from collections.abc import Callable
from pathlib import Path

from pg_anon.common.constants import (
    LOGS_DIR_NAME,
    LOGS_FILE_NAME,
    SENS_PG_TYPES,
    SERVER_SETTINGS,
    TRANSACTIONS_SERVER_SETTINGS,
)
from pg_anon.common.dto import ConnectionParams, RunOptions, SearchRule
from pg_anon.common.enums import AnonMode, VerboseOptions
from pg_anon.common.errors import ErrorCode, PgAnonError
from pg_anon.common.utils import (
    exception_handler,
    filter_db_tables,
    normalize_data_type,
    read_dict_data_from_file,
    read_yaml,
    safe_compile,
    split_constants_to_words_and_phrases,
)
from pg_anon.logger import get_logger, logger_add_file_handler, logger_set_log_level


def _parse_search_rules(items: object, section: str) -> list[SearchRule]:
    """Read a list of search rules: strings or objects with "rule" and "anon_func"."""
    if not isinstance(items, list | tuple | set):
        raise PgAnonError(ErrorCode.INVALID_META_DICT, f'Meta dict section "{section}" must be a list')

    rules = []
    for item in items:
        if isinstance(item, str):
            rules.append(SearchRule(item))
        elif (
            isinstance(item, dict)
            and item.keys() == {"rule", "anon_func"}
            and isinstance(item["rule"], str)
            and isinstance(item["anon_func"], str)
            and item["anon_func"].strip()
        ):
            rules.append(SearchRule(item["rule"], item["anon_func"]))
        else:
            raise PgAnonError(
                ErrorCode.INVALID_META_DICT,
                f'Meta dict section "{section}" has a wrong item: {item!r}. '
                'Use a string or an object {"rule": "<string>", "anon_func": "<string>"}',
            )
    return rules


def _next_rule(previous: SearchRule | None, rule: str | re.Pattern, anon_func: str | None) -> SearchRule:
    """Make a rule that goes right after the previous rule of the same list."""
    if previous is None:
        return SearchRule(rule, anon_func)

    other_func_above = previous.position if previous.anon_func != anon_func else previous.other_func_above
    return SearchRule(rule, anon_func, previous.position + 1, other_func_above)


def _extend_rules(target: list[SearchRule], rules: list[SearchRule], compile_flags: int | None = None) -> None:
    """Add rules to the end of the list; compile them as regular expressions when flags are given."""
    for rule in rules:
        value = rule.rule if compile_flags is None else safe_compile(rule.rule, compile_flags)
        target.append(_next_rule(target[-1] if target else None, value, rule.anon_func))


def _extend_constant_rules(target: dict[str, SearchRule], rules: dict[str, SearchRule]) -> None:
    """Add constants to the end of the ordered map."""
    last = next(reversed(target.values()), None)
    for key, rule in rules.items():
        # A repeated constant never wins over its first place
        if key not in target:
            last = _next_rule(last, key, rule.anon_func)
            target[key] = last


def _make_constant_rules(rules: list[SearchRule], normalize: Callable[[str], str]) -> dict[str, SearchRule]:
    """Map normalized constants to their rules; the first place of a constant wins."""
    result: dict[str, SearchRule] = {}
    for rule in rules:
        key = normalize(rule.rule)
        if key not in result:
            result[key] = SearchRule(key, rule.anon_func)
    return result


class Context:
    @exception_handler
    def __init__(self, options: RunOptions) -> None:
        self.options = options
        self.config = read_yaml(options.config) if options.config else None
        self.pg_version: str = ""
        self.pg_dump: str = options.pg_dump
        self.pg_restore: str = options.pg_restore
        self.validate_limit = "LIMIT 100"
        self.meta_dictionary_obj: dict = {}
        self.prepared_dictionary_obj: dict = {}
        self.metadata: dict | None = None  # for restore process
        self.task_results: dict = {}  # for dump process (key is hash() of SQL query)
        self.total_rows = 0
        self.create_dict_sens_matches: dict = {}  # for create-dict mode
        self.create_dict_no_sens_matches: dict = {}  # for create-dict mode
        self.exclude_schemas: list[str] = []
        self.included_tables_rules: list[dict] = []
        self.excluded_tables_rules: list[dict] = []
        self.tables: list[tuple[str, str]] = []
        self.black_listed_tables: set[tuple[str, str]] = set()
        self.white_listed_tables: set[tuple[str, str]] = set()
        self.extension_table_conditions: dict[tuple[str, str], str] = {}
        self.data_const_constants_min_length: int | None = None
        self.setup_logger()

        if not options.db_user_password:
            options.db_user_password = os.environ.get("PGPASSWORD")

        self.server_settings = SERVER_SETTINGS.copy()
        if self.options.application_name_suffix:
            self.server_settings["application_name"] += "_" + self.options.application_name_suffix

        self.connection_params = ConnectionParams(
            host=options.db_host,
            database=options.db_name,
            port=options.db_port,
            user=options.db_user,
            passfile=options.db_passfile,
            password=options.db_user_password,
            ssl_cert_file=options.db_ssl_cert_file,
            ssl_key_file=options.db_ssl_key_file,
            ssl_ca_file=options.db_ssl_ca_file,
        )

    def _check_meta_dict_types(self, meta_dict: dict) -> None:
        """Check expected types in meta dict fields."""
        if not (
            isinstance(meta_dict["field"]["rules"], list)
            and isinstance(meta_dict["field"]["constants"], list)
            and isinstance(meta_dict["skip_rules"], list)
            and isinstance(meta_dict["include_rules"], list)
            and isinstance(meta_dict["data_regex"]["rules"], list)
            and isinstance(meta_dict["data_const"]["constants"]["words"], set)
            and isinstance(meta_dict["data_const"]["constants"]["phrases"], set)
            and isinstance(meta_dict["data_const"]["constant_rules"], dict)
            and isinstance(meta_dict["data_const"]["partial_constant_rules"], dict)
            and isinstance(meta_dict["data_func"], dict)
            and isinstance(meta_dict["data_sql_condition"], list)
            and isinstance(meta_dict["sens_pg_types"], list)
            and isinstance(meta_dict["funcs"], dict)
            and isinstance(meta_dict["no_sens_dictionary"], list)
        ):
            raise PgAnonError(ErrorCode.INVALID_META_DICT, "Meta dict does not have expected types")

    def _make_meta_dict(self, meta_dict_data: dict | None = None) -> dict:
        """Make meta dict in expected format, from meta dict data."""
        data = meta_dict_data or {}
        field = data.get("field", {})
        data_const = data.get("data_const", {})

        constant_rules = _make_constant_rules(
            _parse_search_rules(data_const.get("constants", []), "data_const.constants"),
            lambda v: v.strip().lower(),
        )
        partial_constant_rules = _make_constant_rules(
            _parse_search_rules(data_const.get("partial_constants", []), "data_const.partial_constants"),
            str.lower,
        )
        constants_words, constants_phrases = split_constants_to_words_and_phrases(list(constant_rules))

        return {
            "field": {
                "rules": _parse_search_rules(field.get("rules", []), "field.rules"),
                "constants": _parse_search_rules(field.get("constants", []), "field.constants"),
            },
            "skip_rules": data.get("skip_rules", []),
            "include_rules": data.get("include_rules", []),
            "data_regex": {
                "rules": _parse_search_rules(data.get("data_regex", {}).get("rules", []), "data_regex.rules"),
            },
            "data_const": {
                "constants": {
                    "words": constants_words,
                    "phrases": constants_phrases,
                },
                # Order of constants for the search: constant -> rule with its position and anon_func
                "constant_rules": constant_rules,
                "partial_constant_rules": partial_constant_rules,
            },
            "data_func": data.get("data_func", {}),
            "data_sql_condition": data.get("data_sql_condition", []),
            "sens_pg_types": data.get("sens_pg_types", []),
            "funcs": data.get("funcs", {}),
            "no_sens_dictionary": data.get("no_sens_dictionary", []),
        }

    def _append_meta_dict(self, meta_dict: dict) -> None:  # noqa: C901, PLR0912
        """Append meta dict to existing meta dict."""
        self._check_meta_dict_types(meta_dict)

        _extend_rules(self.meta_dictionary_obj["field"]["rules"], meta_dict["field"]["rules"], compile_flags=0)
        _extend_rules(self.meta_dictionary_obj["field"]["constants"], meta_dict["field"]["constants"])

        if meta_dict["skip_rules"]:
            self.meta_dictionary_obj["skip_rules"].extend(meta_dict["skip_rules"])

        if meta_dict["include_rules"]:
            self.meta_dictionary_obj["include_rules"].extend(meta_dict["include_rules"])

        # re.DOTALL using for searching in text with \n
        _extend_rules(self.meta_dictionary_obj["data_regex"]["rules"], meta_dict["data_regex"]["rules"], re.DOTALL)

        if meta_dict["data_const"]["constants"]["words"]:
            self.meta_dictionary_obj["data_const"]["constants"]["words"].update(
                meta_dict["data_const"]["constants"]["words"]
            )
            self.data_const_constants_min_length = min(
                map(len, self.meta_dictionary_obj["data_const"]["constants"]["words"])
            )

        if meta_dict["data_const"]["constants"]["phrases"]:
            self.meta_dictionary_obj["data_const"]["constants"]["phrases"].update(
                meta_dict["data_const"]["constants"]["phrases"]
            )

        _extend_constant_rules(
            self.meta_dictionary_obj["data_const"]["constant_rules"], meta_dict["data_const"]["constant_rules"]
        )
        _extend_constant_rules(
            self.meta_dictionary_obj["data_const"]["partial_constant_rules"],
            meta_dict["data_const"]["partial_constant_rules"],
        )

        if meta_dict["data_func"]:
            normalized_data_func_rules = {
                normalize_data_type(field_type): rules for field_type, rules in meta_dict["data_func"].items()
            }
            self.meta_dictionary_obj["data_func"].update(normalized_data_func_rules)

        if meta_dict["data_sql_condition"]:
            self.meta_dictionary_obj["data_sql_condition"].extend(meta_dict["data_sql_condition"])

        if meta_dict["sens_pg_types"]:
            self.meta_dictionary_obj["sens_pg_types"].extend(
                [normalize_data_type(v) for v in meta_dict["sens_pg_types"]]
            )

        if meta_dict["funcs"]:
            self.meta_dictionary_obj["funcs"].update({normalize_data_type(k): v for k, v in meta_dict["funcs"].items()})

        if meta_dict["no_sens_dictionary"]:
            self.meta_dictionary_obj["no_sens_dictionary"].extend(meta_dict["no_sens_dictionary"])

    def read_meta_dict(self) -> None:
        """Read and compile meta dictionary files into a single merged object."""
        self.meta_dictionary_obj = self._make_meta_dict()
        dict_files_list = list(self.options.meta_dict_files or [])

        if self.options.prepared_no_sens_dict_files:
            dict_files_list.extend(self.options.prepared_no_sens_dict_files)

        for dict_file in dict_files_list:
            dict_data = read_dict_data_from_file(Path.cwd() / dict_file)
            if dict_data:
                self._append_meta_dict(self._make_meta_dict(dict_data))

        if not self.meta_dictionary_obj["sens_pg_types"]:
            self.meta_dictionary_obj["sens_pg_types"] = list(SENS_PG_TYPES)

    def read_prepared_dict(self, save_dict_file_name_for_each_rule: bool = False) -> None:
        """Read prepared sensitive dictionary files into a merged object."""
        if not self.options.prepared_sens_dict_files:
            raise PgAnonError(ErrorCode.NO_DICT_FILES, "No prepared sens dict files specified")

        self.prepared_dictionary_obj = {
            "dictionary": [],
            "dictionary_exclude": [],
            "validate_tables": [],
        }
        tmp_prepared_dict = {}

        for dict_file in self.options.prepared_sens_dict_files:
            dictionary_file_name = Path.cwd() / dict_file
            dict_data = read_dict_data_from_file(dictionary_file_name)

            if not dict_data:
                continue

            if dictionary_rules := dict_data.get("dictionary", []):
                for dictionary_rule in dictionary_rules:
                    rule_key = (
                        dictionary_rule.get("schema"),
                        dictionary_rule.get("schema_mask"),
                        dictionary_rule.get("table"),
                        dictionary_rule.get("table_mask"),
                    )

                    if save_dict_file_name_for_each_rule:
                        dictionary_rule["dict_file_name"] = dict_file

                    if rule_key not in tmp_prepared_dict:
                        tmp_prepared_dict[rule_key] = dictionary_rule
                    else:
                        tmp_prepared_dict[rule_key]["fields"].update(dictionary_rule["fields"])
                        tmp_prepared_dict[rule_key]["sql_condition"] = dictionary_rule.get(
                            "sql_condition", tmp_prepared_dict[rule_key].get("sql_condition")
                        )

            self.prepared_dictionary_obj["dictionary_exclude"].extend(dict_data.get("dictionary_exclude", []))
            self.prepared_dictionary_obj["validate_tables"].extend(dict_data.get("validate_tables", []))

        self.prepared_dictionary_obj["dictionary"].extend(tmp_prepared_dict.values())

    def read_partial_tables_dicts(self) -> None:
        """Read partial table inclusion and exclusion dictionary files."""
        if self.options.partial_tables_dict_files:
            for dict_file in self.options.partial_tables_dict_files:
                if dict_data := read_dict_data_from_file(Path.cwd() / dict_file):
                    self.included_tables_rules.extend(dict_data.get("tables", []))

        if self.options.partial_tables_exclude_dict_files:
            for dict_file in self.options.partial_tables_exclude_dict_files:
                if dict_data := read_dict_data_from_file(Path.cwd() / dict_file):
                    self.excluded_tables_rules.extend(dict_data.get("tables", []))

    def set_postgres_version(self, pg_version: str) -> None:
        """Set the PostgreSQL version and resolve pg_dump/pg_restore paths from config."""
        self.pg_version = pg_version
        pg_major_version = int(pg_version.split(".", maxsplit=1)[0])

        if pg_major_version >= 14:  # noqa: PLR2004
            self.server_settings.update(TRANSACTIONS_SERVER_SETTINGS)

        if not self.config:
            return

        utils_versions = self.config.get("pg-utils-versions")
        if not utils_versions:
            return
        pg_utils = utils_versions.get(pg_major_version)
        if not pg_utils:
            pg_utils = utils_versions.get("default")
            if not pg_utils:
                return

        pg_dump = pg_utils.get("pg_dump")
        pg_restore = pg_utils.get("pg_restore")

        if not pg_dump or not pg_restore:
            raise PgAnonError(
                ErrorCode.INVALID_CONFIG, "Config incorrect. Must be specified pg_dump and pg_restore utils paths"
            )

        self.pg_dump = pg_dump
        self.pg_restore = pg_restore

    def setup_logger(self) -> None:
        """Configure the application logger with appropriate level and file handler."""
        log_level = logging.NOTSET

        if self.options.mode not in (AnonMode.VIEW_FIELDS, AnonMode.VIEW_DATA):
            if self.options.verbose == VerboseOptions.DEBUG:
                log_level = logging.DEBUG
            elif self.options.verbose == VerboseOptions.ERROR:
                log_level = logging.ERROR
            elif self.options.verbose == VerboseOptions.INFO:
                log_level = logging.INFO

        log_dir = Path(self.options.run_dir) / LOGS_DIR_NAME

        logger_add_file_handler(log_dir=log_dir, log_file_name=LOGS_FILE_NAME)
        logger_set_log_level(log_level=log_level)
        self.logger = get_logger()

    def set_tables_lists(
        self,
        tables: list[tuple[str, str]],
        partition_ancestors: dict[tuple[str, str], list[tuple[str, str]]] | None = None,
    ) -> None:
        """Filter and assign table lists based on inclusion and exclusion rules."""
        self.tables, self.black_listed_tables, self.white_listed_tables = filter_db_tables(
            tables=tables,
            white_list_rules=self.included_tables_rules,
            black_list_rules=self.excluded_tables_rules,
            partition_ancestors=partition_ancestors,
        )
