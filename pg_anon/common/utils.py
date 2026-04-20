from __future__ import annotations

import ast
import concurrent.futures
import decimal
import json
import re
import subprocess
import sys
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable

    from pg_anon.common.dto import FieldInfo, RunOptions
    from pg_anon.context import Context

import yaml

from pg_anon.common.constants import BASE_TYPE_ALIASES, RUNS_BASE_DIR, SAVED_DICTS_INFO_FILE_NAME, TRACEBACK_LINES_COUNT
from pg_anon.common.errors import ErrorCode, PgAnonError
from pg_anon.logger import get_logger

logger = get_logger()

PARENS_PATTERN = re.compile(r"\([^\)]*\)")
TYPE_PATTERN = re.compile(
    r"""
        ^\s*
        (?P<base>[a-z][a-z0-9_]*)
        (?P<parens>\s*\([^)]*\))?
        (?P<suffix>.*)$
        """,
    re.IGNORECASE | re.VERBOSE,
)


def get_pg_util_version(util_name: str) -> str:
    """Return the version string of a PostgreSQL utility."""
    command = [util_name, "--version"]
    res = subprocess.run(command, capture_output=True, text=True, check=False)
    return re.findall(r"(\d+\.\d+)", str(res.stdout))[0]


def check_pg_util(ctx: Context, util_name: str, output_util_res: str) -> bool:
    """Check that a PostgreSQL utility exists and matches the expected version."""
    if not Path(util_name).is_file():
        ctx.logger.error("ERROR: program %s is not exists!", util_name)
        return False

    command = [util_name, "--version"]
    res = subprocess.run(command, capture_output=True, text=True, check=False)
    if str(res.stdout).find(output_util_res) == -1:
        ctx.logger.error("ERROR: program %s is not %s!", util_name, output_util_res)
        return False

    return True


def exception_helper(show_traceback: bool = True) -> str:
    """Format the current exception as a string."""
    exc_type, exc_value, exc_traceback = sys.exc_info()
    return "\n".join(list(traceback.format_exception(exc_type, exc_value, exc_traceback if show_traceback else None)))


def exception_handler(func: Callable) -> Callable:
    """Decorate a function to print and re-raise any exception."""

    def f(*args, **kwargs) -> None:  # noqa: ANN002, ANN003
        try:
            func(*args, **kwargs)
        except:
            print(exception_helper(show_traceback=True))
            raise

    return f


def get_major_version(str_version: str) -> str:
    """Extract the major version number from a version string."""
    return re.findall(r"(\d+)", str_version)[0]


def pretty_size(bytes_v: int) -> str:
    """Format a byte count as a human-readable size string."""
    if bytes_v < 1024:  # noqa: PLR2004
        if bytes_v == 1:
            return "1 byte"
        return f"{bytes_v} bytes"

    units = ["KB", "MB", "GB", "TB", "PB"]
    value: float = bytes_v
    for unit in units:
        value /= 1024
        if value < 1024:  # noqa: PLR2004
            return f"{int(value)} {unit}"

    return f"{int(value)} {units[-1]}"

def recordset_to_list_flat(rs: list) -> list:
    """Convert a recordset to a list of flat value lists."""
    return [list(dict(rec).values()) for rec in rs]


def setof_to_list(rs: list) -> list:
    """Flatten a set-of recordset into a single list of values."""
    res: list = []
    for rec in rs:
        res.extend(dict(rec).values())
    return res


def to_json(obj, formatted: bool = False) -> str | bytes:  # noqa: ANN001
    """Serialize an object to JSON string or UTF-8 bytes."""

    def type_adapter(o) -> float | None:  # noqa: ANN001
        if isinstance(o, decimal.Decimal):
            return float(o)
        return None

    if formatted:
        return json.dumps(obj, default=type_adapter, ensure_ascii=False, indent=4, sort_keys=True)
    return json.dumps(obj, default=type_adapter, ensure_ascii=False).encode("utf8")


def parse_comma_separated_list(value: str | None = None) -> list[str] | None:
    """Parse a comma-separated string into a list of strings."""
    if not value:
        return None

    return list(value.split(","))


def get_dict_rule_for_table(dictionary_rules: list[dict], schema: str, table: str) -> dict | None:
    """Find matching rules for a table in the prepared dictionary."""
    result = None

    for rule in dictionary_rules:
        schema_matched = False
        table_matched = False
        schema_mask_matched = False
        table_mask_matched = False

        if "schema" in rule and schema == rule["schema"]:
            schema_matched = True

        if "table" in rule and table == rule["table"]:
            table_matched = True

        if schema_matched and table_matched:
            return rule

        if "schema_mask" in rule and (
            rule["schema_mask"] == "*" or re.search(safe_compile(rule["schema_mask"]), schema) is not None
        ):
            schema_mask_matched = True

        if "table_mask" in rule and (
            rule["table_mask"] == "*" or re.search(safe_compile(rule["table_mask"]), table) is not None
        ):
            table_mask_matched = True

        if schema_mask_matched and table_matched:
            result = rule
        if schema_matched and table_mask_matched:
            result = rule
        if schema_mask_matched and table_mask_matched:
            result = rule

    return result


def validate_exists_mode(mode: str) -> bool:
    """Check whether the given mode string is a valid AnonMode."""
    from pg_anon.common.enums import AnonMode  # noqa: PLC0415

    try:
        AnonMode(mode)
    except ValueError:
        return False

    return True


def get_file_size(file_path: str | Path) -> int:
    """Return the size of a file in bytes, or 0 if it does not exist."""
    path = Path(file_path)
    return path.stat().st_size if path.exists() else 0


def get_folder_size(folder_path: str | Path) -> int:
    """Calculate the total size of all files in a folder recursively."""
    total_size = 0
    folder_path = Path(folder_path)
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future_to_file = [executor.submit(get_file_size, fp) for fp in folder_path.rglob("*") if fp.is_file()]

        # Собираем результаты
        for future in concurrent.futures.as_completed(future_to_file):
            total_size += future.result()

    return total_size


def simple_slugify(value: str) -> str:
    """Convert a string to a URL-friendly slug."""
    return re.sub(r"\W+", "-", value).strip("-").lower()


def read_yaml(file_path: str | Path) -> dict:
    """Read and parse a YAML file, returning its contents as a dictionary."""
    path = Path(file_path)
    if path.suffix not in (".yml", ".yaml"):
        raise PgAnonError(ErrorCode.INVALID_FILE_FORMAT, "File must be .yml or .yaml")

    with path.open() as file:
        return yaml.safe_load(file)


def get_base_field_type(field_info: FieldInfo) -> str:
    """Extract the base type name from a field info, stripping parenthesized parts."""
    return PARENS_PATTERN.sub("", field_info.type)


def normalize_data_type(data_type: str) -> str:
    """Normalize a PostgreSQL data type string to a canonical lowercase form."""
    clean_field_type = data_type.strip().lower()
    pattern_match = TYPE_PATTERN.match(clean_field_type)
    if not pattern_match:
        return clean_field_type  # fallback

    # parse type parts
    base = pattern_match.group("base")
    parens = (pattern_match.group("parens") or "").strip()
    suffix = (pattern_match.group("suffix") or "").strip()

    # normalize base type
    normalized_base_type = BASE_TYPE_ALIASES.get(base, base)

    # remove TZ info from type
    tz_piece = ""
    if normalized_base_type.endswith(" with time zone"):
        normalized_base_type = normalized_base_type[: -len(" with time zone")]
        tz_piece = "with time zone"
    elif normalized_base_type.endswith(" without time zone"):
        normalized_base_type = normalized_base_type[: -len(" without time zone")]
        tz_piece = "without time zone"

    tz_in_suffix = ("with time zone" in suffix) or ("without time zone" in suffix)

    if tz_piece:
        if tz_in_suffix:
            suffix = re.sub(r"\s*(?:with|without)\s+time\s+zone", "", suffix, flags=re.IGNORECASE).strip()
    elif normalized_base_type in ("time", "timestamp") and not tz_in_suffix:
        tz_piece = "without time zone"

    # Compose type name by parts
    result = normalized_base_type

    if parens:
        result += parens.strip()

    if tz_piece:
        result += " " + tz_piece

    if suffix:
        result += " " + suffix

    result = re.sub(r"\s+\(", "(", result)
    result = re.sub(r"\s+", " ", result).strip()

    return result.lower()


def split_constants_to_words_and_phrases(constants: list[str]) -> tuple[set, set]:
    """Split a list of string constants into single-word and multi-word sets."""
    single_words = set()
    multi_words = set()

    for constant in constants:
        normalized = constant.strip().lower()
        if not normalized:
            continue

        if " " in normalized:
            multi_words.add(normalized)
        else:
            single_words.add(normalized)

    return single_words, multi_words


def exception_to_str(exc: Exception, limit: int = TRACEBACK_LINES_COUNT) -> str:
    """Format an exception traceback as a string, limited to the last N lines."""
    tb_exc = traceback.TracebackException.from_exception(exc)
    lines = list(tb_exc.format())
    return "".join(lines[-limit:])


def filter_db_tables(
    tables: list[tuple[str, str]],
    white_list_rules: list[dict] | None = None,
    black_list_rules: list[dict] | None = None,
) -> tuple[list[tuple[str, str]], set[tuple[str, str]], set[tuple[str, str]]]:
    """Filter database tables by whitelist and blacklist rules."""
    filtered_tables: list[tuple[str, str]] = []
    black_listed_tables: set[tuple[str, str]] = set()
    white_listed_tables: set[tuple[str, str]] = set()
    if not (white_list_rules or black_list_rules):
        return tables, black_listed_tables, white_listed_tables

    for table_data in tables:
        # black list has the highest priority for pg_dump / pg_restore
        if black_list_rules and get_dict_rule_for_table(black_list_rules, *table_data):
            # if table in black list, this table must be filtered out
            black_listed_tables.add(table_data)
            continue

        # white list has the second priority for pg_dump / pg_restore
        if white_list_rules:
            if get_dict_rule_for_table(white_list_rules, *table_data):
                # if white list is using and table in white list, this table must not be filtered
                white_listed_tables.add(table_data)
                filtered_tables.append(table_data)
            continue

        # if table not in black list and white list not using, this table must not be filtered
        filtered_tables.append(table_data)

    return filtered_tables, black_listed_tables, white_listed_tables


def resolve_dependencies(
    extension_name: str, extensions_map: dict[str, list[dict[str, Any]]], seen: set | None = None
) -> set:
    """Recursively resolve all dependencies for a PostgreSQL extension."""
    if seen is None:
        seen = set()

    if extension_name in seen:
        return seen

    seen.add(extension_name)

    for extension_data in extensions_map.get(extension_name, []):
        if not extension_data["requires"]:
            continue

        for dependency in extension_data["requires"]:
            resolve_dependencies(dependency, extensions_map, seen)

    return seen


def safe_compile(pattern: str, flags: int = 0) -> re.Pattern:
    """Compile a regex pattern, returning a never-matching pattern on error."""
    try:
        return re.compile(pattern, flags)
    except re.error:
        logger.warning("Regex pattern is invalid: %s. This pattern will be ignored", pattern)
        return re.compile(r"(?!)")  # Never matching. Instead of None


def save_json_file(file_path: str | Path, data: dict) -> None:
    """Write a dictionary to a JSON file with pretty formatting."""
    with Path(file_path).open("w", encoding="utf-8") as out_file:
        out_file.write(json.dumps(data, indent=4, ensure_ascii=False))


def read_dict_data(data: str, dict_name: str | Path) -> dict[str, Any] | None:
    """Parse a string as a Python literal and validate it is a dictionary."""
    try:
        dict_data = ast.literal_eval(data)
    except Exception as ex:
        raise PgAnonError(ErrorCode.INVALID_DICT_FILE, f"Can't read data from file: {dict_name}") from ex

    if not dict_data:
        return None

    if not isinstance(dict_data, dict):
        raise PgAnonError(ErrorCode.INVALID_DICT_FILE, f"Received non-dictionary structure from file: {dict_name}")

    return dict_data


def read_dict_data_from_file(dictionary_file_path: Path) -> dict[str, Any] | None:
    """Read and parse dictionary data from a file."""
    with dictionary_file_path.open() as dictionary_file:
        data = dictionary_file.read().strip()

    if not data:
        return None

    return read_dict_data(data, dictionary_file_path)


def save_dicts_info_file(options: RunOptions) -> None:
    """Serialize all dictionary file contents and save them as a JSON info file."""

    def serialize_dict(file_path: str) -> dict[str, Any] | None:
        path = Path(file_path)
        if not path.exists():
            return None

        with path.open() as file:
            content = file.read().strip()

        return {"name": path.name, "content": content}

    data = {
        "meta_dict_files": [serialize_dict(file) for file in options.meta_dict_files]
        if options.meta_dict_files
        else None,
        "output_sens_dict_file": serialize_dict(options.output_sens_dict_file)
        if options.output_sens_dict_file
        else None,
        "output_no_sens_dict_file": serialize_dict(options.output_no_sens_dict_file)
        if options.output_no_sens_dict_file
        else None,
        "prepared_sens_dict_files": [serialize_dict(file) for file in options.prepared_sens_dict_files]
        if options.prepared_sens_dict_files
        else None,
        "prepared_no_sens_dict_files": [serialize_dict(file) for file in options.prepared_no_sens_dict_files]
        if options.prepared_no_sens_dict_files
        else None,
        "partial_tables_dict_files": [serialize_dict(file) for file in options.partial_tables_dict_files]
        if options.partial_tables_dict_files
        else None,
        "partial_tables_exclude_dict_files": [
            serialize_dict(file) for file in options.partial_tables_exclude_dict_files
        ]
        if options.partial_tables_exclude_dict_files
        else None,
    }

    saved_dicts_info_file = Path(options.run_dir) / SAVED_DICTS_INFO_FILE_NAME
    save_json_file(saved_dicts_info_file, data)


def make_run_dir(internal_operation_id: str) -> str:
    """Create dir for operation logs and data."""
    today = datetime.today()
    return str(
        RUNS_BASE_DIR /
        str(today.year) /
        str(today.month) /
        str(today.day) /
        internal_operation_id
    )
