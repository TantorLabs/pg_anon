import ast
import concurrent.futures
import decimal
import json
import re
import subprocess
import sys
import traceback
from pathlib import Path
from typing import List, Optional, Dict, Union, Tuple, Set, Any

import yaml

from pg_anon.common.constants import BASE_TYPE_ALIASES, TRACEBACK_LINES_COUNT, SAVED_DICTS_INFO_FILE_NAME
from pg_anon.common.dto import FieldInfo, RunOptions
from pg_anon.logger import get_logger

logger = get_logger()

PARENS_PATTERN = re.compile(r'\([^\)]*\)')
TYPE_PATTERN = re.compile(
        r"""
        ^\s*
        (?P<base>[a-z][a-z0-9_]*)
        (?P<parens>\s*\([^)]*\))?
        (?P<suffix>.*)$          
        """,
        re.IGNORECASE | re.VERBOSE,
    )

def get_pg_util_version(util_name):
    command = [util_name, "--version"]
    res = subprocess.run(
        command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True
    )
    return re.findall(r"(\d+\.\d+)", str(res.stdout))[0]


def check_pg_util(ctx, util_name, output_util_res):
    if not Path(util_name).is_file():
        ctx.logger.error("ERROR: program %s is not exists!" % util_name)
        return False

    command = [util_name, "--version"]
    res = subprocess.run(
        command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True
    )
    if str(res.stdout).find(output_util_res) == -1:
        ctx.logger.error("ERROR: program %s is not %s!" % (util_name, output_util_res))
        return False

    return True


def exception_helper(show_traceback=True):
    exc_type, exc_value, exc_traceback = sys.exc_info()
    return "\n".join(
        [
            v
            for v in traceback.format_exception(
                exc_type, exc_value, exc_traceback if show_traceback else None
            )
        ]
    )


def exception_handler(func):
    def f(*args, **kwargs):
        try:
            func(*args, **kwargs)
        except:
            print(exception_helper(show_traceback=True))
            raise

    return f


def get_major_version(str_version):
    return re.findall(r"(\d+)", str_version)[0]


def pretty_size(bytes_v):
    units = [
        (1 << 50, " PB"),
        (1 << 40, " TB"),
        (1 << 30, " GB"),
        (1 << 20, " MB"),
        (1 << 10, " KB"),
        (1, (" byte", " bytes")),
    ]
    for factor, suffix in units:
        if bytes_v >= factor:
            break
    amount = int(bytes_v / factor)

    if isinstance(suffix, tuple):
        singular, multiple = suffix
        if amount == 1:
            suffix = singular
        else:
            suffix = multiple
    return str(amount) + suffix


def chunkify(lst, n):
    result = [lst[i::n] for i in range(n)]
    result = [x for x in result if x]  # clear empty lists
    return result


def recordset_to_list_flat(rs):
    res = []
    for rec in rs:
        row = []
        for _, v in dict(rec).items():
            row.append(v)
        res.append(row)
    return res


def setof_to_list(rs) -> List:
    res = []
    for rec in rs:
        for _, v in dict(rec).items():
            res.append(v)
    return res


def to_json(obj, formatted=False):
    def type_adapter(o):
        if isinstance(o, decimal.Decimal):
            return float(o)

    if formatted:
        return json.dumps(
            obj, default=type_adapter, ensure_ascii=False, indent=4, sort_keys=True
        )
    else:
        return json.dumps(obj, default=type_adapter, ensure_ascii=False).encode("utf8")


def parse_comma_separated_list(value: str = None) -> Optional[List[str]]:
    if not value:
        return None

    return [item for item in value.split(',')]


def get_dict_rule_for_table(dictionary_rules: List[Dict], schema: str, table: str) -> Optional[Union[List[Dict], Dict]]:
    """
    Find matches rules for field in prepared dictionary
    :param dictionary_rules: prepared dictionary rules
    :param schema: schema of table which needs to be checked
    :param table: table name which needs to be checked
    :return: last matched rule
    """
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

        if "schema_mask" in rule:
            if rule["schema_mask"] == "*":
                schema_mask_matched = True
            elif re.search(safe_compile(rule["schema_mask"]), schema) is not None:
                schema_mask_matched = True

        if "table_mask" in rule:
            if rule["table_mask"] == "*":
                table_mask_matched = True
            elif re.search(safe_compile(rule["table_mask"]), table) is not None:
                table_mask_matched = True

        if schema_mask_matched and table_matched:
            result = rule
        if schema_matched and table_mask_matched:
            result = rule
        if schema_mask_matched and table_mask_matched:
            result = rule

    return result


def validate_exists_mode(mode: str):
    from pg_anon.common.enums import AnonMode

    try:
        AnonMode(mode)
    except ValueError:
        return False

    return True


def get_file_size(file_path: Union[str, Path]) -> int:
    path = Path(file_path)
    return path.stat().st_size if path.exists() else 0


def get_folder_size(folder_path: Union[str, Path]) -> int:
    total_size = 0
    folder_path = Path(folder_path)
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future_to_file = []
        for file_path in folder_path.rglob('*'):
            if file_path.is_file():
                future_to_file.append(executor.submit(get_file_size, file_path))

        # Собираем результаты
        for future in concurrent.futures.as_completed(future_to_file):
            total_size += future.result()

    return total_size


def simple_slugify(value: str):
    return re.sub(r'\W+', '-', value).strip('-').lower()


def read_yaml(file_path: Union[str, Path]) -> Dict:
    path = Path(file_path)
    if path.suffix not in ('.yml', '.yaml'):
        raise ValueError("File must be .yml or .yaml")

    with open(path.absolute(), "r") as file:
        data = yaml.safe_load(file)

    return data


def get_base_field_type(field_info: FieldInfo) -> str:
    return PARENS_PATTERN.sub('', field_info.type)


def normalize_data_type(data_type: str) -> str:
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
            suffix = re.sub(r'\s*(?:with|without)\s+time\s+zone', '', suffix, flags=re.I).strip()
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


def split_constants_to_words_and_phrases(constants: List[str]) -> Tuple[set, set]:
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
    tb_exc = traceback.TracebackException.from_exception(exc)
    lines = list(tb_exc.format())
    return "".join(lines[-limit:])


def filter_db_tables(
        tables: List[Tuple[str, str]],
        white_list_rules: Optional[List[Dict]] = None,
        black_list_rules: Optional[List[Dict]] = None
) -> Tuple[List[Tuple[str, str]], Set[Tuple[str, str]], Set[Tuple[str, str]]]:
    filtered_tables = []
    black_listed_tables = set()
    white_listed_tables = set()
    if not (white_list_rules or black_list_rules):
        return tables, black_listed_tables, white_listed_tables

    for table_data in tables:
        # black list has the highest priority for pg_dump / pg_restore
        if black_list_rules:
            if table_excluded := get_dict_rule_for_table(black_list_rules, *table_data):
                # if table in black list, this table must be filtered out
                black_listed_tables.add(table_data)
                continue

        # white list has the second priority for pg_dump / pg_restore
        if white_list_rules:
            if table_included := get_dict_rule_for_table(white_list_rules, *table_data):
                # if white list is using and table in white list, this table must not be filtered
                white_listed_tables.add(table_data)
                filtered_tables.append(table_data)
            continue

        # if table not in black list and white list not using, this table must not be filtered
        filtered_tables.append(table_data)

    return filtered_tables, black_listed_tables, white_listed_tables


def safe_compile(pattern: str, flags=0):
    try:
        return re.compile(pattern, flags)
    except re.error:
        logger.warn(f"Regex pattern is invalid: {pattern}. This pattern will be ignored")
        return re.compile(r"(?!)")  # Never matching. Instead of None


def save_json_file(file_path: Union[str, Path], data: Dict):
    with open(file_path, "w", encoding='utf-8') as out_file:
        out_file.write(json.dumps(data, indent=4, ensure_ascii=False))


def read_dict_data_from_file(dictionary_file_path: Path) -> Optional[Dict[str, Any]]:
    with open(dictionary_file_path, "r") as dictionary_file:
        data = dictionary_file.read().strip()

    if not data:
        return

    try:
        dict_data = ast.literal_eval(data)
    except Exception as exc:
        raise ValueError(f"Can't read data from file: {dictionary_file_path}")

    if not dict_data:
        return

    if not isinstance(dict_data, dict):
        raise ValueError(f"Received non-dictionary structure from file: {dictionary_file_path}")

    return dict_data


def save_dicts_info_file(options: RunOptions):
    def serialize_dict(file_path: str) -> Optional[Dict[str, Any]]:
        file_path = Path(file_path)
        if not file_path.exists():
            return None

        with open(file_path, "r") as file:
            content = file.read().strip()

        return {
            "name": file_path.name,
            "content": content
        }

    data = {
        "meta_dict_files": [
            serialize_dict(file) for file in options.meta_dict_files
        ] if options.meta_dict_files else None,
        "output_sens_dict_file": serialize_dict(
            options.output_sens_dict_file
        ) if options.output_sens_dict_file else None,
        "output_no_sens_dict_file": serialize_dict(
            options.output_no_sens_dict_file
        ) if options.output_no_sens_dict_file else None,
        "prepared_sens_dict_files": [
            serialize_dict(file) for file in options.prepared_sens_dict_files
        ] if options.prepared_sens_dict_files else None,
        "prepared_no_sens_dict_files": [
            serialize_dict(file) for file in options.prepared_no_sens_dict_files
        ] if options.prepared_no_sens_dict_files else None,
        "partial_tables_dict_files": [
            serialize_dict(file) for file in options.partial_tables_dict_files
        ] if options.partial_tables_dict_files else None,
        "partial_tables_exclude_dict_files": [
            serialize_dict(file) for file in options.partial_tables_exclude_dict_files
        ] if options.partial_tables_exclude_dict_files else None,
    }

    saved_dicts_info_file = Path(options.run_dir) / SAVED_DICTS_INFO_FILE_NAME
    save_json_file(saved_dicts_info_file, data)
