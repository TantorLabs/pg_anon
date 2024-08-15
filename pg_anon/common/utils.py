import decimal
import json
import os.path
import re
import subprocess
import sys
import traceback
from typing import List, Optional, Dict, Union

from pkg_resources import parse_version as version


def get_pg_util_version(util_name):
    command = [util_name, "--version"]
    res = subprocess.run(
        command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True
    )
    return re.findall(r"(\d+\.\d+)", str(res.stdout))[0]


def check_pg_util(ctx, util_name, output_util_res):
    if not os.path.isfile(util_name):
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

    return f


def get_major_version(str_version):
    return version(re.findall(r"(\d+)", str_version)[0])


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
    return [lst[i::n] for i in range(n)]


def recordset_to_list_flat(rs):
    res = []
    for rec in rs:
        row = []
        for _, v in dict(rec).items():
            row.append(v)
        res.append(row)
    return res


def setof_to_list(rs):
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
            elif re.search(rule["schema_mask"], schema) is not None:
                schema_mask_matched = True

        if "table_mask" in rule:
            if rule["table_mask"] == "*":
                table_mask_matched = True
            elif re.search(rule["table_mask"], table) is not None:
                table_mask_matched = True

        if schema_mask_matched and table_matched:
            result = rule
        if schema_matched and table_mask_matched:
            result = rule
        if schema_mask_matched and table_mask_matched:
            result = rule

    return result
