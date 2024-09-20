import decimal
import hashlib
import json
import os.path
import re
import subprocess
import sys
import traceback
from typing import List, Optional, Dict, Union

from pkg_resources import parse_version as version

from pg_anon.common.db_utils import get_fields_list


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


async def get_dump_query(ctx, table_schema: str, table_name: str, table_rule,
                         files: Dict, excluded_objs: List, included_objs: List):

    table_name_full = f'"{table_schema}"."{table_name}"'

    found_white_list = table_rule is not None

    # dictionary_exclude has the highest priority
    if ctx.prepared_dictionary_obj.get("dictionary_exclude"):
        exclude_rule = get_dict_rule_for_table(
            dictionary_rules=ctx.prepared_dictionary_obj["dictionary_exclude"],
            schema=table_schema,
            table=table_name,
        )
        found = exclude_rule is not None
        if found and not found_white_list:
            excluded_objs.append(
                [
                    exclude_rule,
                    table_schema,
                    table_name,
                    "if found and not found_white_list",
                ]
            )
            ctx.logger.info("Skipping: " + str(table_name_full))
            return None

    hashed_name = hashlib.md5(
        (table_schema + "_" + table_name).encode()
    ).hexdigest()

    files[f"{hashed_name}.bin.gz"] = {"schema": table_schema, "table": table_name}

    if not found_white_list:
        included_objs.append(
            [table_rule, table_schema, table_name, "if not found_white_list"]
        )
        # there is no table in the dictionary, so it will be transferred "as is"
        if (ctx.args.dbg_stage_1_validate_dict
                or ctx.args.dbg_stage_2_validate_data
                or ctx.args.dbg_stage_3_validate_full):
            query = "SELECT * FROM %s %s" % (table_name_full, ctx.validate_limit)
            return query
        else:
            query = f"SELECT * FROM {table_name_full}"
            return query
    else:
        included_objs.append(
            [table_rule, table_schema, table_name, "if found_white_list"]
        )
        # table found in dictionary
        if "raw_sql" in table_rule:
            # the table is transferred using "raw_sql"
            if (ctx.args.dbg_stage_1_validate_dict
                    or ctx.args.dbg_stage_2_validate_data
                    or ctx.args.dbg_stage_3_validate_full):
                query = table_rule["raw_sql"] + " " + ctx.validate_limit
                ctx.logger.info(str(query))
                return query
            else:
                query = table_rule["raw_sql"]
                return query
        else:
            # the table is transferred with the specific fields for anonymization
            fields_list = await get_fields_list(
                connection_params=ctx.connection_params,
                server_settings=ctx.server_settings,
                table_schema=table_schema,
                table_name=table_name
            )

            sql_expr = ""

            def check_field(field_name: str):
                if field_name in table_rule["fields"]:
                    return field_name, table_rule["fields"][field_name]
                return None, None

            for cnt, column_info in enumerate(fields_list):
                column_name = column_info["column_name"]
                udt_name = column_info["udt_name"]
                field_name, field_value = check_field(column_name)

                if field_name:
                    if field_value.find("SQL:") == 0:
                        sql_expr += f'({field_value[4:]}) as "{field_name}"'
                    else:
                        sql_expr += f'{field_value}::{udt_name} as "{field_name}"'
                else:
                    # field "as is"
                    sql_expr += f'"{column_name}" as "{column_name}"'

                if cnt != len(fields_list) - 1:
                    sql_expr += ",\n"

            query = f"SELECT {sql_expr} FROM {table_name_full}"
            if (ctx.args.dbg_stage_1_validate_dict
                    or ctx.args.dbg_stage_2_validate_data
                    or ctx.args.dbg_stage_3_validate_full):
                query += f" {ctx.validate_limit}"

            return query


def get_file_name_from_path(path: str) -> str:
    """
    Extract file name without extension from path
    :param path: file path
    :return: only file_name without extension
    """
    file_name = os.path.basename(path)
    return os.path.splitext(file_name)[0]
