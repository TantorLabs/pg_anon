import asyncio
import json
import os
import re
import time
from typing import List, Dict, Optional

from aioprocessing import AioQueue
from asyncpg import Connection

from pg_anon.common.constants import ANON_UTILS_DB_SCHEMA_NAME
from pg_anon.common.db_queries import get_data_from_field
from pg_anon.common.db_utils import get_scan_fields_list, exec_data_scan_func_query, create_pool
from pg_anon.common.dto import PgAnonResult, FieldInfo
from pg_anon.common.enums import ResultCode, ScanMode
from pg_anon.common.multiprocessing_utils import init_process
from pg_anon.common.utils import (
    chunkify,
    exception_helper,
    setof_to_list,
    get_dict_rule_for_table,
)
from pg_anon.context import Context

SENS_PG_TYPES = ["text", "integer", "bigint", "character", "json", "mvarchar"]


def _check_field_match_by_rule(field: Dict, rule: Dict) -> bool:
    schema_match = False
    tbl_match = False
    fld_match = False

    if "schema" in rule and field["nspname"] == rule["schema"]:
        schema_match = True

    if "table" in rule and field["relname"] == rule["table"]:
        tbl_match = True

    if "fields" in rule and field["column_name"] in rule["fields"]:
        fld_match = True

    if schema_match and tbl_match and fld_match:
        return True

    if "fields" not in rule and schema_match and tbl_match:
        return True

    if "table" not in rule and "fields" not in rule and schema_match:
        return True

    return False


def check_not_skip_fields(ctx: Context, field: Dict) -> bool:
    for rule in ctx.meta_dictionary_obj["skip_rules"]:
        if _check_field_match_by_rule(field=field, rule=rule):
            ctx.logger.debug(f"!!! ------> check_skip_fields: filtered field {field} by rule {rule}")
            return False

    return True


def check_include_fields(ctx: Context, field: Dict) -> bool:
    if not ctx.meta_dictionary_obj["include_rules"]:
        return True

    for rule in ctx.meta_dictionary_obj["include_rules"]:
        if _check_field_match_by_rule(field=field, rule=rule):
            ctx.logger.debug(f"!!! ------> check_include_fields: filtered field {field} by rule {rule}")
            return True

    return False


async def get_fields_for_scan(ctx: Context) -> Dict[str, FieldInfo]:
    """
    Get scanning fields for create dictionary mode
    :param ctx: context for db connection and meta dictionary rules
    :return: dict of fields with key by obj_id for create dictionary mode
    """
    fields_list = await get_scan_fields_list(connection_params=ctx.connection_params, server_settings=ctx.server_settings)

    return {
        field['obj_id']: FieldInfo(**field) for field in fields_list
        if check_include_fields(ctx, field) and check_not_skip_fields(ctx, field)
    }


def prepare_meta_dictionary_obj(ctx):
    ctx.meta_dictionary_obj["data_const"]["constants"] = set(
        ctx.meta_dictionary_obj["data_const"]["constants"]
    )
    ctx.meta_dictionary_obj["data_const"]["partial_constants"] = set(
        ctx.meta_dictionary_obj["data_const"]["partial_constants"]
    )

    regex_for_compile = []
    for v in ctx.meta_dictionary_obj["data_regex"]["rules"]:
        regex_for_compile.append(re.compile(v))

    ctx.meta_dictionary_obj["data_regex"]["rules"] = regex_for_compile.copy()

    regex_for_compile = []
    for v in ctx.meta_dictionary_obj["field"]["rules"]:
        regex_for_compile.append(re.compile(v))

    ctx.meta_dictionary_obj["field"]["rules"] = regex_for_compile.copy()


def scan_fields_by_names(ctx, fields_info: Dict[str, FieldInfo]):
    """
    Scanning fields by names and removes matches according to dict rules

    Priorities of rules:
        - prepared-sens-dict-file
        - meta-dict-file
        - prepared-no-sens-dict-file
    """

    for obj_id, field_info in fields_info.copy().items():
        matched: bool = False
        include_rule: Optional[Dict] = None
        exclude_rule: Optional[Dict] = None

        if ctx.prepared_dictionary_obj.get("dictionary"):
            include_rule = get_dict_rule_for_table(
                dictionary_rules=ctx.prepared_dictionary_obj["dictionary"],
                schema=field_info.nspname,
                table=field_info.relname,
            )

        if ctx.prepared_dictionary_obj.get("dictionary_exclude"):
            exclude_rule = get_dict_rule_for_table(
                dictionary_rules=ctx.prepared_dictionary_obj["dictionary_exclude"],
                schema=field_info.nspname,
                table=field_info.relname,
            )

        # include_rule + has field in include_rule => sensitive field
        # not include_rule + exclude_rule => not sensitive field
        # not include_rule + not exclude_rule => unknown. Go to next rules priority -> check by meta-dict
        if include_rule and field_info.column_name in include_rule['fields']:
            ctx.logger.debug(
                f'!!! ------> check_sensitive_fld_names: match as sensitive by "{include_rule}", removed {field_info}')
            del fields_info[obj_id]
            field_info.rule = include_rule['fields'][field_info.column_name]
            ctx.create_dict_sens_matches[obj_id] = field_info
            matched = True

        elif exclude_rule:
            ctx.logger.debug(
                f'!!! ------> check_sensitive_fld_names: match as insensitive by "{exclude_rule}", removed {field_info}')
            del fields_info[obj_id]
            ctx.create_dict_no_sens_matches[obj_id] = field_info
            matched = True

        if matched:
            continue

        for rule in ctx.meta_dictionary_obj["field"]["constants"]:
            if rule == field_info.column_name:
                if obj_id in fields_info:
                    ctx.logger.debug(f'!!! ------> check_sensitive_fld_names: match as sensitive by "{rule}", removed {field_info}')
                    del fields_info[obj_id]
                    ctx.create_dict_sens_matches[obj_id] = field_info
                matched = True
                break

        if matched:
            continue

        for rule in ctx.meta_dictionary_obj["field"]["rules"]:
            if re.search(rule, field_info.column_name) is not None:
                if obj_id in fields_info:
                    ctx.logger.debug(f'!!! ------> check_sensitive_fld_names: match as sensitive by "{rule}", removed {field_info}')
                    del fields_info[obj_id]
                    ctx.create_dict_sens_matches[obj_id] = field_info
                matched = True
                break

        if matched:
            continue

        for rule in ctx.meta_dictionary_obj["no_sens_dictionary"]:
            if (rule['schema'] == field_info.nspname and
                rule['table'] == field_info.relname and
                field_info.column_name in rule['fields']
            ):
                if obj_id in fields_info:
                    ctx.logger.debug(f'!!! ------> check_sensitive_fld_names: match as insensitive by "{rule}", removed {field_info}')
                    del fields_info[obj_id]
                    ctx.create_dict_no_sens_matches[obj_id] = field_info
                break


def check_data_by_constants(
        ctx: Context,
        name: str,
        dictionary_obj: Dict,
        field_info: FieldInfo,
        fld_data: List
) -> bool:
    if not dictionary_obj["data_const"]["constants"]:
        return False

    for value in fld_data:
        if value is None:
            continue

        for word in value.split():
            if len(word) >= 5 and word.lower() in dictionary_obj["data_const"]["constants"]:
                ctx.logger.debug(f"========> Process[{name}]: check_sensitive_data: match by constant , {field_info}")
                return True

    return False


def check_data_by_partial_constants(
        ctx: Context,
        name: str,
        dictionary_obj: Dict,
        field_info: FieldInfo,
        fld_data: List
) -> bool:
    if not dictionary_obj["data_const"]["partial_constants"]:
        return False

    for value in fld_data:
        if value is None:
            continue

        for partial_constant in dictionary_obj["data_const"]["partial_constants"]:
            if partial_constant in value:
                ctx.logger.debug(
                    f"========> Process[{name}]: check_sensitive_data: match by partial constant {partial_constant} , {field_info}"
                )
                return True
    
    return False


async def check_data_by_functions(
        ctx: Context,
        connection: Connection,
        name: str,
        dictionary_obj: Dict,
        field_info: FieldInfo,
        fld_data: List
) -> bool:
    if not dictionary_obj["data_func"]:
        return False

    rules_by_type = dictionary_obj["data_func"].get(field_info.type, [])
    rules_for_anyelements = dictionary_obj["data_func"].get('anyelement', [])

    for rules in [rules_by_type, rules_for_anyelements]:
        for rule in rules:
            matched_count = 0
            rule_expected_matches_count = rule.get("n_count", 1)

            for value in fld_data:
                if value is None:
                    continue

                if matched := await exec_data_scan_func_query(
                    connection=connection,
                    scan_func=rule["scan_func"],
                    value=value,
                    field_info=field_info
                ):
                    matched_count += 1

                    if matched_count == rule_expected_matches_count:
                        field_info.rule = rule["anon_func"]
                        ctx.logger.debug(
                            f'========> Process[{name}]: check_sensitive_data: match by data scan func {rule["scan_func"]} , {field_info}')
                        return True

    return False


def check_data_by_regexp(
        ctx: Context,
        name: str,
        dictionary_obj: Dict,
        create_dict_matches: List,
        field_info: FieldInfo,
        fld_data: List
) -> bool:
    if field_info.obj_id in create_dict_matches:
        return False

    if dictionary_obj["data_regex"]["rules"]:
        for value in fld_data:
            for rule in dictionary_obj["data_regex"]["rules"]:
                if value is not None and re.search(rule, value) is not None:
                    ctx.logger.debug(
                        f'========> Process[{name}]: check_sensitive_data: match by "{rule}", {value}, {field_info}'
                    )
                    return True
    
    return False


async def check_sensitive_data_in_fld(
        ctx: Context,
        connection: Connection,
        name: str,
        dictionary_obj: Dict,
        create_dict_matches: List,
        field_info: FieldInfo,
        fld_data: List
) -> dict:
    ctx.logger.debug(f"---> Process[{name}]: Started check_sensitive_data for {field_info.column_name}")
    result = {field_info.obj_id: field_info}
    matched = False

    if not matched and await check_data_by_functions(
        ctx=ctx,
        connection=connection,
        name=name,
        dictionary_obj=dictionary_obj,
        field_info=field_info,
        fld_data=fld_data,
    ):
        matched = True

    if check_data_by_constants(
        ctx=ctx,
        name=name,
        dictionary_obj=dictionary_obj,
        field_info=field_info,
        fld_data=fld_data,
    ):
        matched = True

    if not matched and check_data_by_partial_constants(
        ctx=ctx,
        name=name,
        dictionary_obj=dictionary_obj,
        field_info=field_info,
        fld_data=fld_data,
    ):
        matched = True

    if not matched and check_data_by_regexp(
        ctx=ctx,
        name=name,
        dictionary_obj=dictionary_obj,
        create_dict_matches=create_dict_matches,
        field_info=field_info,
        fld_data=fld_data,
    ):
        matched = True

    if matched:
        ctx.logger.debug(f"<--- Process[{name}]: Finished check_sensitive_data for {field_info.column_name}")
        return result

    return {}


def check_sens_pg_types(dictionary_obj, field_type: str):
    """Check if actual field type is sens."""
    data_types = dictionary_obj.get("sens_pg_types", [])
    sens_types = data_types if data_types else SENS_PG_TYPES

    return any(pg_type in field_type for pg_type in sens_types)


async def scan_obj_func(
    name,
    ctx,
    pool,
    field_info: FieldInfo,
    scan_mode: ScanMode,
    dictionary_obj,
    scan_partial_rows,
):

    ctx.logger.debug(f"====>>> Process[{name}]: Started task {field_info}")

    start_t = time.time()
    if not check_sens_pg_types(dictionary_obj, field_info.type):
        ctx.logger.debug(
            f"========> Process[%s]: scan_obj_func: task %s skipped by field type %s"
            % (name, str(field_info), "[integer, text, bigint, character varying(x)]")
        )
        return None

    res = {}
    condition = None
    if ctx.meta_dictionary_obj.get("data_sql_condition"):
        rule = get_dict_rule_for_table(
            dictionary_rules=ctx.meta_dictionary_obj["data_sql_condition"],
            schema=field_info.nspname,
            table=field_info.relname,
        )
        if rule:
            condition = rule.get('sql_condition')

    try:
        async with pool.acquire() as db_conn:
            if scan_mode == ScanMode.PARTIAL:
                query = get_data_from_field(
                    field_info=field_info,
                    limit=scan_partial_rows,
                    condition=condition
                )
                fld_data = await db_conn.fetch(query)
                res = await check_sensitive_data_in_fld(
                    ctx=ctx,
                    connection=db_conn,
                    name=name,
                    dictionary_obj=dictionary_obj,
                    create_dict_matches=ctx.create_dict_sens_matches,
                    field_info=field_info,
                    fld_data=setof_to_list(fld_data),
                )
            elif scan_mode == ScanMode.FULL:
                async with db_conn.transaction(isolation='repeatable_read', readonly=True):
                    query = get_data_from_field(
                        field_info=field_info,
                        condition=condition
                    )
                    cursor = await db_conn.cursor(query)
                    next_rows = True
                    while next_rows:
                        fld_data = await cursor.fetch(scan_partial_rows)
                        res = await check_sensitive_data_in_fld(
                            ctx=ctx,
                            connection=db_conn,
                            name=name,
                            dictionary_obj=dictionary_obj,
                            create_dict_matches=ctx.create_dict_sens_matches,
                            field_info=field_info,
                            fld_data=setof_to_list(fld_data),
                        )
                        if len(fld_data) == 0 or len(res) > 0:
                            break

    except Exception as e:
        ctx.logger.error("Exception in scan_obj_func:\n" + exception_helper())
        raise Exception("Can't execute task: %s" % field_info)

    end_t = time.time()
    if end_t - start_t > 10:
        ctx.logger.warning(
            "!!! Process[%s]: scan_obj_func took %s sec. Task %s"
            % (name, str(round(end_t - start_t, 2)), str(field_info))
        )

    ctx.logger.debug(
        "<<<<==== Process[%s]: Found %s items(s) Finished task %s "
        % (name, str(len(res)), str(field_info))
    )
    return res


def process_impl(name: str, ctx: Context, queue: AioQueue, fields_info_chunk: List[FieldInfo]):
    tasks_res = []

    status_ratio = 10
    if len(fields_info_chunk) > 1000:
        status_ratio = 100
    if len(fields_info_chunk) > 50000:
        status_ratio = 1000

    async def run():
        pool = await create_pool(
            connection_params=ctx.connection_params,
            server_settings=ctx.server_settings,
            min_size=ctx.args.db_connections_per_process,
            max_size=ctx.args.db_connections_per_process
        )
        tasks = set()

        ctx.logger.info(
            "============> Started collecting list_tagged_fields in mode: create-dict"
        )
        ctx.logger.info(
            "<============ Finished collecting list_tagged_fields in mode: create-dict"
        )

        for idx, field_info in enumerate(fields_info_chunk):
            if len(tasks) >= ctx.args.db_connections_per_process:
                done, tasks = await asyncio.wait(
                    tasks, return_when=asyncio.FIRST_COMPLETED
                )
                exception = done.pop().exception()
                if exception is not None:
                    await pool.close()
                    raise exception
            task_res = loop.create_task(
                scan_obj_func(
                    name,
                    ctx,
                    pool,
                    field_info,
                    ctx.args.scan_mode,
                    ctx.meta_dictionary_obj,
                    ctx.args.scan_partial_rows,
                )
            )
            tasks_res.append(task_res)
            tasks.add(task_res)
            if idx % status_ratio:
                progress_percents = round(float(idx) * 100 / len(fields_info_chunk), 2)
                ctx.logger.info(f"Process [{name}] Progress {progress_percents}%")
        if len(tasks) > 0:
            await asyncio.wait(tasks)
        await pool.close()

    loop = asyncio.new_event_loop()

    try:
        asyncio.set_event_loop(loop)
        loop.run_until_complete(run())

        tasks_res_final = []
        for task in tasks_res:
            if task.result() is not None and len(task.result()) > 0:
                tasks_res_final.append(task.result())

        queue.put(tasks_res_final)
    except asyncio.exceptions.TimeoutError:
        ctx.logger.error(f"================> Process [{name}]: asyncio.exceptions.TimeoutError")
    except Exception as ex:
        ctx.logger.error(f"================> Process [{name}]: {ex}")
        raise ex
    finally:
        ctx.logger.error(f"================> Process [{name}] closing")
        loop.close()
        queue.put(None)  # Shut down the worker
        queue.close()
        ctx.logger.error(f"================> Process [{name}] closed")


def prepare_sens_dict_rule(meta_dictionary_obj: dict, field_info: FieldInfo, prepared_sens_dict_rules: dict):
    res_hash_func = field_info.rule

    if res_hash_func is None:
        hash_func = f"{ANON_UTILS_DB_SCHEMA_NAME}.digest(\"%s\", 'salt_word', 'md5')"  # by default use md5 with salt

        for fld_type, func in meta_dictionary_obj["funcs"].items():
            if str(field_info.type).find(fld_type) > -1:
                hash_func = func

        res_hash_func = hash_func if hash_func.find("%s") == -1 else hash_func % field_info.column_name

    if field_info.tbl_id not in prepared_sens_dict_rules:
        prepared_sens_dict_rules[field_info.tbl_id] = {
            "schema": field_info.nspname,
            "table": field_info.relname,
            "fields": {field_info.column_name: res_hash_func},
        }
    else:
        prepared_sens_dict_rules[field_info.tbl_id]["fields"].update(
            {field_info.column_name: res_hash_func}
        )
    return prepared_sens_dict_rules


def prepare_no_sens_dict_rule(field_info: FieldInfo, prepared_no_sens_dict_rules: dict):
    if field_info.tbl_id not in prepared_no_sens_dict_rules:
        prepared_no_sens_dict_rules[field_info.tbl_id] = {
            "schema": field_info.nspname,
            "table": field_info.relname,
            "fields": [field_info.column_name],
        }
    else:
        prepared_no_sens_dict_rules[field_info.tbl_id]["fields"].append(field_info.column_name)
    return prepared_no_sens_dict_rules


async def create_dict_impl(ctx):
    result = PgAnonResult()
    result.result_code = ResultCode.DONE

    fields_info: Dict[str, FieldInfo] = await get_fields_for_scan(ctx)
    if not fields_info:
        raise Exception("No objects for create dictionary!")

    scan_fields_by_names(ctx, fields_info)  # fill ctx.create_dict_sens_matches and ctx.create_dict_no_sens_matches

    # create output dict
    prepared_sens_dict_rules = {}
    need_prepare_no_sens_dict: bool = bool(ctx.args.output_no_sens_dict_file)

    if fields_info:
        fields_info_chunks = list(chunkify(list(fields_info.values()), ctx.args.processes))

        tasks = []
        for idx, fields_info_chunk in enumerate(fields_info_chunks):
            tasks.append(
                asyncio.ensure_future(
                    init_process(
                        name=str(idx + 1),
                        ctx=ctx,
                        target_func=process_impl,
                        tasks=fields_info_chunk
                    )
                )
            )
        await asyncio.wait(tasks)

        # ============================================================================================
        # Fill results based on processes
        # ============================================================================================
        for v in tasks:
            for res in v.result():
                for field_info in res.values():
                    prepared_sens_dict_rules = prepare_sens_dict_rule(
                        ctx.meta_dictionary_obj, field_info, prepared_sens_dict_rules
                    )
                    if need_prepare_no_sens_dict:
                        del fields_info[field_info.obj_id]

    # ============================================================================================
    # Fill results based on check_sensitive_fld_names
    # ============================================================================================
    for field_info in ctx.create_dict_sens_matches.values():
        prepared_sens_dict_rules = prepare_sens_dict_rule(
            ctx.meta_dictionary_obj, field_info, prepared_sens_dict_rules
        )
    # ============================================================================================

    output_sens_dict = {"dictionary": list(prepared_sens_dict_rules.values())}

    output_sens_dict_filename = os.path.join(ctx.current_dir, "dict", ctx.args.output_sens_dict_file)
    output_dir = os.path.dirname(output_sens_dict_filename)
    os.makedirs(output_dir, exist_ok=True)

    with open(output_sens_dict_filename, "w", encoding='utf-8') as file:
        file.write(json.dumps(output_sens_dict, indent=4, ensure_ascii=False))

    if need_prepare_no_sens_dict:
        prepared_no_sens_dict_rules = {}

        for field_info in ctx.create_dict_no_sens_matches.values():
            prepared_no_sens_dict_rules = prepare_no_sens_dict_rule(
                field_info, prepared_no_sens_dict_rules
            )

        for field_info in fields_info.values():
            prepared_no_sens_dict_rules = prepare_no_sens_dict_rule(
                field_info, prepared_no_sens_dict_rules
            )

        output_no_sens_dict = {"no_sens_dictionary": list(prepared_no_sens_dict_rules.values())}
        output_no_sens_dict_file_name = os.path.join(ctx.current_dir, "dict", ctx.args.output_no_sens_dict_file)
        with open(output_no_sens_dict_file_name, "w", encoding='utf-8') as file:
            file.write(json.dumps(output_no_sens_dict, indent=4, ensure_ascii=False))

    return result


async def create_dict(ctx):
    result = PgAnonResult()
    result.result_code = ResultCode.DONE
    ctx.logger.info("-------------> Started create_dict mode")

    try:
        ctx.read_meta_dict()
        prepare_meta_dictionary_obj(ctx)
        if ctx.args.prepared_sens_dict_files:
            ctx.read_prepared_dict()
    except:
        ctx.logger.error("<------------- create_dict failed\n" + exception_helper())
        result.result_code = ResultCode.FAIL
        return result

    try:
        result = await create_dict_impl(ctx)
    except:
        ctx.logger.error("<------------- create_dict failed\n" + exception_helper())
        result.result_code = ResultCode.FAIL
        return result

    if result.result_code == ResultCode.DONE:
        ctx.logger.info("<------------- Finished create_dict mode")
    return result
