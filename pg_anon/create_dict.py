import asyncio
import json
import os
import random
import re
import time
from typing import List, Optional

import aioprocessing
import asyncpg
import nest_asyncio

from pg_anon.common import (
    PgAnonResult,
    ResultCode,
    ScanMode,
    chunkify,
    exception_helper,
    setof_to_list,
)


SENS_PG_TYPES = ["text", "integer", "bigint", "character", "json", "mvarchar"]


class TaggedFields:
    def __init__(
        self, nspname: str, relname: str, column_name: str, column_comment: str
    ):
        self.nspname = nspname
        self.relname = relname
        self.column_name = column_name
        self.column_comment = column_comment


class FieldInfo:
    def __init__(
        self,
        nspname: str,
        relname: str,
        column_name: str,
        type: str,
        oid: int,
        attnum: int,
        obj_id: str,
        tbl_id: str,
    ):
        self.nspname = nspname
        self.relname = relname
        self.column_name = column_name
        self.type = type
        self.oid = oid
        self.attnum = attnum
        self.obj_id = obj_id
        self.tbl_id = tbl_id

    def __str__(self):
        return (
            f"nspname={self.nspname}, "
            f"relname={self.relname}, "
            f"column_name={self.column_name}, "
            f"type={self.type}, oid={self.oid}, "
            f"attnum={self.attnum}, "
            f"obj_id={self.obj_id}, "
            f"tbl_id={self.tbl_id}"
        )


async def get_tagged_fields(conn_params) -> List[TaggedFields]:
    """Get fields tagged sens and nosens."""
    query = """
    SELECT
        nspname AS schema_name,
        relname AS table_name,
        attname AS column_name,
        description AS column_comment
    FROM
        pg_description
        JOIN pg_attribute ON pg_description.objoid = pg_attribute.attrelid
                           AND pg_description.objsubid = pg_attribute.attnum
        JOIN pg_class ON pg_class.oid = pg_attribute.attrelid
        JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace
    WHERE
        pg_class.relkind = 'r' AND pg_attribute.attnum > 0 AND NOT pg_attribute.attisdropped
        and (description like '%:sens%' OR description like '%:nosens%')
    ORDER BY
        nspname,
        relname,
        attname;
    """
    db_conn = await asyncpg.connect(**conn_params)
    try:
        query_res = await db_conn.fetch(query)
    finally:
        await db_conn.close()
    tagged_fields = [
        TaggedFields(
            nspname=record["schema_name"],
            relname=record["table_name"],
            column_name=record["column_name"],
            column_comment=record["column_comment"],
        )
        for record in query_res
    ]
    return tagged_fields


def check_skip_fields(ctx, fld):
    if "skip_rules" not in ctx.dictionary_obj:
        return True
    for v in ctx.dictionary_obj["skip_rules"]:
        schema_match = False
        tbl_match = False
        fld_match = False
        res = True
        if "schema" in v and fld["nspname"] == v["schema"]:
            schema_match = True
        if "table" in v and fld["relname"] == v["table"]:
            tbl_match = True
        if "fields" in v and fld["column_name"] in v["fields"]:
            fld_match = True
        if schema_match and tbl_match and fld_match:
            res = False

        if "fields" not in v and schema_match and tbl_match:
            res = False

        if "table" not in v and "fields" not in v and schema_match:
            res = False

        if not res:
            ctx.logger.debug(
                "!!! ------> check_skip_fields: filtered fld %s by rule %s"
                % (str(dict(fld)), str(v))
            )
            return res

    return True


async def generate_scan_objs(ctx):
    db_conn = await asyncpg.connect(**ctx.conn_params)
    query = """
    -- generate task queue
    SELECT 
        n.nspname,
        c.relname,
        a.attname AS column_name,
        format_type(a.atttypid, a.atttypmod) as type,
        -- a.*
        c.oid, a.attnum,
        anon_funcs.digest(n.nspname || '.' || c.relname || '.' || a.attname, '', 'md5') as obj_id,
        anon_funcs.digest(n.nspname || '.' || c.relname, '', 'md5') as tbl_id
    FROM pg_class c
    JOIN pg_namespace n on c.relnamespace = n.oid
    JOIN pg_attribute a ON a.attrelid = c.oid
    JOIN pg_type t ON a.atttypid = t.oid
    LEFT JOIN pg_index i ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
    WHERE
        a.attnum > 0
        AND c.relkind IN ('r', 'p')
        AND a.atttypid = t.oid
        AND n.nspname not in ('pg_catalog', 'information_schema', 'pg_toast')
        AND coalesce(i.indisprimary, false) = false
        AND row(c.oid, a.attnum) not in (
            SELECT
                t.oid,
                a.attnum --,
                -- pn_t.nspname,
                -- t.relname AS table_name,
                -- a.attname AS column_name
            FROM pg_class AS t
            JOIN pg_attribute AS a ON a.attrelid = t.oid
            JOIN pg_depend AS d ON d.refobjid = t.oid AND d.refobjsubid = a.attnum
            JOIN pg_class AS s ON s.oid = d.objid
            JOIN pg_namespace AS pn_t ON pn_t.oid = t.relnamespace
            WHERE
                t.relkind IN ('r', 'p')
                AND s.relkind = 'S'
                AND d.deptype = 'a'
                AND d.classid = 'pg_catalog.pg_class'::regclass
                AND d.refclassid = 'pg_catalog.pg_class'::regclass
        )
        -- AND c.relname = '_reference866'  -- debug
        -- and a.attname = '_code'
    ORDER BY 1, 2, a.attnum
    """
    query_res = await db_conn.fetch(query)
    await db_conn.close()

    return [
        FieldInfo(**fld) for fld in query_res if check_skip_fields(ctx, fld)
    ]


async def prepare_dictionary_obj(ctx):
    ctx.dictionary_obj["data_const"]["constants"] = set(
        ctx.dictionary_obj["data_const"]["constants"]
    )

    regex_for_compile = []
    for v in ctx.dictionary_obj["data_regex"]["rules"]:
        regex_for_compile.append(re.compile(v))

    ctx.dictionary_obj["data_regex"]["rules"] = regex_for_compile.copy()

    regex_for_compile = []
    for v in ctx.dictionary_obj["field"]["rules"]:
        regex_for_compile.append(re.compile(v))

    ctx.dictionary_obj["field"]["rules"] = regex_for_compile.copy()


async def check_sensitive_fld_names(ctx, fields_info: List[FieldInfo]):
    for field_info in fields_info:
        if "rules" in ctx.dictionary_obj["field"]:
            for rule in ctx.dictionary_obj["field"]["rules"]:
                if re.search(rule, field_info.column_name) is not None:
                    ctx.logger.debug(
                        '!!! ------> check_sensitive_fld_names: match by "%s", removed %s'
                        % (str(rule), str(field_info))
                    )
                    # objs.remove(v)
                    ctx.create_dict_matches[field_info.obj_id] = field_info

        if "constants" in ctx.dictionary_obj["field"]:
            for rule in ctx.dictionary_obj["field"]["constants"]:
                if rule == field_info.column_name:
                    ctx.logger.debug(
                        '!!! ------> check_sensitive_fld_names: match by "%s", removed %s'
                        % (str(rule), str(field_info))
                    )
                    fields_info.remove(field_info)
                    ctx.create_dict_matches[field_info.obj_id] = field_info


def check_sensitive_data_in_fld(
    ctx, name, dictionary_obj, create_dict_matches, field_info: FieldInfo, fld_data
) -> dict:
    if field_info.relname == "_reference866":
        x = 1
    fld_data_set = set()
    dict_matches = {}
    for v in fld_data:
        if v is None:
            continue
        for word in v.split():
            if len(word) >= 5:
                fld_data_set.add(word.lower())

    result = set.intersection(dictionary_obj["data_const"]["constants"], fld_data_set)
    if len(result) > 0:
        ctx.logger.debug(
            "========> Process[%s]: check_sensitive_data: match by constant %s , %s"
            % (name, str(result), str(field_info))
        )
        dict_matches[field_info.obj_id] = field_info

    for v in fld_data:
        if (
            field_info.obj_id not in dict_matches
            and field_info.obj_id not in create_dict_matches
        ):
            for r in dictionary_obj["data_regex"]["rules"]:
                if v is not None and re.search(r, v) is not None:
                    ctx.logger.debug(
                        '========> Process[%s]: check_sensitive_data: match by "%s", %s, %s'
                        % (name, str(r), str(v), str(field_info))
                    )
                    dict_matches[field_info.obj_id] = field_info
        else:
            break

    return dict_matches


def check_sens_pg_types(field_type: str):
    """Check if actual field type is sens."""
    for pg_type in SENS_PG_TYPES:
        if pg_type in field_type:
            return True
    return False


async def scan_obj_func(
    name,
    ctx,
    pool,
    field_info: FieldInfo,
    tagged_fields: List[Optional[TaggedFields]],
    scan_mode: ScanMode,
    dictionary_obj,
    scan_partial_rows,
):

    ctx.logger.debug("====>>> Process[%s]: Started task %s" % (name, str(field_info)))

    start_t = time.time()
    if not check_sens_pg_types(field_info.type):
        ctx.logger.debug(
            "========> Process[%s]: scan_obj_func: task %s skipped by field type %s"
            % (name, str(field_info), "[integer, text, bigint, character varying(x)]")
        )
        return None

    db_conn = await pool.acquire()
    res = {}
    scanning_flag = True
    try:
        for field in tagged_fields:
            if (
                field.nspname == field_info.nspname
                and field.relname == field_info.relname
                and field.column_name == field_info.column_name
            ):
                if ":sens" in field.column_comment:
                    res[field_info.obj_id] = field_info
                scanning_flag = False
                break

        if scan_mode == ScanMode.PARTIAL and scanning_flag:
            # TODO: Create check for bigger than 10MB fields
            fld_data = await db_conn.fetch(
                """SELECT distinct(\"%s\")::text FROM \"%s\".\"%s\" WHERE \"%s\" is not null LIMIT %s"""
                % (
                    field_info.column_name,
                    field_info.nspname,
                    field_info.relname,
                    field_info.column_name,
                    str(scan_partial_rows),
                )
            )
            res = check_sensitive_data_in_fld(
                ctx,
                name,
                dictionary_obj,
                ctx.create_dict_matches,
                field_info,
                setof_to_list(fld_data),
            )
        if scan_mode == ScanMode.FULL and scanning_flag:
            async with db_conn.transaction():
                cur = await db_conn.cursor(
                    """select distinct(\"%s\")::text from \"%s\".\"%s\" WHERE \"%s\" is not null"""
                    % (
                        field_info.column_name,
                        field_info.nspname,
                        field_info.relname,
                        field_info.column_name,
                    )
                )
                next_rows = True
                while next_rows:
                    fld_data = await cur.fetch(scan_partial_rows)
                    res = check_sensitive_data_in_fld(
                        ctx,
                        name,
                        dictionary_obj,
                        ctx.create_dict_matches,
                        field_info,
                        setof_to_list(fld_data),
                    )
                    if len(fld_data) == 0 or len(res) > 0:
                        next_rows = False
                        break

    except Exception as e:
        ctx.logger.error("Exception in scan_obj_func:\n" + exception_helper())
        raise Exception("Can't execute task: %s" % field_info)
    finally:
        await db_conn.close()
        await pool.release(db_conn)

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


def process_impl(
    name, ctx, queue, fields_info_chunk: List[FieldInfo], conn_params, threads: int
):
    tasks_res = []

    status_ratio = 10
    if len(fields_info_chunk) > 1000:
        status_ratio = 100
    if len(fields_info_chunk) > 50000:
        status_ratio = 1000

    async def run():
        pool = await asyncpg.create_pool(
            **conn_params, min_size=threads, max_size=threads
        )
        tasks = set()

        ctx.logger.info(
            "============> Started collecting list_tagged_fields in mode: create-dict"
        )
        tagged_fields = await get_tagged_fields(conn_params)
        ctx.logger.info(
            "<============ Finished collecting list_tagged_fields in mode: create-dict"
        )

        for idx, field_info in enumerate(fields_info_chunk):
            if len(tasks) >= threads:
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
                    tagged_fields,
                    ctx.args.scan_mode,
                    ctx.dictionary_obj,
                    ctx.args.scan_partial_rows,
                )
            )
            tasks_res.append(task_res)
            tasks.add(task_res)
            if idx % status_ratio:
                progress = (
                    str(round(float(idx) * 100 / len(fields_info_chunk), 2)) + "%"
                )
                ctx.logger.info("Process [%s] Progress %s" % (name, str(progress)))
        if len(tasks) > 0:
            await asyncio.wait(tasks)
        await pool.close()

    nest_asyncio.apply()
    loop = asyncio.new_event_loop()

    try:
        loop.run_until_complete(run())
    except asyncio.exceptions.TimeoutError:
        ctx.logger.error(
            "================> Process [%s]: asyncio.exceptions.TimeoutError" % name
        )
    finally:
        loop.close()

    tasks_res_final = []
    for v in tasks_res:
        if v.result() is not None and len(v.result()) > 0:
            tasks_res_final.append(v.result())

    queue.put(tasks_res_final)
    queue.put(None)  # Shut down the worker
    queue.close()


async def init_process(name, ctx, fields_info_chunk: List[FieldInfo]):
    start_t = time.time()
    ctx.logger.info(
        "================> Process [%s] started. Input items: %s"
        % (name, str(len(fields_info_chunk)))
    )
    queue = aioprocessing.AioQueue()

    p = aioprocessing.AioProcess(
        target=process_impl,
        args=(name, ctx, queue, fields_info_chunk, ctx.conn_params, ctx.args.processes),
    )
    p.start()
    res = None
    while True:
        result = await queue.coro_get()
        if result is None:
            break
        res = result
    await p.coro_join()
    end_t = time.time()
    ctx.logger.info(
        "<================ Process [%s] finished, elapsed: %s sec. Result %s item(s)"
        % (
            name,
            str(round(end_t - start_t, 2)),
            str(len(res)) if res is not None else "0",
        )
    )
    return res


def add_metadict_rule(dictionary_obj: dict, field_info: FieldInfo, anon_rules: dict):
    hash_func = (
        "anon_funcs.digest(\"%s\", 'salt_word', 'md5')"  # by default use md5 with salt
    )

    for fld_type, func in dictionary_obj["funcs"].items():
        if str(field_info.type).find(fld_type) > -1:
            hash_func = func

    res_hash_func = (
        hash_func if hash_func.find("%s") == -1 else hash_func % field_info.column_name
    )

    if field_info.tbl_id not in anon_rules:
        anon_rules[field_info.tbl_id] = {
            "schema": field_info.nspname,
            "table": field_info.relname,
            "fields": {field_info.column_name: res_hash_func},
        }
    else:
        anon_rules[field_info.tbl_id]["fields"].update(
            {field_info.column_name: res_hash_func}
        )
    return anon_rules


async def create_dict_impl(ctx):
    result = PgAnonResult()
    result.result_code = ResultCode.DONE

    fields_info: List[FieldInfo] = await generate_scan_objs(ctx)
    if not fields_info:
        raise Exception("No objects for create dictionary!")

    await check_sensitive_fld_names(ctx, fields_info)  # fill ctx.create_dict_matches

    # objs_prepared = recordset_to_list(fields_info)
    random.shuffle(fields_info)
    fields_info_chunks = list(chunkify(fields_info, ctx.args.threads))

    tasks = []
    for idx, fields_info_chunk in enumerate(fields_info_chunks):
        tasks.append(
            asyncio.ensure_future(init_process(str(idx + 1), ctx, fields_info_chunk))
        )
    await asyncio.wait(tasks)

    # create output dict
    output_dict = {}
    output_dict["dictionary"] = []
    anon_dict_rules = {}

    # ============================================================================================
    # Fill results based on processes
    # ============================================================================================
    for v in tasks:
        for res in v.result():
            for field_info in res.values():
                anon_dict_rules = add_metadict_rule(
                    ctx.dictionary_obj, field_info, anon_dict_rules
                )

    # ============================================================================================
    # Fill results based on check_sensitive_fld_names
    # ============================================================================================
    for field_info in ctx.create_dict_matches.values():
        anon_dict_rules = add_metadict_rule(
            ctx.dictionary_obj, field_info, anon_dict_rules
        )
    # ============================================================================================

    for _, v in anon_dict_rules.items():
        output_dict["dictionary"].append(v)

    output_dict_file = open(
        os.path.join(ctx.current_dir, "dict", ctx.args.output_dict_file), "w"
    )
    output_dict_file.write(json.dumps(output_dict, indent=4))
    output_dict_file.close()

    return result


async def create_dict(ctx):
    result = PgAnonResult()
    result.result_code = ResultCode.DONE
    ctx.logger.info("-------------> Started create_dict mode")

    try:
        dictionary_file = open(
            os.path.join(ctx.current_dir, "dict", ctx.args.dict_file), "r"
        )
        ctx.dictionary_content = dictionary_file.read()
        dictionary_file.close()
        ctx.dictionary_obj = eval(ctx.dictionary_content)
        await prepare_dictionary_obj(ctx)
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
