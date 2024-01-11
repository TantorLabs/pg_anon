import random
import time
from common import *
import os
import asyncpg
import asyncio
import json
import aioprocessing
import nest_asyncio


async def list_tagged_fields(ctx):
    db_conn = await asyncpg.connect(**ctx.conn_params)
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
        and description like '%:sens%'
    ORDER BY
        nspname,
        relname,
        attname;
    """
    query_res = await db_conn.fetch(query)
    await db_conn.close()
    return [{'nspname': record['schema_name'],
             'relname': record['table_name'],
             'column_name': record['column_name'],
             'column_comment': record['column_comment']} for record in query_res]


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
        -- AND c.relname = 'card_numbers'  -- debug
    ORDER BY 1, 2, a.attnum
    """
    query_res = await db_conn.fetch(query)
    await db_conn.close()

    def check_skip_fields(fld):
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
                if ctx.args.debug:
                    ctx.logger.debug(
                        '!!! ------> check_skip_fields: filtered fld %s by rule %s' % (
                            str(dict(fld)),
                            str(v)
                        )
                    )
                return res

        return True

    return [fld for fld in query_res if check_skip_fields(fld)]


async def prepare_dictionary_obj(ctx):
    ctx.dictionary_obj['data_const']['constants'] = set(ctx.dictionary_obj['data_const']['constants'])

    regex_for_compile = []
    for v in ctx.dictionary_obj['data_regex']['rules']:
        regex_for_compile.append(re.compile(v))

    ctx.dictionary_obj['data_regex']['rules'] = regex_for_compile.copy()

    regex_for_compile = []
    for v in ctx.dictionary_obj['field']['rules']:
        regex_for_compile.append(re.compile(v))

    ctx.dictionary_obj['field']['rules'] = regex_for_compile.copy()


async def check_sensitive_fld_names(ctx, objs):
    for v in objs:
        if 'rules' in ctx.dictionary_obj['field']:
            for r in ctx.dictionary_obj['field']['rules']:
                if re.search(r, v['column_name']) is not None:
                    if ctx.args.debug:
                        ctx.logger.debug(
                            '!!! ------> check_sensitive_fld_names: match by "%s", removed %s' % (
                                str(r),
                                str(dict(v))
                            )
                        )
                    #objs.remove(v)
                    ctx.create_dict_matches[v['obj_id']] = v

        if 'constants' in ctx.dictionary_obj['field']:
            for r in ctx.dictionary_obj['field']['constants']:
                if r == v['column_name']:
                    if ctx.args.debug:
                        ctx.logger.debug(
                            '!!! ------> check_sensitive_fld_names: match by "%s", removed %s' % (
                                str(r),
                                str(dict(v))
                            )
                        )
                    objs.remove(v)
                    ctx.create_dict_matches[v['obj_id']] = v


def check_sensitive_data_in_fld(name, ctx, task, fld_data):
    if task['relname'] == 'card_numbers':
        x = 1
    fld_data_set = set()
    create_dict_matches = {}
    for v in fld_data:
        if v is None:
            continue
        for word in v.split():
            if len(word) >= 5:
                fld_data_set.add(word.lower())

    result = set.intersection(ctx.dictionary_obj['data_const']['constants'], fld_data_set)
    if len(result) > 0:
        if ctx.args.debug:
            ctx.logger.debug(
                '========> Process[%s]: check_sensitive_data: match by constant %s , %s' % (
                    name,
                    str(result),
                    str(task)
                )
            )
        create_dict_matches[task['obj_id']] = task

    for v in fld_data:
        if task['obj_id'] not in create_dict_matches and task['obj_id'] not in ctx.create_dict_matches:
            for r in ctx.dictionary_obj['data_regex']['rules']:
                if v is not None and re.search(r, v) is not None:
                    if ctx.args.debug:
                        ctx.logger.debug(
                            '========> Process[%s]: check_sensitive_data: match by "%s", %s, %s' % (
                                name,
                                str(r),
                                str(v),
                                str(task)
                            )
                        )
                    create_dict_matches[task['obj_id']] = task
        else:
            break

    return create_dict_matches


async def scan_obj_func(name, ctx, pool, task, tagged_fields):
    if ctx.args.debug:
        ctx.logger.debug('====>>> Process[%s]: Started task %s' % (name, str(task)))

    start_t = time.time()
    if not(
            task["type"] in ('text', 'integer', 'bigint') or
            task["type"].find("character varying") > -1
    ):
        if ctx.args.debug:
            ctx.logger.debug(
                '========> Process[%s]: scan_obj_func: task %s skipped by field type %s' % (
                    name,
                    str(task),
                    '[integer, text, bigint, character varying(x)]'
                )
            )
        return None

    db_conn = await pool.acquire()
    res = {}
    scanning_flag = True
    try:
        for field in tagged_fields:
            if (
                    field['nspname'] == task['nspname'] and
                    field['relname'] == task['relname'] and
                    field['column_name'] == task['column_name']):
                res[task['obj_id']] = task
                scanning_flag = False
                break

        if ctx.args.scan_mode == ScanMode.PARTIAL and scanning_flag:
            fld_data = await db_conn.fetch(
                """SELECT distinct(\"%s\")::text FROM \"%s\".\"%s\" WHERE \"%s\" is not null LIMIT %s""" % (
                    task['column_name'],
                    task['nspname'],
                    task['relname'],
                    task['column_name'],
                    str(ctx.args.scan_partial_rows)
                )
            )
            res = check_sensitive_data_in_fld(name, ctx, task, setof_to_list(fld_data))
        if ctx.args.scan_mode == ScanMode.FULL and scanning_flag:
            async with db_conn.transaction():
                cur = await db_conn.cursor(
                    """select distinct(\"%s\")::text from \"%s\".\"%s\" WHERE \"%s\" is not null""" % (
                        task['column_name'],
                        task['nspname'],
                        task['relname'],
                        task['column_name']
                    )
                )
                next_rows = True
                while next_rows:
                    fld_data = await cur.fetch(ctx.args.scan_partial_rows)
                    res = check_sensitive_data_in_fld(name, ctx, task, setof_to_list(fld_data))
                    if len(fld_data) == 0 or len(res) > 0:
                        next_rows = False
                        break

    except Exception as e:
        ctx.logger.error("Exception in scan_obj_func:\n" + exception_helper())
        raise Exception("Can't execute task: %s" % task)
    finally:
        await db_conn.close()
        await pool.release(db_conn)

    end_t = time.time()
    if end_t - start_t > 10:
        ctx.logger.warning("!!! Process[%s]: scan_obj_func took %s sec. Task %s" % (
                name,
                str(round(end_t - start_t, 2)),
                str(task)
            )
        )

    if ctx.args.debug:
        ctx.logger.debug(
            '<<<<==== Process[%s]: Found %s items(s) Finished task %s ' % (
                name,
                str(len(res)),
                str(task)
            )
        )
    return res


def process_impl(name, ctx, queue, items):
    tasks_res = []

    status_ratio = 10
    if len(items) > 1000:
        status_ratio = 100
    if len(items) > 50000:
        status_ratio = 1000

    async def run():
        pool = await asyncpg.create_pool(
            **ctx.conn_params,
            min_size=ctx.args.threads,
            max_size=ctx.args.threads
        )
        tasks = set()

        ctx.logger.info('============> Started collecting list_tagged_fields in mode: create-dict')
        tagged_fields = await list_tagged_fields(ctx)
        ctx.logger.info('<============ Finished collecting list_tagged_fields in mode: create-dict')

        for i, item in enumerate(items):
            if len(tasks) >= ctx.args.threads:
                done, tasks = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                exception = done.pop().exception()
                if exception is not None:
                    await pool.close()
                    raise exception

            task_res = loop.create_task(scan_obj_func(name, ctx, pool, item, tagged_fields))
            tasks_res.append(task_res)
            tasks.add(task_res)
            if i % status_ratio:
                progress = str(round(float(i) * 100 / len(items), 2)) + "%"
                ctx.logger.info('Process [%s] Progress %s' % (name, str(progress)))
        if len(tasks) > 0:
            await asyncio.wait(tasks)
        await pool.close()

    nest_asyncio.apply()
    loop = asyncio.new_event_loop()

    try:
        loop.run_until_complete(run())
    except asyncio.exceptions.TimeoutError:
        ctx.logger.error('================> Process [%s]: asyncio.exceptions.TimeoutError' % name)
    finally:
        loop.close()

    tasks_res_final = []
    for v in tasks_res:
        if v.result() is not None and len(v.result()) > 0:
            tasks_res_final.append(v.result())

    queue.put(tasks_res_final)
    queue.put(None)     # Shut down the worker
    queue.close()


async def init_process(name, ctx, items):
    start_t = time.time()
    ctx.logger.info('================> Process [%s] started. Input items: %s' % (name, str(len(items))))
    queue = aioprocessing.AioQueue()

    p = aioprocessing.AioProcess(target=process_impl, args=(name, ctx, queue, items))
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
        '<================ Process [%s] finished, elapsed: %s sec. Result %s item(s)' % (
            name,
            str(round(end_t - start_t, 2)),
            str(len(res)) if res is not None else "0"
        )
    )
    return res


async def create_dict_impl(ctx):
    result = PgAnonResult()
    result.result_code = ResultCode.DONE

    objs = await generate_scan_objs(ctx)
    if not objs:
        raise Exception("No objects for create dictionary!")

    await check_sensitive_fld_names(ctx, objs)  # fill ctx.create_dict_matches

    objs_prepared = recordset_to_list(objs)
    random.shuffle(objs_prepared)
    part_objs = list(chunkify(objs_prepared,  ctx.args.threads))

    tasks = []
    for i, part in enumerate(part_objs):
        tasks.append(
            asyncio.ensure_future(init_process(str(i + 1), ctx, part))
        )
    await asyncio.wait(tasks)

    # create output dict
    output_dict = {}
    output_dict["dictionary"] = []
    anon_dict_rules = {}

    def fill_res_dict(dict_val):
        hash_func = "anon_funcs.digest(\"%s\", 'salt_word', 'md5')"   # by default use md5 with salt
        for fld_type, func in ctx.dictionary_obj["funcs"].items():
            if str(dict_val['type']).find(fld_type) > -1:
                hash_func = func

        res_hash_func = hash_func if hash_func.find("%s") == -1 else hash_func % dict_val["column_name"]

        if dict_val['tbl_id'] not in anon_dict_rules:
            anon_dict_rules[dict_val['tbl_id']] = {
                "schema": dict_val['nspname'],
                "table": dict_val['relname'],
                "fields": {
                    dict_val["column_name"]: res_hash_func
                }
            }
        else:
            anon_dict_rules[dict_val['tbl_id']]["fields"].update(
                {
                    dict_val["column_name"]: res_hash_func
                }
            )

    # ============================================================================================
    # Fill results based on processes
    # ============================================================================================
    for v in tasks:
        for res in v.result():
            for _, val in res.items():
                fill_res_dict(val)
    # ============================================================================================
    # Fill results based on check_sensitive_fld_names
    # ============================================================================================
    for _, v in ctx.create_dict_matches.items():
        fill_res_dict(v)
    # ============================================================================================

    for _, v in anon_dict_rules.items():
        output_dict["dictionary"].append(v)

    output_dict_file = open(os.path.join(ctx.current_dir, 'dict', ctx.args.output_dict_file), 'w')
    output_dict_file.write(json.dumps(output_dict, indent=4))
    output_dict_file.close()

    return result


async def create_dict(ctx):
    result = PgAnonResult()
    result.result_code = ResultCode.DONE
    ctx.logger.info("-------------> Started create_dict mode")

    try:
        dictionary_file = open(os.path.join(ctx.current_dir, 'dict', ctx.args.dict_file), 'r')
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
