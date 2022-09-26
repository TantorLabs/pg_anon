from common import *
import os
import asyncpg
import asyncio
import json


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
        anon_funcs.digest(n.nspname || '.' || c.relname || '.' || a.attname, '', 'md5') as obj_id
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
    ORDER BY 1, 2, a.attnum
    """
    query_res = await db_conn.fetch(query)
    await db_conn.close()
    return query_res


async def scan_borders(ctx):
    db_conn = await asyncpg.connect(**ctx.conn_params)
    query = """
    -- get fld for batches
    SELECT
        c.oid,
        n.nspname,
        c.relname AS table_name,
        a.attname AS column_name,
        format_type(a.atttypid, a.atttypmod) as type,
        anon_funcs.digest(n.nspname || '.' || c.relname || '.' || a.attname, '', 'md5') as obj_id
        -- s.relname AS sequence_name
    FROM pg_class AS c
    JOIN pg_attribute AS a ON a.attrelid = c.oid
    JOIN pg_depend AS d ON d.refobjid = c.oid AND d.refobjsubid = a.attnum
    JOIN pg_class AS s ON s.oid = d.objid
    JOIN pg_namespace AS n ON n.oid = c.relnamespace
    WHERE
        c.relkind IN ('r', 'p')
        AND s.relkind = 'S'
        AND d.deptype = 'a'
        AND d.classid = 'pg_catalog.pg_class'::regclass
        AND d.refclassid = 'pg_catalog.pg_class'::regclass
    """

    borders_res = await db_conn.fetch(query)
    borders_res_dict = {}
    for v in borders_res:
        max_val = await db_conn.fetchval(
            """select max(%s) from \"%s\".\"%s\"""" % (
                v['column_name'],
                v['nspname'],
                v['table_name']
            )
        )
        borders_res_dict[v['obj_id']] = {
            "schema": v['nspname'],
            "table": v['table_name'],
            "pk": v['column_name'],
            "max_val": max_val
        }
    await db_conn.close()
    return borders_res_dict


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
        for r in ctx.dictionary_obj['field']['rules']:
            if re.search(r, v['column_name']) is not None:
                ctx.logger.debug(
                    '------> check_sensitive_fld_names: match by %s, removed %s' % (
                        str(r),
                        str(v)
                    )
                )
                objs.remove(v)
                ctx.create_dict_matches[v['obj_id']] = v


async def check_sensitive_data(ctx, task, fld_data):
    result = set.intersection(ctx.dictionary_obj['data_const']['constants'], fld_data)
    if len(result) > 0:
        if ctx.args.debug:
            ctx.logger.debug(
                '========> check_sensitive_data: match by constant %s , %s' % (
                    str(result),
                    str(task)
                )
            )
        ctx.create_dict_matches[task['obj_id']] = task

    for v in fld_data:
        if task['obj_id'] not in ctx.create_dict_matches:
            for r in ctx.dictionary_obj['data_regex']['rules']:
                if re.search(r, v) is not None:
                    if ctx.args.debug:
                        ctx.logger.debug(
                            '========> check_sensitive_data: match by %s, %s, %s' % (
                                str(r),
                                str(v),
                                str(task)
                            )
                        )
                    ctx.create_dict_matches[task['obj_id']] = task
        else:
            break


async def scan_obj_func(ctx, pool, task):
    # ctx.logger.debug('================> Started task %s' % str(task))
    db_conn = await pool.acquire()
    try:
        res = await db_conn.fetch(
            """select distinct(\"%s\")::text from \"%s\".\"%s\" limit 10000""" % (
                task['column_name'],
                task['nspname'],
                task['relname']
            )
        )
        fld_values = set()
        for v in res:
            for word in v[0].split():
                if len(word) > 3:
                    fld_values.add(word.lower())
        await check_sensitive_data(ctx, task, fld_values)
    except Exception as e:
        ctx.logger.error("Exception in scan_obj_func:\n" + exception_helper())
        raise Exception("Can't execute task: %s" % task)
    finally:
        await db_conn.close()
        await pool.release(db_conn)

    # ctx.logger.debug('<================ Finished task %s' % str(task))


async def create_dict_impl(ctx):
    result = PgAnonResult()
    loop = asyncio.get_event_loop()
    tasks = set()
    pool = await asyncpg.create_pool(
        **ctx.conn_params,
        min_size=ctx.args.threads,
        max_size=ctx.args.threads
    )

    objs = await generate_scan_objs(ctx)
    if not objs:
        await pool.close()
        raise Exception("No objects for create dictionary!")

    # borders = await scan_borders(ctx)   # currently ignored
    await check_sensitive_fld_names(ctx, objs)

    for v in objs:
        if len(tasks) >= ctx.args.threads:
            # Wait for some task to finish before adding a new one
            done, tasks = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            exception = done.pop().exception()
            if exception is not None:
                await pool.close()
                raise exception
        tasks.add(loop.create_task(scan_obj_func(ctx, pool, v)))

    # create output dict
    output_dict = {}
    output_dict["dictionary"] = []
    objs = {}

    for _, v in ctx.create_dict_matches.items():
        item = v['nspname'] + "." + v['relname']
        if item not in objs:
            objs[item] = {
                "schema": v['nspname'],
                "table": v['relname'],
                "fields": {
                    v["column_name"]: 'md5(%s)' % v["column_name"]
                }
            }
        else:
            objs[item]["fields"].update({v["column_name"]: 'md5(%s)' % v["column_name"]})

    for _, v in objs.items():
        output_dict["dictionary"].append(v)

    # print(json.dumps(output_dict, indent=4))
    # print(str(output_dict))

    output_dict_file = open(os.path.join(ctx.current_dir, 'dict', ctx.args.output_dict_file), 'w')
    output_dict_file.write(json.dumps(output_dict, indent=4))
    output_dict_file.close()

    # Wait for the remaining scans to finish
    await asyncio.wait(tasks)
    await pool.close()
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
