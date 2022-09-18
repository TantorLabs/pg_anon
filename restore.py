import os
import asyncpg
import asyncio
from common import *
import shutil
import json


async def run_pg_restore(ctx, section):
    os.environ["PGPASSWORD"] = ctx.args.db_user_password
    command = [
        ctx.args.pg_restore,
        "-h", ctx.args.db_host,
        "-p", str(ctx.args.db_port), "-v", "-w",
        "-U", ctx.args.db_user,
        "-d", ctx.args.db_name,
        os.path.join(
            ctx.args.input_dir,
            section.replace("-", "_") + ".backup"
        )
    ]
    if not ctx.args.db_host:
        del command[command.index("-h"):command.index("-h") + 2]

    if not ctx.args.db_user:
        del command[command.index("-U"):command.index("-U") + 2]

    ctx.logger.debug(str(command))
    proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    err, out = proc.communicate()
    for v in out.decode("utf-8").split("\n"):
        ctx.logger.info(v)


async def seq_init(ctx):
    db_conn = await asyncpg.connect(**ctx.conn_params)
    if ctx.args.seq_init_by_max_value:
        query = """
            DO $$
            DECLARE
                cmd text;
                schema text;
            BEGIN
                FOR cmd, schema IN (
                    select
                       ('SELECT setval(''' || T.seq_name || ''', max("' || T.column_name || '") + 1) FROM "' || T.table_name || '"') as cmd,
                       T.table_schema as schema
                    FROM (
                            select
                               substring(t.column_default from 10 for length(t.column_default) - 21) as seq_name,
                               t.table_schema,
                               t.table_name,
                               t.column_name
                               FROM (
                                   SELECT table_schema, table_name, column_name, column_default
                                   FROM information_schema.columns
                                   WHERE column_default LIKE 'nextval%'
                               ) T
                    ) T
                ) LOOP
                    EXECUTE 'SET search_path = ''' || schema || ''';';
                    -- EXECUTE cmd;
                    raise notice '%', cmd;
                END LOOP;
                SET search_path = 'public';
            END$$;"""
        ctx.logger.debug(query)
        await db_conn.execute(query)
    else:
        for _, v in ctx.metadata['seq_lastvals'].items():
            query = """
                SET search_path = '%s';
                SELECT setval(quote_ident('%s'), %s + 1);
            """ % (
                v['schema'].replace("'", "''"),
                v['seq_name'].replace("'", "''"),
                v['value']
            )
            ctx.logger.info(query)
            await db_conn.execute(query)

    await db_conn.close()


def generate_restore_queries(ctx):
    queries = []
    for file_name, target in ctx.metadata['files'].items():
        full_path = os.path.join(ctx.current_dir, 'output', ctx.args.input_dir, file_name)
        schema = target["schema"]
        table = target["table"]
        query = "COPY \"%s\".\"%s\" FROM PROGRAM 'gunzip -c %s' %s" % (
            schema,
            table,
            full_path,
            ctx.args.copy_options
        )
        queries.append(query)
    return queries


def generate_analyze_queries(ctx):
    analyze_queries = []
    for file_name, target in ctx.metadata['files'].items():
        schema = target["schema"]
        table = target["table"]
        analyze_query = "analyze \"%s\".\"%s\"" % (
            schema,
            table
        )
        analyze_queries.append(analyze_query)
    return analyze_queries


async def restore_obj_func(ctx, pool, task, sn_id):
    ctx.logger.info('================> Started task %s' % str(task))

    db_conn = await pool.acquire()
    try:
        await db_conn.execute("BEGIN ISOLATION LEVEL REPEATABLE READ;")
        await db_conn.execute("SET TRANSACTION SNAPSHOT '%s';" % sn_id)
        res = await db_conn.execute(task)
        ctx.total_rows += int(re.findall(r"(\d+)", res)[0])
        await db_conn.execute("COMMIT;")
        ctx.logger.debug("COPY %s [rows] Task: %s " % (ctx.total_rows, str(task)))
    except Exception as e:
        ctx.logger.error("Exception in restore_obj_func:\n" + exception_helper())
        raise Exception("Can't execute task: %s" % task)
    finally:
        await pool.release(db_conn)

    ctx.logger.info('================> Finished task %s' % str(task))


async def make_restore_impl(ctx, sn_id):
    pool = await asyncpg.create_pool(
        **ctx.conn_params,
        min_size=ctx.args.threads,
        max_size=ctx.args.threads
    )

    queries = generate_restore_queries(ctx)
    loop = asyncio.get_event_loop()
    tasks = set()
    for v in queries:
        if len(tasks) >= ctx.args.threads:
            # Wait for some restore to finish before adding a new one
            done, tasks = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            exception = done.pop().exception()
            if exception is not None:
                await pool.close()
                raise exception
        tasks.add(loop.create_task(restore_obj_func(ctx, pool, v, sn_id)))

    # Wait for the remaining restores to finish
    await asyncio.wait(tasks)
    await pool.close()


async def check_free_disk_space(ctx, db_conn):
    data_directory_location = await db_conn.fetchval(
        """
        SELECT setting
        FROM pg_settings
        WHERE name = 'data_directory'
        """
    )
    disk_size = shutil.disk_usage(data_directory_location)
    ctx.logger.info("Free disk space: " + pretty_size(disk_size.free))
    ctx.logger.info("Required disk space: " + pretty_size(int(ctx.metadata['total_tables_size']) * 1.5))
    if disk_size.free < int(ctx.metadata['total_tables_size']) * 1.5:
        raise Exception(
            "Not enough freed disk space! Free %s, Required %s" % (
                pretty_size(disk_size.free),
                pretty_size(int(ctx.metadata['total_tables_size']) * 1.5)
            )
        )


async def make_restore(ctx):
    result = PgAnonResult()
    ctx.logger.info("-------------> Started restore")

    if ctx.args.input_dir.find("""/""") == -1 and ctx.args.input_dir.find("""\\""") == -1:
        ctx.args.input_dir = os.path.join(ctx.current_dir, 'output', ctx.args.input_dir)

    if not os.path.exists(ctx.args.input_dir):
        msg = 'ERROR: input directory %s does not exists' % ctx.args.input_dir
        ctx.logger.error(msg)
        raise RuntimeError(msg)

    db_conn = await asyncpg.connect(**ctx.conn_params)
    db_is_empty = await db_conn.fetchval("""
        SELECT NOT EXISTS(
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_schema not in (
                    'pg_catalog',
                    'information_schema',
                    'anon_funcs'
                ) AND table_type = 'BASE TABLE'
        )""")

    if not db_is_empty and ctx.args.mode != AnonMode.SYNC_DATA_RESTORE:
        raise Exception("Target DB is not empty!")

    metadata_file = open(os.path.join(ctx.current_dir, 'dict', ctx.args.input_dir, 'metadata.json'), 'r')
    metadata_content = metadata_file.read()
    metadata_file.close()
    ctx.metadata = eval(metadata_content)

    if not ctx.args.disable_checks:
        if get_major_version(ctx.pg_version) < get_major_version(ctx.metadata['pg_version']):
            raise Exception(
                "Target PostgreSQL major version %s is below than source %s!" % (
                    ctx.pg_version,
                    ctx.metadata['pg_version']
                )
            )
        if get_major_version(get_pg_util_version(ctx.args.pg_restore)) < get_major_version(ctx.metadata['pg_dump_version']):
            raise Exception(
                "pg_restore major version %s is below than source pg_dump version %s!" % (
                    get_pg_util_version(ctx.args.pg_restore),
                    ctx.metadata['pg_dump_version']
                )
            )
        await check_free_disk_space(ctx, db_conn)

    if ctx.args.mode != AnonMode.SYNC_DATA_DUMP:
        await run_pg_restore(ctx, 'pre-data')

    if ctx.args.drop_custom_check_constr:
        # drop all CHECK constrains containing user-defined procedures to avoid
        # performance degradation at the data loading stage
        check_constraints = await db_conn.fetch("""
            SELECT nsp.nspname,  cl.relname, pc.conname, pg_get_constraintdef(pc.oid)
            -- pc.consrc removed in 12 version
            FROM (
                SELECT substring(T.v FROM position(' ' in T.v) + 1 for length(T.v) )::bigint as func_oid, t.conoid
                from (
                    SELECT T.v as v, t.conoid
                    FROM (
                            SELECT ((SELECT regexp_matches(t.v, '(:funcid\s\d+)', 'g'))::text[])[1] as v, t.conoid
                            FROM (
                                SELECT conbin::text as v, oid as conoid
                                FROM pg_constraint
                                WHERE contype = 'c'
                            ) T
                    ) T WHERE length(T.v) > 0
                ) T
            ) T
            INNER JOIN pg_constraint pc on T.conoid = pc.oid
            INNER JOIN pg_class cl on cl.oid = pc.conrelid
            INNER JOIN pg_namespace nsp on cl.relnamespace = nsp.oid
            WHERE T.func_oid in (
                SELECT  p.oid
                FROM    pg_namespace n
                INNER JOIN pg_proc p ON p.pronamespace = n.oid
                WHERE   n.nspname not in ( 'pg_catalog', 'information_schema' )
            )
        """)

        if check_constraints is not None:
            for conn in check_constraints:
                ctx.logger.info("Removing constraints: " + conn[2])
                query = 'ALTER TABLE "{0}"."{1}" DROP CONSTRAINT IF EXISTS "{2}" CASCADE'.format(conn[0], conn[1], conn[2])
                await db_conn.execute(query)

    result.result_code = ResultCode.DONE
    if ctx.args.mode != AnonMode.SYNC_STRUCT_RESTORE:
        tr = db_conn.transaction()
        await tr.start()
        try:
            await db_conn.execute("BEGIN ISOLATION LEVEL REPEATABLE READ;")
            await db_conn.execute("SET CONSTRAINTS ALL DEFERRED;")
            sn_id = await db_conn.fetchval("select pg_export_snapshot()")
            await make_restore_impl(ctx, sn_id)
        except:
            ctx.logger.error("<------------- make_restore failed\n" + exception_helper())
            result.result_code = "fail"
        finally:
            await tr.commit()
            await db_conn.close()

        if ctx.total_rows != int(ctx.metadata["total_rows"]):
            ctx.logger.error("The number of restored rows (%s) is different from the metadata (%s)" % (
                    str(ctx.total_rows),
                    ctx.metadata["total_rows"]
                )
            )
            result.result_code = ResultCode.FAIL

    if ctx.args.mode != AnonMode.SYNC_DATA_RESTORE:
        await run_pg_restore(ctx, 'post-data')

    if ctx.args.mode != AnonMode.SYNC_STRUCT_RESTORE:
        await seq_init(ctx)

    ctx.logger.info("<------------- Finished restore")
    return result


async def run_custom_query(ctx, pool, query):
    # in single tx
    ctx.logger.info('================> Started query %s' % str(query))

    db_conn = await pool.acquire()
    try:
        await db_conn.execute(query)
    except Exception as e:
        ctx.logger.error("Exception in dump_obj_func:\n" + exception_helper())
        raise Exception("Can't execute query: %s" % query)
    finally:
        await db_conn.close()
        await pool.release(db_conn)

    ctx.logger.info('<================ Finished query %s' % str(query))


async def run_analyze(ctx):
    ctx.logger.info("-------------> Started analyze")
    pool = await asyncpg.create_pool(
        **ctx.conn_params,
        min_size=ctx.args.threads,
        max_size=ctx.args.threads
    )

    queries = generate_analyze_queries(ctx)
    loop = asyncio.get_event_loop()
    tasks = set()
    for v in queries:
        if len(tasks) >= ctx.args.threads:
            done, tasks = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            exception = done.pop().exception()
            if exception is not None:
                await pool.close()
                raise exception
        tasks.add(loop.create_task(run_custom_query(ctx, pool, v)))

    # Wait for the remaining queries to finish
    await asyncio.wait(tasks)
    await pool.close()
    ctx.logger.info("<------------- Finished analyze")


async def validate_restore(ctx):
    result = PgAnonResult()
    result.result_code = ResultCode.DONE
    ctx.logger.info("-------------> Started validate_restore")

    try:
        dictionary_file = open(os.path.join(ctx.current_dir, 'dict', ctx.args.dict_file), 'r')
        ctx.dictionary_content = dictionary_file.read()
        dictionary_file.close()
        dictionary_obj = eval(ctx.dictionary_content)
    except:
        ctx.logger.error(exception_helper(show_traceback=True))
        return [], {}

    if "validate_tables" in dictionary_obj:
        db_conn = await asyncpg.connect(**ctx.conn_params)
        db_objs = await db_conn.fetch("""
            select n.nspname, c.relname --, c.reltuples
            from pg_class c
            join pg_namespace n on c.relnamespace = n.oid
            where
                c.relkind = 'r' and
                n.nspname not in ('pg_catalog', 'information_schema') and
                c.reltuples > 0
        """)
        await db_conn.close()
        tables_in_target_db = []
        for item in db_objs:
            tables_in_target_db.append(item[0] + "." + item[1])

        tables_in_dict = []
        for d in dictionary_obj["validate_tables"]:
            tables_in_dict.append(d["schema"] + "." + d["table"])

        diff_l = list(set(tables_in_target_db) - set(tables_in_dict))
        diff_r = list(set(tables_in_dict) - set(tables_in_target_db))

        if len(diff_r) > 0:
            ctx.logger.error("validate_tables: in target DB not found tables:\n%s" % json.dumps(diff_r, indent=4))
            result.result_code = ResultCode.FAIL

        if len(diff_l) > 0:
            ctx.logger.error(
                """validate_tables: non-empty tables were found in target database that
                are not described in the dictionary:\n%s""" % json.dumps(diff_l, indent=4)
            )
            result.result_code = ResultCode.FAIL
    else:
        ctx.logger.error("Section validate_tables is not found in dictionary!")
        result.result_code = ResultCode.FAIL

    ctx.logger.info("<------------- Finished validate_restore")
    return result
