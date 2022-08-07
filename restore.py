import os
import asyncpg
import asyncio
from common import *
import shutil


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
        await db_conn.execute(task)
        await db_conn.execute("COMMIT;")
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
            WHERE table_schema not in ( 'pg_catalog', 'information_schema' ) AND table_type = 'BASE TABLE'
        )""")

    if not db_is_empty:
        raise Exception("Target DB is not empty!")

    metadata_file = open(os.path.join(ctx.current_dir, 'dict', ctx.args.input_dir, 'metadata.json'), 'r')
    metadata_content = metadata_file.read()
    metadata_file.close()
    ctx.metadata = eval(metadata_content)

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
    await run_pg_restore(ctx, 'pre-data')

    # drop all CHECK constrains containing user-defined procedures to avoid
    # performance degradation at the data loading stage
    check_constraints = await db_conn.fetchval("""
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

    result = True
    tr = db_conn.transaction()
    await tr.start()
    try:
        await db_conn.execute("BEGIN ISOLATION LEVEL REPEATABLE READ;")
        await db_conn.execute("SET CONSTRAINTS ALL DEFERRED;")
        sn_id = await db_conn.fetchval("select pg_export_snapshot()")
        await make_restore_impl(ctx, sn_id)
    except:
        ctx.logger.error("<------------- make_restore failed\n" + exception_helper())
        result = False
    finally:
        await tr.commit()
        await db_conn.close()

    await run_pg_restore(ctx, 'post-data')
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
