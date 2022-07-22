from common import *


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


async def generate_restore_queries(ctx, db_conn):
    return None


async def restore_obj_func(ctx, pool, task, sn_id):
    ctx.logger.info('================> Started task %s' % str(task))

    db_conn = await pool.acquire()
    try:
        tr = db_conn.transaction()
        await tr.start()
        try:
            await db_conn.execute("BEGIN ISOLATION LEVEL REPEATABLE READ;")
            sn_id = await db_conn.fetch("select pg_export_snapshot()")
            await make_restore_impl(ctx, db_conn, sn_id)
        except:
            await tr.rollback()
            raise
        else:
            await tr.commit()
        # BEGIN ISOLATION LEVEL REPEATABLE READ;
        # SET TRANSACTION SNAPSHOT '00000004-00000E7B-1';
    finally:
        await pool.release(db_conn)

    ctx.logger.info('================> Finished task %s' % str(task))


async def make_restore_impl(ctx, db_conn, sn_id):
    loop = asyncio.get_event_loop()
    tasks = set()
    pool = await asyncpg.create_pool(
        **ctx.conn_params,
        min_size=ctx.args.threads,
        max_size=ctx.args.threads
    )

    queries = await generate_restore_queries(ctx, db_conn)

    for v in queries:
        if len(tasks) >= ctx.args.threads:
            # Wait for some upload to finish before adding a new one
            done, tasks = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            exception = done.pop().exception()
            if exception is not None:
                await pool.close()
                raise exception
        if v != (None, None):
            tasks.add(loop.create_task(restore_obj_func(ctx, pool, v, sn_id)))

    # Wait for the remaining uploads to finish
    await asyncio.wait(tasks)
    await pool.close()


async def make_restore(ctx):
    ctx.logger.info("-------------> Started restore")

    if ctx.args.input_dir.find("""/""") == -1 and ctx.args.input_dir.find("""\\""") == -1:
        ctx.args.input_dir = os.path.join(ctx.current_dir, 'output', ctx.args.input_dir)

    if not os.path.exists(ctx.args.input_dir):
        msg = 'ERROR: input directory %s does not exists' % ctx.args.input_dir
        ctx.logger.error(msg)
        raise RuntimeError(msg)

    await run_pg_restore(ctx, 'pre-data')

    db_conn = await asyncpg.connect(**ctx.conn_params)
    result = True
    tr = db_conn.transaction()
    await tr.start()
    try:
        await db_conn.execute("BEGIN ISOLATION LEVEL REPEATABLE READ;")
        sn_id = await db_conn.fetchval("select pg_export_snapshot()")
        await make_restore_impl(ctx, db_conn, sn_id)
    except:
        ctx.logger.error("<------------- make_dump failed\n" + exception_helper())
        result = False
    finally:
        await tr.rollback()
        await db_conn.close()

    await run_pg_restore(ctx, 'post-data')
    ctx.logger.info("<------------- Finished restore")
    return result
