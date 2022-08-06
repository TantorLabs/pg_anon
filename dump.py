from datetime import datetime
import json
import os
import asyncpg
import asyncio
from hashlib import sha256
from common import *


async def run_pg_dump(ctx, section):
    os.environ["PGPASSWORD"] = ctx.args.db_user_password
    command = [
        ctx.args.pg_dump,
        "-h", ctx.args.db_host,
        "-p", str(ctx.args.db_port), "-v", "-w",
        "-U", ctx.args.db_user,
        "--exclude-schema", "anon_funcs",
        "--section", section, "-E", "UTF8", "-F", "c", "-s", "-f",
        os.path.join(
            ctx.args.output_dir,
            section.replace("-", "_") + ".backup"
        ),
        ctx.args.db_name
    ]
    if not ctx.args.db_host:
        del command[command.index("-h"):command.index("-h") + 2]

    ctx.logger.debug(str(command))
    proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    err, out = proc.communicate()
    if err.decode("utf-8") != "":
        msg = 'ERROR: database schema dump has failed! \n%s' % err.decode("utf-8")
        ctx.logger.error(msg)
        raise RuntimeError(msg)

    for v in out.decode("utf-8").split("\n"):
        ctx.logger.info(v)


async def dump_obj_func(ctx, pool, task, sn_id):
    ctx.logger.info('================> Started task %s' % str(task))

    db_conn = await pool.acquire()
    try:
        await db_conn.execute("BEGIN ISOLATION LEVEL REPEATABLE READ;")
        await db_conn.execute("SET TRANSACTION SNAPSHOT '%s';" % sn_id)
        res = await db_conn.execute(task)
        ctx.task_results[hash(task)] = re.findall(r"(\d+)", res)[0]
    except Exception as e:
        ctx.logger.error("Exception in dump_obj_func:\n" + exception_helper())
        raise Exception("Can't execute task: %s" % task)
        # asyncpg.exceptions.ExternalRoutineError: program "gzip > ... chm_other_1.dat.gz" failed
        # usermod -a -G current_user_name postgres
        # chmod -R g+rw /home/current_user_name/Desktop/pg_anon
        # chmod g+x /home/current_user_name/Desktop/pg_anon/output/test
        # su - postgres
        # touch /home/current_user_name/Desktop/pg_anon/test/1.txt
    finally:
        await db_conn.close()
        await pool.release(db_conn)

    ctx.logger.info('<================ Finished task %s' % str(task))


async def generate_dump_queries(ctx, db_conn):
    def check_obj_exists(dictionary_obj, schema, table):
        for v in dictionary_obj:
            if "schema" in v and schema == v["schema"] and table == v["table"]:
                return v
            if "schema" not in v and table == v["table"] and schema == 'public':
                return v
        return None

    def check_obj_exclude(dictionary_obj, schema, table):
        for v in dictionary_obj:
            if "schema" in v and schema == v["schema"] and table == v["table"]:
                return True
            if "schema" not in v and table == v["table"] and schema == 'public':
                return True
            # regex part
            if "schema" not in v and schema == 'public' and v["table"].startswith('REGEX:'):
                if re.search(v["table"][6:].strip(), table):
                    return True
            if "schema" in v and schema == v["schema"] and v["table"].startswith('REGEX:'):
                if re.search(v["table"][6:].strip(), table):
                    return True
        return False

    db_objs = await db_conn.fetch("""
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE
            table_schema not in ('pg_catalog', 'information_schema') and
            table_type = 'BASE TABLE'
    """)

    dictionary_file = open(os.path.join(ctx.current_dir, 'dict', ctx.args.dict_file), 'r')
    ctx.dictionary_content = dictionary_file.read()
    dictionary_file.close()
    dictionary_obj = eval(ctx.dictionary_content)

    queries = []
    files = {}

    for item in db_objs:
        table_name = "\"" + item[0] + "\".\"" + item[1] + "\""
        if check_obj_exclude(dictionary_obj['dictionary_exclude'], item[0], item[1]):
            ctx.logger.info("Skipping: " + str(table_name))
            continue

        file_name = "%s.dat.gz" % os.path.join(ctx.args.output_dir, item[0] + "_" + item[1])
        files["%s.dat.gz" % (item[0] + "_" + item[1])] = {"schema": item[0], "table": item[1]}

        a_obj = check_obj_exists(dictionary_obj['dictionary'], item[0], item[1])

        if a_obj is None:
            if not ctx.args.validate_dict:
                query = "COPY (SELECT * FROM %s %s) to PROGRAM 'gzip > %s' %s" % (
                    table_name,
                    (ctx.validate_limit if ctx.args.validate_full else ""),
                    file_name,
                    ctx.args.copy_options
                )
                ctx.logger.info(str(query))
                queries.append(query)
            else:
                query = "SELECT * FROM %s %s" % (table_name, ctx.validate_limit)
                ctx.logger.info(str(query))
                queries.append(query)
        else:
            if "raw_sql" in a_obj:
                if not ctx.args.validate_dict:
                    query = "COPY (%s %s) to PROGRAM 'gzip > %s' %s" % (
                        a_obj['raw_sql'],
                        (ctx.validate_limit if ctx.args.validate_full else ""),
                        file_name,
                        ctx.args.copy_options
                    )
                    ctx.logger.info(str(query))
                    queries.append(query)
                else:
                    query = a_obj['raw_sql'] + ctx.validate_limit
                    ctx.logger.info(str(query))
                    queries.append(query)
            else:
                fields_list = await db_conn.fetch("""
                    SELECT column_name FROM information_schema.columns 
                    WHERE table_schema = '""" + item[0] + """' AND table_name='""" + item[1] + """'
                    ORDER BY ordinal_position ASC
                """)

                sql_expr = ""

                def check_fld(fld_name):
                    if fld_name in a_obj["fields"]:
                        return [fld_name, a_obj["fields"][fld_name]]
                    return None

                for cnt, v in enumerate(fields_list):
                    a_fld = check_fld(v[0])
                    if a_fld is not None:
                        fld_name = a_fld[0]
                        fld_val = a_fld[1]
                        if fld_val.find("SQL:") == 0:
                            sql_expr += "(" + fld_val[4:] + ") as \"" + fld_name + "\""
                        else:
                            sql_expr += fld_val + " as \"" + fld_name + "\""
                    else:
                        # field "as is"
                        if (not v[0].islower() and not v[0].isupper()) or v[0].isupper():
                            sql_expr += "\"" + v[0] + "\" as \"" + v[0] + "\""
                        else:
                            sql_expr += " " + v[0] + " as \"" + v[0] + "\""
                    if cnt != len(fields_list) - 1:
                        sql_expr += ",\n"

                if not ctx.args.validate_dict:
                    query = "COPY (SELECT %s FROM %s %s) to PROGRAM 'gzip > %s' %s" % (
                        sql_expr,
                        table_name,
                        (ctx.validate_limit if ctx.args.validate_full else ""),
                        file_name,
                        ctx.args.copy_options
                    )
                    ctx.logger.info(str(query))
                    queries.append(query)
                else:
                    query = "SELECT %s FROM %s %s" % (sql_expr, table_name, ctx.validate_limit)
                    ctx.logger.info(str(query))
                    queries.append(query)

    return queries, files


async def make_dump_impl(ctx, db_conn, sn_id):
    loop = asyncio.get_event_loop()
    tasks = set()
    pool = await asyncpg.create_pool(
        **ctx.conn_params,
        min_size=ctx.args.threads,
        max_size=ctx.args.threads
    )

    queries, files = await generate_dump_queries(ctx, db_conn)
    zipped_list = list(zip([hash(v) for v in queries], files))

    for v in queries:
        if len(tasks) >= ctx.args.threads:
            # Wait for some dump to finish before adding a new one
            done, tasks = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            exception = done.pop().exception()
            if exception is not None:
                await pool.close()
                raise exception
        tasks.add(loop.create_task(dump_obj_func(ctx, pool, v, sn_id)))

    # Wait for the remaining dumps to finish
    await asyncio.wait(tasks)
    await pool.close()

    # Generate metadata.json
    query = """
        SELECT
            pn_t.nspname,
            t.relname AS table_name,
            a.attname AS column_name,
            pn_s.nspname,
            s.relname AS sequence_name
        FROM pg_class AS t
        JOIN pg_attribute AS a ON a.attrelid = t.oid
        JOIN pg_depend AS d ON d.refobjid = t.oid AND d.refobjsubid = a.attnum
        JOIN pg_class AS s ON s.oid = d.objid
        JOIN pg_namespace AS pn_t ON pn_t.oid = t.relnamespace
        JOIN pg_namespace AS pn_s ON pn_s.oid = s.relnamespace
        WHERE
            t.relkind IN ('r', 'P')
            AND s.relkind = 'S'
            AND d.deptype = 'a'
            AND d.classid = 'pg_catalog.pg_class'::regclass
            AND d.refclassid = 'pg_catalog.pg_class'::regclass
        """
    ctx.logger.debug(str(query))

    seq_res = await db_conn.fetch(query)
    seq_res_dict = {}
    for v in seq_res:
        seq_name = v[3] + "." + v[4]
        seq_val = await db_conn.fetchval("""select last_value from \"""" + v[3] + """\".\"""" + v[4] + "\"")

        for _, f in files.items():
            if v[0] == f["schema"] and v[1] == f["table"]:
                seq_res_dict[seq_name] = {"schema": v[3], "seq_name": v[4], "value": seq_val}

    metadata = {}
    metadata["db_size"] = await db_conn.fetchval("""SELECT pg_database_size('""" + ctx.args.db_name + """')""")
    metadata["created"] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    metadata["seq_lastvals"] = seq_res_dict
    metadata["pg_version"] = ctx.pg_version
    metadata["pg_dump_version"] = get_pg_util_version(ctx.args.pg_dump)
    metadata["dictionary_content_hash"] = sha256(ctx.dictionary_content.encode('utf-8')).hexdigest()
    metadata["dict_file"] = ctx.args.dict_file

    for v in zipped_list:
        files[v[1]].update({"rows": ctx.task_results[v[0]]})

    metadata["files"] = files
    with open(os.path.join(ctx.args.output_dir, "metadata.json"), "w") as out_file:
        out_file.write(json.dumps(metadata, indent=4))


async def make_dump(ctx):
    ctx.logger.info("-------------> Started dump mode")

    try:
        if ctx.args.output_dir.find("""/""") != -1 or ctx.args.output_dir.find("""\\""") != -1:
            output_dir = ctx.args.output_dir
        else:
            output_dir = os.path.join(ctx.current_dir, 'output', os.path.splitext(ctx.args.dict_file)[0])
            ctx.args.output_dir = output_dir
            dir_exists = os.path.exists(output_dir)
            if not dir_exists:
                os.makedirs(output_dir)

        if not ctx.args.validate_dict:
            dir_empty = True
            for root, dirs, files in os.walk(output_dir):
                for _ in files:
                    dir_empty = False
                    break

            if not dir_empty and not ctx.args.clear_output_dir:
                msg = "Output directory " + output_dir + " is not empty!"
                ctx.logger.error(msg)
                raise Exception(msg)

            if not dir_empty and ctx.args.clear_output_dir:
                for root, dirs, files in os.walk(output_dir):
                    for file in files:
                        if file.endswith('.sql') or file.endswith('.gz') or \
                                file.endswith('.json') or file.endswith('.backup'):
                            os.remove(os.path.join(root, file))
                        else:
                            msg = "Option --clear-output-dir enabled. Unexpected file extension: %s" % \
                                  os.path.join(root, file)
                            ctx.logger.error(msg)
                            raise Exception(msg)

        ctx.logger.info("-------------> Started pg_dump")
        await run_pg_dump(ctx, 'pre-data')
        await run_pg_dump(ctx, 'post-data')
        ctx.logger.info("<------------- Finished pg_dump")
    except:
        ctx.logger.error("<------------- make_dump failed\n" + exception_helper())
        return False

    db_conn = await asyncpg.connect(**ctx.conn_params)
    result = True
    tr = db_conn.transaction()
    await tr.start()
    try:
        await db_conn.execute("BEGIN ISOLATION LEVEL REPEATABLE READ;")
        sn_id = await db_conn.fetchval("select pg_export_snapshot()")
        await make_dump_impl(ctx, db_conn, sn_id)
    except:
        ctx.logger.error("<------------- make_dump failed\n" + exception_helper())
        result = False
    finally:
        await tr.rollback()
        await db_conn.close()

    if result:
        ctx.logger.info("<------------- Finished dump mode")
    return result

