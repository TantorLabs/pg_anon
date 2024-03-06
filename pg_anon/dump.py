import asyncio
import gzip
import hashlib
import json
import os
import re
import subprocess
from datetime import datetime
from hashlib import sha256
from logging import getLogger

import asyncpg

from pg_anon.common import (
    AnonMode,
    PgAnonResult,
    ResultCode,
    VerboseOptions,
    exception_helper,
    get_pg_util_version,
)

logger = getLogger(__name__)


async def run_pg_dump(ctx, section):
    os.environ["PGPASSWORD"] = ctx.args.db_user_password

    specific_tables = []
    if ctx.args.mode == AnonMode.SYNC_STRUCT_DUMP:
        tmp_list = []
        for v in ctx.dictionary_obj["dictionary"]:
            tmp_list.append(["-t", '"%s"."%s"' % (v["schema"], v["table"])])
        specific_tables = [item for sublist in tmp_list for item in sublist]

    exclude_schemas = []
    tmp_list = []
    for v in ctx.exclude_schemas:
        tmp_list.append(["--exclude-schema", v])
    exclude_schemas = [item for sublist in tmp_list for item in sublist]

    command = [
        ctx.args.pg_dump,
        "-h",
        ctx.args.db_host,
        "-p",
        str(ctx.args.db_port),
        "-v",
        "-w",
        "-U",
        ctx.args.db_user,
        *exclude_schemas,
        *specific_tables,
        "--section",
        section,
        "-E",
        "UTF8",
        "-F",
        "c",
        "-s",
        "-f",
        os.path.join(ctx.args.output_dir, section.replace("-", "_") + ".backup"),
        ctx.args.db_name,
    ]
    if not ctx.args.db_host:
        del command[command.index("-h") : command.index("-h") + 2]

    ctx.logger.debug(str(command))
    proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    err, out = proc.communicate()
    if err.decode("utf-8") != "":
        msg = "ERROR: database schema dump has failed! \n%s" % err.decode("utf-8")
        ctx.logger.error(msg)
        raise RuntimeError(msg)

    for v in out.decode("utf-8").split("\n"):
        ctx.logger.info(v)


async def get_dump_table(query: str, file_name: str, db_conn, output_dir: str):
    full_file_name = os.path.join(output_dir, file_name.split(".")[0])
    try:
        result = await db_conn.copy_from_query(
            query, output=f"{full_file_name}.bin", format="binary"
        )
        with open(f"{full_file_name}.bin", "rb") as f_in, gzip.open(
            f"{full_file_name}.dat.gz", "wb"
        ) as f_out:
            f_out.writelines(f_in)
        os.remove(f"{full_file_name}.bin")
        return result
    except Exception as exc:
        logger.error(exc)
        raise exc


async def dump_obj_func(ctx, pool, task, sn_id, file_name):
    ctx.logger.info("================> Started task %s" % str(task))

    db_conn = await pool.acquire()
    try:
        await db_conn.execute("BEGIN ISOLATION LEVEL REPEATABLE READ;")
        await db_conn.execute("SET TRANSACTION SNAPSHOT '%s';" % sn_id)
        res = await get_dump_table(
            query=task,
            file_name=file_name,
            db_conn=db_conn,
            output_dir=ctx.args.output_dir,
        )
        count_rows = re.findall(r"(\d+)", res)[0]
        ctx.task_results[hash(task)] = count_rows
        ctx.logger.debug("COPY %s [rows] Task: %s " % (count_rows, str(task)))
    except Exception as e:
        ctx.logger.error("Exception in dump_obj_func:\n" + exception_helper())
        raise Exception("Can't execute task: %s" % task)
    finally:
        await db_conn.close()
        await pool.release(db_conn)

    ctx.logger.info("<================ Finished task %s" % str(task))


def find_obj_in_dict(dictionary_obj, schema, table):
    result = None
    for v in dictionary_obj:
        schema_matched = False
        table_matched = False
        schema_mask_matched = False
        table_mask_matched = False

        if "schema" in v and schema == v["schema"]:
            schema_matched = True

        if "table" in v and table == v["table"]:
            table_matched = True

        if schema_matched and table_matched:
            return v

        if (
            "schema_mask" in v
            and v["schema_mask"] != "*"
            and re.search(v["schema_mask"], schema) is not None
        ):
            schema_mask_matched = True

        if (
            "table_mask" in v
            and v["table_mask"] != "*"
            and re.search(v["table_mask"], table) is not None
        ):
            table_mask_matched = True

        if "schema_mask" in v and v["schema_mask"] == "*":
            schema_mask_matched = True

        if "table_mask" in v and v["table_mask"] == "*":
            table_mask_matched = True

        if schema_mask_matched and table_matched:
            result = v

        if schema_matched and table_mask_matched:
            result = v

        if schema_mask_matched and table_mask_matched:
            result = v

    return result


async def generate_dump_queries(ctx, db_conn):
    db_objs = await db_conn.fetch(
        """
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE
                table_schema not in (%s) and
                table_type = 'BASE TABLE'
        """
        % ", ".join(
            [
                "'" + v + "'"
                for v in ctx.exclude_schemas + ["pg_catalog", "information_schema"]
            ]
        )
    )
    queries = []
    files = {}

    included_objs = []  # for debug purposes
    excluded_objs = []  # for debug purposes

    for item in db_objs:
        table_name = '"' + item[0] + '"."' + item[1] + '"'

        if item[0] == "public" and item[1] == "tbl_100":
            x = 1

        a_obj = find_obj_in_dict(ctx.dictionary_obj["dictionary"], item[0], item[1])
        found_white_list = not (a_obj is None)

        # dictionary_exclude has the highest priority
        if "dictionary_exclude" in ctx.dictionary_obj:
            exclude_obj = find_obj_in_dict(
                ctx.dictionary_obj["dictionary_exclude"], item[0], item[1]
            )
            found = not (exclude_obj is None)
            if found and not found_white_list:
                excluded_objs.append(
                    [exclude_obj, item[0], item[1], "if found and not found_white_list"]
                )
                ctx.logger.info("Skipping: " + str(table_name))
                continue

        hashed_name = hashlib.md5((item[0] + "_" + item[1]).encode()).hexdigest()

        files["%s.dat.gz" % hashed_name] = {"schema": item[0], "table": item[1]}

        if not found_white_list:
            included_objs.append([a_obj, item[0], item[1], "if not found_white_list"])
            # there is no table in the dictionary, so it will be transferred "as is"
            if not ctx.args.validate_dict:
                query = f"SELECT * FROM {table_name} {(ctx.validate_limit if ctx.args.validate_full else '')}"
                ctx.logger.info(str(query))
                queries.append(query)
            else:
                query = "SELECT * FROM %s %s" % (table_name, ctx.validate_limit)
                ctx.logger.info(str(query))
                queries.append(query)
        else:
            included_objs.append([a_obj, item[0], item[1], "if found_white_list"])
            # table found in dictionary
            if "raw_sql" in a_obj:
                # the table is transferred using "raw_sql"
                if not ctx.args.validate_dict:
                    query = "%s %s" % (
                        a_obj["raw_sql"],
                        (ctx.validate_limit if ctx.args.validate_full else ""),
                    )
                    ctx.logger.info(str(query))
                    queries.append(query)
                else:
                    query = a_obj["raw_sql"] + " " + ctx.validate_limit
                    ctx.logger.info(str(query))
                    queries.append(query)
            else:
                # the table is transferred with the specific fields for anonymization
                fields_list = await db_conn.fetch(
                    """
                        SELECT column_name FROM information_schema.columns
                        WHERE table_schema = '%s' AND table_name='%s'
                        ORDER BY ordinal_position ASC
                    """
                    % (item[0].replace("'", "''"), item[1].replace("'", "''"))
                )

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
                            sql_expr += "(" + fld_val[4:] + ') as "' + fld_name + '"'
                        else:
                            sql_expr += fld_val + ' as "' + fld_name + '"'
                    else:
                        # field "as is"
                        if (not v[0].islower() and not v[0].isupper()) or v[
                            0
                        ].isupper():
                            sql_expr += '"' + v[0] + '" as "' + v[0] + '"'
                        else:
                            sql_expr += ' "' + v[0] + '" as "' + v[0] + '"'
                    if cnt != len(fields_list) - 1:
                        sql_expr += ",\n"

                if not ctx.args.validate_dict:
                    query = "SELECT %s FROM %s %s" % (
                        sql_expr,
                        table_name,
                        (ctx.validate_limit if ctx.args.validate_full else ""),
                    )
                    ctx.logger.info(str(query))
                    queries.append(query)
                else:
                    query = "SELECT %s FROM %s %s" % (
                        sql_expr,
                        table_name,
                        ctx.validate_limit,
                    )
                    ctx.logger.info(str(query))
                    queries.append(query)

    if ctx.args.verbose == VerboseOptions.DEBUG:
        ctx.logger.debug("included_objs:\n" + json.dumps(included_objs, indent=4))
        ctx.logger.debug("excluded_objs:\n" + json.dumps(excluded_objs, indent=4))

    return queries, files


async def make_dump_impl(ctx, db_conn, sn_id):
    loop = asyncio.get_event_loop()
    tasks = set()
    pool = await asyncpg.create_pool(
        **ctx.conn_params, min_size=ctx.args.threads, max_size=ctx.args.threads
    )

    queries, files = await generate_dump_queries(ctx, db_conn)
    if not queries:
        await pool.close()
        raise Exception("No objects for dump!")

    zipped_list = list(zip([hash(v) for v in queries], files))

    for file_name, query in zip(files.keys(), queries):
        if len(tasks) >= ctx.args.threads:
            # Wait for some dump to finish before adding a new one
            done, tasks = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            exception = done.pop().exception()
            if exception is not None:
                await pool.close()
                raise exception
        tasks.add(loop.create_task(dump_obj_func(ctx, pool, query, sn_id, file_name)))

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
            t.relkind IN ('r', 'p')
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
        seq_val = await db_conn.fetchval(
            """select last_value from \"""" + v[3] + """\".\"""" + v[4] + '"'
        )

        for _, f in files.items():
            if v[0] == f["schema"] and v[1] == f["table"]:
                seq_res_dict[seq_name] = {
                    "schema": v[3],
                    "seq_name": v[4],
                    "value": seq_val,
                }

    metadata = {}
    metadata["db_size"] = await db_conn.fetchval(
        """SELECT pg_database_size('""" + ctx.args.db_name + """')"""
    )
    metadata["created"] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    metadata["seq_lastvals"] = seq_res_dict
    metadata["pg_version"] = ctx.pg_version
    metadata["pg_dump_version"] = get_pg_util_version(ctx.args.pg_dump)
    metadata["dictionary_content_hash"] = sha256(
        ctx.dictionary_content.encode("utf-8")
    ).hexdigest()
    metadata["dict_file"] = ctx.args.dict_file

    for v in zipped_list:
        files[v[1]].update({"rows": ctx.task_results[v[0]]})

    metadata["files"] = files

    total_tables_size = 0
    total_rows = 0
    for k, v in files.items():
        # print("""select pg_total_relation_size('"%s"."%s"')""" % (v['schema'], v['table']))
        schema = v["schema"].replace("'", "''")
        table = v["table"].replace("'", "''")
        total_tables_size += await db_conn.fetchval(
            """select pg_total_relation_size('"%s"."%s"')""" % (schema, table)
        )
        total_rows += int(v["rows"])
    metadata["total_tables_size"] = total_tables_size
    metadata["total_rows"] = total_rows

    with open(os.path.join(ctx.args.output_dir, "metadata.json"), "w") as out_file:
        out_file.write(json.dumps(metadata, indent=4))


async def make_dump(ctx):
    result = PgAnonResult()
    ctx.logger.info("-------------> Started dump mode")

    try:
        dictionary_file = open(
            os.path.join(ctx.current_dir, "dict", ctx.args.dict_file), "r"
        )
        ctx.dictionary_content = dictionary_file.read()
        dictionary_file.close()
        ctx.dictionary_obj = eval(ctx.dictionary_content)
    except:
        ctx.logger.error("<------------- make_dump failed\n" + exception_helper())
        result.result_code = ResultCode.FAIL
        return result

    try:
        if (
            ctx.args.output_dir.find("""/""") != -1
            or ctx.args.output_dir.find("""\\""") != -1
        ):
            output_dir = ctx.args.output_dir
        if len(ctx.args.output_dir) > 1:
            output_dir = os.path.join(ctx.current_dir, "output", ctx.args.output_dir)
        else:
            output_dir = os.path.join(
                ctx.current_dir, "output", os.path.splitext(ctx.args.dict_file)[0]
            )

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
                        if (
                            file.endswith(".sql")
                            or file.endswith(".gz")
                            or file.endswith(".json")
                            or file.endswith(".backup")
                        ):
                            os.remove(os.path.join(root, file))
                        else:
                            msg = (
                                "Option --clear-output-dir enabled. Unexpected file extension: %s"
                                % os.path.join(root, file)
                            )
                            ctx.logger.error(msg)
                            raise Exception(msg)

        if not ctx.args.validate_dict and ctx.args.mode != AnonMode.SYNC_DATA_DUMP:
            ctx.logger.info("-------------> Started pg_dump")
            await run_pg_dump(ctx, "pre-data")
            await run_pg_dump(ctx, "post-data")
            ctx.logger.info("<------------- Finished pg_dump")
    except:
        ctx.logger.error("<------------- make_dump failed\n" + exception_helper())
        result.result_code = ResultCode.FAIL
        return result

    result.result_code = ResultCode.DONE

    if ctx.args.mode != AnonMode.SYNC_STRUCT_DUMP:
        db_conn = await asyncpg.connect(**ctx.conn_params)
        tr = db_conn.transaction()
        await tr.start()
        try:
            await db_conn.execute("BEGIN ISOLATION LEVEL REPEATABLE READ;")
            sn_id = await db_conn.fetchval("select pg_export_snapshot()")
            await make_dump_impl(ctx, db_conn, sn_id)
        except:
            ctx.logger.error("<------------- make_dump failed\n" + exception_helper())
            result.result_code = ResultCode.FAIL
        finally:
            await tr.rollback()
            await db_conn.close()

    if ctx.args.mode == AnonMode.SYNC_STRUCT_DUMP:
        metadata = {}
        metadata["created"] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        metadata["pg_version"] = ctx.pg_version
        metadata["pg_dump_version"] = get_pg_util_version(ctx.args.pg_dump)
        metadata["dictionary_content_hash"] = sha256(
            ctx.dictionary_content.encode("utf-8")
        ).hexdigest()
        metadata["dict_file"] = ctx.args.dict_file
        metadata["total_tables_size"] = 0
        metadata["total_rows"] = 0

        tmp_list = []
        for v in ctx.dictionary_obj["dictionary"]:
            tmp_list.append(v["schema"])
        metadata["schemas"] = list(set(tmp_list))

        with open(os.path.join(ctx.args.output_dir, "metadata.json"), "w") as out_file:
            out_file.write(json.dumps(metadata, indent=4))

    if result.result_code == ResultCode.DONE:
        ctx.logger.info("<------------- Finished dump mode")
    return result
