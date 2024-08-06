import asyncio
import gzip
import hashlib
import json
import os
import re
import subprocess
from datetime import datetime
from hashlib import sha256

import asyncpg

from pg_anon.common import (
    AnonMode,
    PgAnonResult,
    ResultCode,
    VerboseOptions,
    exception_helper,
    get_pg_util_version,
    get_dict_rule_for_table,
)

DEFAULT_EXCLUDED_SCHEMAS = ["pg_catalog", "information_schema"]


async def run_pg_dump(ctx, section):
    os.environ["PGPASSWORD"] = ctx.args.db_user_password

    specific_tables = []
    if ctx.args.mode == AnonMode.SYNC_STRUCT_DUMP:
        tmp_list = []
        for v in ctx.prepared_dictionary_obj["dictionary"]:
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
        del command[command.index("-h"): command.index("-h") + 2]

    ctx.logger.debug(str(command))
    proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    err, out = proc.communicate()
    if err.decode("utf-8") != "":
        msg = "ERROR: database schema dump has failed! \n%s" % err.decode("utf-8")
        ctx.logger.error(msg)
        raise RuntimeError(msg)

    for v in out.decode("utf-8").split("\n"):
        ctx.logger.info(v)


async def get_dump_table(ctx, query: str, file_name: str, db_conn, output_dir: str):
    full_file_name = os.path.join(output_dir, file_name.split(".")[0])
    try:
        if ctx.args.dbg_stage_1_validate_dict:
            result = await db_conn.execute(query)
            return result
        result = await db_conn.copy_from_query(
            query, output=f"{full_file_name}.bin", format="binary"
        )
        with open(f"{full_file_name}.bin", "rb") as f_in, gzip.open(
                f"{full_file_name}.bin.gz", "wb"
        ) as f_out:
            f_out.writelines(f_in)
        os.remove(f"{full_file_name}.bin")
        return result
    except Exception as exc:
        ctx.logger.error(exc)
        raise exc


async def dump_obj_func(ctx, pool, task, sn_id, file_name):
    ctx.logger.info("================> Started task %s" % str(task))

    try:
        async with pool.acquire() as db_conn:
            async with db_conn.transaction():
                await db_conn.execute("BEGIN ISOLATION LEVEL REPEATABLE READ;")
                await db_conn.execute("SET TRANSACTION SNAPSHOT '%s';" % sn_id)
                res = await get_dump_table(
                    ctx,
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

    ctx.logger.info("<================ Finished task %s" % str(task))


async def get_tables_to_dump(excluded_schemas: list, db_conn: asyncpg.Connection):
    excluded_schemas_str = ", ".join(
        [f"'{v}'" for v in [*excluded_schemas, *DEFAULT_EXCLUDED_SCHEMAS]]
    )
    query_db_obj = f"""
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE
                table_schema not in ({excluded_schemas_str}) and
                table_type = 'BASE TABLE'
        """
    db_objs = await db_conn.fetch(query_db_obj)
    return db_objs


async def generate_dump_queries(ctx, db_conn):
    tables = await get_tables_to_dump(
        excluded_schemas=ctx.exclude_schemas, db_conn=db_conn
    )
    queries = []
    files = {}

    included_objs = []  # for debug purposes
    excluded_objs = []  # for debug purposes

    for table_schema, table_name in tables:
        table_name_full = f'"{table_schema}"."{table_name}"'

        table_rule = get_dict_rule_for_table(
            ctx.prepared_dictionary_obj["dictionary"], table_schema, table_name
        )
        found_white_list = table_rule is not None

        # dictionary_exclude has the highest priority
        if "dictionary_exclude" in ctx.prepared_dictionary_obj:
            exclude_rule = get_dict_rule_for_table(
                ctx.prepared_dictionary_obj["dictionary_exclude"], table_schema, table_name
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
                continue

        hashed_name = hashlib.md5(
            (table_schema + "_" + table_name).encode()
        ).hexdigest()

        files["%s.bin.gz" % hashed_name] = {"schema": table_schema, "table": table_name}

        if not found_white_list:
            included_objs.append(
                [table_rule, table_schema, table_name, "if not found_white_list"]
            )
            # there is no table in the dictionary, so it will be transferred "as is"
            if (ctx.args.dbg_stage_1_validate_dict
                    or ctx.args.dbg_stage_2_validate_data
                    or ctx.args.dbg_stage_3_validate_full):
                query = "SELECT * FROM %s %s" % (table_name_full, ctx.validate_limit)
                ctx.logger.info(str(query))
                queries.append(query)
            else:
                query = f"SELECT * FROM {table_name_full}"
                ctx.logger.info(str(query))
                queries.append(query)
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
                    queries.append(query)
                else:
                    query = table_rule["raw_sql"]
                    ctx.logger.info(str(query))
                    queries.append(query)
            else:
                # the table is transferred with the specific fields for anonymization
                fields_list = await db_conn.fetch(
                    """
                        SELECT column_name, udt_name FROM information_schema.columns
                        WHERE table_schema = '%s' AND table_name='%s'
                        ORDER BY ordinal_position ASC
                    """
                    % (table_schema.replace("'", "''"), table_name.replace("'", "''"))
                )

                sql_expr = ""

                def check_fld(fld_name):
                    if fld_name in table_rule["fields"]:
                        return fld_name, table_rule["fields"][fld_name]
                    return None, None

                for cnt, column_info in enumerate(fields_list):
                    column_name = column_info["column_name"]
                    udt_name = column_info["udt_name"]
                    fld_name, fld_val = check_fld(column_name)
                    if fld_name:
                        if fld_val.find("SQL:") == 0:
                            sql_expr += f'({fld_val[4:]}) as "{fld_name}"'
                        else:
                            sql_expr += f'{fld_val}::{udt_name} as "{fld_name}"'
                    else:
                        # field "as is"
                        if (
                                not column_name.islower() and not column_name.isupper()
                        ) or column_name.isupper():
                            sql_expr += f'"{column_name}" as "{column_name}"'
                        else:
                            sql_expr += f'"{column_name}" as "{column_name}"'
                    if cnt != len(fields_list) - 1:
                        sql_expr += ",\n"

                if (ctx.args.dbg_stage_1_validate_dict
                        or ctx.args.dbg_stage_2_validate_data
                        or ctx.args.dbg_stage_3_validate_full):
                    query = "SELECT %s FROM %s %s" % (
                        sql_expr,
                        table_name_full,
                        ctx.validate_limit,
                    )
                    ctx.logger.info(str(query))
                    queries.append(query)
                else:
                    query = "SELECT %s FROM %s" % (
                        sql_expr,
                        table_name_full,
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
            f'select last_value from "{v[3]}"."{v[4]}"'
        )
        if ((ctx.args.dbg_stage_2_validate_data or ctx.args.dbg_stage_3_validate_full)
                and seq_val > int(ctx.validate_limit.split()[1])):
            seq_val = 100

        for _, f in files.items():
            if v[0] == f["schema"] and v[1] == f["table"]:
                seq_res_dict[seq_name] = {
                    "schema": v[3],
                    "seq_name": v[4],
                    "value": seq_val,
                }

    metadata = dict()
    metadata["db_size"] = await db_conn.fetchval(
        """SELECT pg_database_size('""" + ctx.args.db_name + """')"""
    )
    metadata["created"] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    metadata["seq_lastvals"] = seq_res_dict
    metadata["pg_version"] = ctx.pg_version
    metadata["pg_dump_version"] = get_pg_util_version(ctx.args.pg_dump)

    metadata["dictionary_content_hash"] = {}
    for dictionary_file_name, dictionary_content in ctx.prepared_dictionary_contents.items():
        metadata["dictionary_content_hash"][dictionary_file_name] = sha256(
            dictionary_content.encode("utf-8")
        ).hexdigest()
    metadata["prepared_sens_dict_files"] = ','.join(ctx.args.prepared_sens_dict_files)

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
        # print('<---------------------------------', int(v["rows"]))
        total_rows += int(v["rows"])
    metadata["total_tables_size"] = total_tables_size
    metadata["total_rows"] = total_rows
    if ctx.args.dbg_stage_2_validate_data:
        metadata["dbg_stage_2_validate_data"] = True
    else:
        metadata["dbg_stage_2_validate_data"] = False
    if ctx.args.dbg_stage_3_validate_full:
        metadata["dbg_stage_3_validate_full"] = True
    else:
        metadata["dbg_stage_3_validate_full"] = False

    if not ctx.args.dbg_stage_1_validate_dict:
        with open(os.path.join(ctx.args.output_dir, "metadata.json"), "w") as out_file:
            out_file.write(json.dumps(metadata, indent=4))


async def make_dump(ctx):
    result = PgAnonResult()
    ctx.logger.info("-------------> Started dump mode")

    try:
        ctx.read_prepared_dict()
    except:
        ctx.logger.error("<------------- make_dump failed\n" + exception_helper())
        result.result_code = ResultCode.FAIL
        return result

    try:
        if not ctx.args.output_dir:
            output_dir = os.path.join(
                ctx.current_dir, "output", os.path.splitext(ctx.args.prepared_sens_dict_files[0])[0]
            )
        elif ctx.args.output_dir.find("""/""") == -1 and ctx.args.output_dir.find("""\\""") == -1:
            output_dir = os.path.join(ctx.current_dir, "output", ctx.args.output_dir)
        else:
            output_dir = ctx.args.output_dir

        ctx.args.output_dir = output_dir
        dir_exists = os.path.exists(output_dir)
        if not dir_exists:
            os.makedirs(output_dir)

        if not ctx.args.dbg_stage_1_validate_dict:
            dir_empty = True
            for root, dirs, files in os.walk(output_dir):
                for _ in files:
                    dir_empty = False
                    break

            if not dir_empty:
                if not ctx.args.clear_output_dir:
                    msg = "Output directory " + output_dir + " is not empty!"
                    ctx.logger.error(msg)
                    raise Exception(msg)

                else:
                    for root, dirs, files in os.walk(output_dir):
                        for file in files:
                            if (
                                    file.endswith(".sql")
                                    or file.endswith(".gz")
                                    or file.endswith(".json")
                                    or file.endswith(".backup")
                                    or file.endswith(".bin")
                            ):
                                os.remove(os.path.join(root, file))
                            else:
                                msg = (
                                        "Option --clear-output-dir enabled. Unexpected file extension: %s"
                                        % os.path.join(root, file)
                                )
                                ctx.logger.error(msg)
                                raise Exception(msg)

            if ctx.args.mode in (AnonMode.SYNC_STRUCT_DUMP, AnonMode.DUMP) and not ctx.args.dbg_stage_2_validate_data:
                ctx.logger.info("-------------> Started pg_dump")
                await run_pg_dump(ctx, "pre-data")
                if not ctx.args.dbg_stage_3_validate_full:
                    await run_pg_dump(ctx, "post-data")
                ctx.logger.info("<------------- Finished pg_dump")
    except:
        ctx.logger.error("<------------- make_dump failed\n" + exception_helper())
        result.result_code = ResultCode.FAIL
        return result

    result.result_code = ResultCode.DONE

    if ctx.args.mode in (AnonMode.SYNC_DATA_DUMP, AnonMode.DUMP):
        db_conn = await asyncpg.connect(**ctx.conn_params)
        try:
            async with db_conn.transaction():
                await db_conn.execute("BEGIN ISOLATION LEVEL REPEATABLE READ;")
                sn_id = await db_conn.fetchval("select pg_export_snapshot()")
                await make_dump_impl(ctx, db_conn, sn_id)
        except:
            ctx.logger.error("<------------- make_dump failed\n" + exception_helper())
            result.result_code = ResultCode.FAIL
        finally:
            await db_conn.close()

    if ctx.args.mode == AnonMode.SYNC_STRUCT_DUMP:
        metadata = dict()
        metadata["created"] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        metadata["pg_version"] = ctx.pg_version
        metadata["pg_dump_version"] = get_pg_util_version(ctx.args.pg_dump)

        metadata["dictionary_content_hash"] = {}
        for dictionary_file_name, dictionary_content in ctx.prepared_dictionary_contents.items():
            metadata["dictionary_content_hash"][dictionary_file_name] = sha256(
                dictionary_content.encode("utf-8")
            ).hexdigest()

        metadata["prepared_sens_dict_files"] = ','.join(ctx.args.prepared_sens_dict_files)

        metadata["total_tables_size"] = 0
        metadata["total_rows"] = 0
        metadata["dbg_stage_2_validate_data"] = False
        metadata["dbg_stage_3_validate_full"] = False

        tmp_list = []
        for v in ctx.prepared_dictionary_obj["dictionary"]:
            tmp_list.append(v["schema"])
        metadata["schemas"] = list(set(tmp_list))

        with open(os.path.join(ctx.args.output_dir, "metadata.json"), "w") as out_file:
            out_file.write(json.dumps(metadata, indent=4))

    if result.result_code == ResultCode.DONE:
        ctx.logger.info("<------------- Finished dump mode")
    return result
