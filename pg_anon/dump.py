import asyncio
import gzip
import hashlib
import json
import os
import re
import subprocess
from datetime import datetime
from hashlib import sha256
from typing import List, Tuple

import asyncpg
import nest_asyncio
from aioprocessing import AioQueue
from asyncpg import Connection, Pool

from pg_anon.common.db_utils import create_connection, create_pool
from pg_anon.common.dto import PgAnonResult
from pg_anon.common.enums import ResultCode, VerboseOptions, AnonMode
from pg_anon.common.multiprocessing_utils import init_process
from pg_anon.common.utils import (
    exception_helper, get_pg_util_version, get_dict_rule_for_table, get_dump_query, get_file_name_from_path, chunkify
)
from pg_anon.context import Context

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
    out, err = proc.communicate()

    if proc.returncode != 0:
        msg = "ERROR: database dump has failed! \n%s" % err.decode("utf-8")
        ctx.logger.error(msg)
        raise RuntimeError(msg)

    if out:
        for v in out.decode("utf-8").split("\n"):
            ctx.logger.info(v)


async def dump_into_file(ctx: Context, db_conn: Connection, query: str, file_name: str):
    try:
        if ctx.args.dbg_stage_1_validate_dict:
            return await db_conn.execute(query)

        return await db_conn.copy_from_query(
            query=query,
            output=file_name,
            format="binary",
        )
    except Exception as exc:
        ctx.logger.error(exc)
        raise exc


async def compress_file(ctx: Context, file_path: str, remove_origin_file_after_compress: bool = True):
    gzipped_file_path = f'{file_path}.gz'

    ctx.logger.debug(f"Start compressing file: {file_path}")
    with (open(file_path, "rb") as f_in,
          gzip.open(gzipped_file_path, "wb") as f_out):
        f_out.writelines(f_in)
    ctx.logger.debug(f"Compressing has done. Output file: {gzipped_file_path}")

    if remove_origin_file_after_compress:
        ctx.logger.debug(f"Removing origin file: {file_path}")
        os.remove(file_path)


async def dump_by_query(ctx: Context, pool: Pool, query: str, transaction_snapshot_id: str, file_name: str):
    file_path = str(os.path.join(ctx.args.output_dir, file_name.split(".")[0]))
    binary_output_file_path = f'{file_path}.bin'
    ctx.logger.info(f"================> Started task {query} to file {binary_output_file_path}")

    try:
        async with pool.acquire() as db_conn:
            async with db_conn.transaction(isolation='repeatable_read', readonly=True):
                await db_conn.execute(f"SET TRANSACTION SNAPSHOT '{transaction_snapshot_id}';")

                result = await dump_into_file(
                    ctx,
                    db_conn=db_conn,
                    query=query,
                    file_name=binary_output_file_path,
                )

        count_rows = re.findall(r"(\d+)", result)[0]
        ctx.logger.debug(f"COPY {count_rows} [rows] Task: {query}")

        if not ctx.args.dbg_stage_1_validate_dict:
            # Processing files no need to keep connection, after receiving data into binary file
            await compress_file(
                ctx=ctx,
                file_path=binary_output_file_path
            )

    except Exception as e:
        ctx.logger.error("Exception in dump_obj_func:\n" + exception_helper())
        raise Exception(f"Can't execute task: {query}")

    ctx.logger.info(f"<================ Finished task {query}")

    return {hashlib.sha256(query.encode()).hexdigest(): count_rows}


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


async def generate_dump_queries(ctx: Context, db_conn: Connection):
    tables = await get_tables_to_dump(
        excluded_schemas=ctx.exclude_schemas, db_conn=db_conn
    )
    queries = []
    files = {}

    included_objs = []  # for debug purposes
    excluded_objs = []  # for debug purposes

    for table_schema, table_name in tables:
        table_rule = get_dict_rule_for_table(
            dictionary_rules=ctx.prepared_dictionary_obj["dictionary"],
            schema=table_schema,
            table=table_name,
        )
        query = await get_dump_query(
            ctx=ctx,
            table_schema=table_schema,
            table_name=table_name,
            table_rule=table_rule,
            files=files,
            included_objs=included_objs,
            excluded_objs=excluded_objs
        )
        if query:
            ctx.logger.info(str(query))
            queries.append(query)

    if ctx.args.verbose == VerboseOptions.DEBUG:
        ctx.logger.debug("included_objs:\n" + json.dumps(included_objs, indent=4))
        ctx.logger.debug("excluded_objs:\n" + json.dumps(excluded_objs, indent=4))

    return queries, files


def process_dump_impl(name: str, ctx: Context, queue: AioQueue, query_tasks: List[Tuple[str, str]], transaction_snapshot_id: str):
    tasks_res = []

    status_ratio = 10
    if len(query_tasks) > 1000:
        status_ratio = 100
    if len(query_tasks) > 50000:
        status_ratio = 1000

    async def run():
        pool = await create_pool(
            connection_params=ctx.connection_params,
            server_settings=ctx.server_settings,
            min_size=ctx.args.db_connections_per_process,
            max_size=ctx.args.db_connections_per_process
        )
        tasks = set()

        for idx, (file_name, query) in enumerate(query_tasks):
            if len(tasks) >= ctx.args.db_connections_per_process:
                # Wait for some dump to finish before adding a new one
                done, tasks = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                exception = done.pop().exception()
                if exception is not None:
                    await pool.close()
                    raise exception

            task_res = loop.create_task(
                dump_by_query(ctx, pool, query, transaction_snapshot_id, file_name)
            )

            tasks.add(task_res)
            tasks_res.append(task_res)

            if idx % status_ratio:
                progress_percents = round(float(idx) * 100 / len(query_tasks), 2)
                ctx.logger.info(f"Process [{name}] Progress {progress_percents}%")

        if len(tasks) > 0:
            await asyncio.wait(tasks)

        await pool.close()

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        loop.run_until_complete(run())
    except asyncio.exceptions.TimeoutError:
        ctx.logger.error(f"================> Process [{name}]: asyncio.exceptions.TimeoutError")
    finally:
        loop.close()

    try:
        tasks_res_final = []
        for task in tasks_res:
            if task.result() is not None and len(task.result()) > 0:
                tasks_res_final.append(task.result())

        queue.put(tasks_res_final)
    except Exception as ex:
        ctx.logger.error(f"================> Process [{name}]: {ex}")
    finally:
        queue.put(None)  # Shut down the worker
        queue.close()


async def make_dump_impl(ctx: Context, db_conn: Connection, transaction_snapshot_id: str):
    queries, files = await generate_dump_queries(ctx, db_conn)
    if not queries:
        raise Exception("No objects for dump!")

    queries_chunks = chunkify(list(zip(files.keys(), queries)), ctx.args.processes)

    process_tasks = []
    for idx, queries_chunk in enumerate(queries_chunks):
        process_tasks.append(
            asyncio.ensure_future(
                init_process(
                    name=str(idx + 1),
                    ctx=ctx,
                    target_func=process_dump_impl,
                    tasks=queries_chunk,
                    transaction_snapshot_id=transaction_snapshot_id,
                )
            )
        )

    # Wait for the remaining dumps to finish
    task_group = asyncio.gather(*process_tasks)

    while not task_group.done():
        """
        Keeps main transaction in active by using simple query `SELECT 1`
        It's needs for large databases, when dump can making for very long time

        Avoids lots of queries by sleep
        Big value for sleeping isn't recommended, because it can freeze processing, when tasks will be done
        """
        await asyncio.sleep(5)
        await db_conn.execute('SELECT 1')

    await task_group

    task_results = {}
    for process_task in process_tasks:
        process_task_result = process_task.result()
        if not process_task_result:
            raise ValueError("One or more dump queries has been failed!")

        for res in process_task_result:
            task_results.update(res)

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

    metadata = {}
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

    for query, file in zip(queries, files):
        key = hashlib.sha256(query.encode()).hexdigest()
        files[file].update({"rows": task_results[key]})

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
    if ctx.args.dbg_stage_2_validate_data:
        metadata["dbg_stage_2_validate_data"] = True
    else:
        metadata["dbg_stage_2_validate_data"] = False
    if ctx.args.dbg_stage_3_validate_full:
        metadata["dbg_stage_3_validate_full"] = True
    else:
        metadata["dbg_stage_3_validate_full"] = False

    if not ctx.args.dbg_stage_1_validate_dict:
        with open(os.path.join(ctx.args.output_dir, "metadata.json"), "w", encoding='utf-8') as out_file:
            out_file.write(json.dumps(metadata, indent=4, ensure_ascii=False))


async def make_dump(ctx: Context):
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
            prepared_dict_name = get_file_name_from_path(ctx.args.prepared_sens_dict_files[0])
            output_dir = os.path.join(ctx.current_dir, "output", prepared_dict_name)
        elif ctx.args.output_dir.find("""/""") == -1 and ctx.args.output_dir.find("""\\""") == -1:
            output_dir = os.path.join(ctx.current_dir, "output", ctx.args.output_dir)
        else:
            output_dir = ctx.args.output_dir

        ctx.args.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)

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
        db_conn = await create_connection(ctx.connection_params, server_settings=ctx.server_settings)
        try:
            async with db_conn.transaction(isolation='repeatable_read', readonly=True):
                transaction_snapshot_id = await db_conn.fetchval("select pg_export_snapshot()")
                await make_dump_impl(ctx, db_conn, transaction_snapshot_id)
        except:
            ctx.logger.error("<------------- make_dump failed\n" + exception_helper())
            result.result_code = ResultCode.FAIL
        finally:
            await db_conn.close()

    if ctx.args.mode == AnonMode.SYNC_STRUCT_DUMP:
        metadata = {}
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

        with open(os.path.join(ctx.args.output_dir, "metadata.json"), "w", encoding='utf-8') as out_file:
            out_file.write(json.dumps(metadata, indent=4, ensure_ascii=False))

    if result.result_code == ResultCode.DONE:
        ctx.logger.info("<------------- Finished dump mode")
    return result
