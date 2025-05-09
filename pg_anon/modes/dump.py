import asyncio
import gzip
import hashlib
import json
import os
import re
import subprocess
import uuid
from datetime import datetime
from pathlib import Path
from typing import List, Tuple, Optional, Dict

from aioprocessing import AioQueue
from asyncpg import Connection, Pool

from pg_anon.common.db_queries import get_relation_size_query, get_sequences_query
from pg_anon.common.db_utils import create_connection, create_pool, get_tables_to_dump, get_db_size
from pg_anon.common.dto import Metadata
from pg_anon.common.enums import VerboseOptions, AnonMode
from pg_anon.common.multiprocessing_utils import init_process
from pg_anon.common.utils import (
    exception_helper, get_dict_rule_for_table, get_dump_query, get_file_name_from_path, chunkify, get_pg_util_version
)
from pg_anon.context import Context


class DumpMode:
    context: Context
    output_dir: str

    metadata: Metadata
    metadata_file_name: str = 'metadata.json'
    metadata_file_path: str

    _data_dump_queries: Optional[List[str]] = None
    _data_dump_files: Optional[Dict] = None
    _data_dump_tasks_results: Optional[Dict] = None

    _total_tables_size: int = 0
    _total_rows: int = 0
    _sequences_last_values: Dict = None

    _need_dump_pre_and_post_sections: bool = True
    _need_dump_data: bool = True
    _skip_pre_data_dump: bool = False
    _skip_post_data_dump: bool = False

    def __init__(self, context: Context):
        self.context = context
        self.metadata = Metadata()
        os.environ["PGPASSWORD"] = self.context.args.db_user_password

        if not self.context.args.output_dir:
            prepared_dict_name = get_file_name_from_path(self.context.args.prepared_sens_dict_files[0])
            self.output_dir = os.path.join(self.context.current_dir, "output", prepared_dict_name)
        elif self.context.args.output_dir.find("""/""") == -1 and self.context.args.output_dir.find("""\\""") == -1:
            self.output_dir = os.path.join(self.context.current_dir, "output", str(self.context.args.output_dir))
        else:
            self.output_dir = self.context.args.output_dir

        self.metadata_file_path = os.path.join(self.output_dir, self.metadata_file_name)

        self._need_dump_pre_and_post_sections = self.context.args.mode in (AnonMode.SYNC_STRUCT_DUMP, AnonMode.DUMP)
        self._need_dump_data = self.context.args.mode in (AnonMode.SYNC_DATA_DUMP, AnonMode.DUMP)
        self._skip_pre_data_dump = (
            not self._need_dump_pre_and_post_sections
            or self.context.args.dbg_stage_2_validate_data
        )
        self._skip_post_data_dump = (
            not self._need_dump_pre_and_post_sections
            or self.context.args.dbg_stage_2_validate_data
            or self.context.args.dbg_stage_3_validate_full
        )

    def _prepare_output_dir(self):
        os.makedirs(self.output_dir, exist_ok=True)

        if self.output_dir_is_empty:
            return
        elif not self.context.args.clear_output_dir:
            msg = f"Output directory {self.output_dir} is not empty!"
            self.context.logger.error(msg)
            raise Exception(msg)
        else:
            self._clear_output_dir()

    def _clear_output_dir(self):
        expected_file_extensions = [
            ".sql",
            ".gz",
            ".json",
            ".backup",
            ".bin",
        ]

        for root, dirs, files in os.walk(self.output_dir):
            for file in files:
                file_extension = Path(file).suffix.lower()
                if file_extension not in expected_file_extensions:
                    msg = f"Option --clear-output-dir enabled. Unexpected file extension: {os.path.join(root, file)}"
                    self.context.logger.error(msg)
                    raise Exception(msg)

                os.remove(os.path.join(root, file))

    @property
    def output_dir_is_empty(self) -> bool:
        return not bool(os.listdir(self.output_dir))

    async def _count_totals(self, connection: Connection):
        for query, file_key in zip(self._data_dump_queries, self._data_dump_files):
            file = self._data_dump_files[file_key]

            result_key = hashlib.sha256(query.encode()).hexdigest()
            file.update({"rows": self._data_dump_tasks_results[result_key]})

            schema = file["schema"].replace("'", "''")
            table = file["table"].replace("'", "''")

            self._total_tables_size += await connection.fetchval(
                get_relation_size_query(schema=schema, table=table)
            )
            self._total_rows += int(file["rows"])

    async def _prepare_sequences_last_values(self, connection: Connection):
        self._sequences_last_values = {}

        query = get_sequences_query()
        self.context.logger.debug(str(query))
        sequences_data = await connection.fetch(query)

        for sequence_data in sequences_data:
            seq_name = sequence_data[3] + "." + sequence_data[4]
            sequence_last_value = await connection.fetchval(
                f'select last_value from "{sequence_data[3]}"."{sequence_data[4]}"'
            )
            if ((self.context.args.dbg_stage_2_validate_data or self.context.args.dbg_stage_3_validate_full)
                    and sequence_last_value > int(self.context.validate_limit.split()[1])):
                sequence_last_value = 100

            for file in self._data_dump_files.values():
                if sequence_data[0] == file["schema"] and sequence_data[1] == file["table"]:
                    self._sequences_last_values[seq_name] = {
                        "schema": sequence_data[3],
                        "seq_name": sequence_data[4],
                        "value": sequence_last_value,
                    }

    async def _prepare_and_save_metadata(self):
        if self.context.args.dbg_stage_1_validate_dict:
            return

        self.metadata.created = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        self.metadata.pg_version = self.context.pg_version
        self.metadata.pg_dump_version = get_pg_util_version(self.context.pg_dump)

        self.metadata.dictionary_content_hash = {}
        for dictionary_file_name, dictionary_content in self.context.prepared_dictionary_contents.items():
            self.metadata.dictionary_content_hash[dictionary_file_name] = hashlib.sha256(
                dictionary_content.encode("utf-8")
            ).hexdigest()

        self.metadata.prepared_sens_dict_files = ','.join(self.context.args.prepared_sens_dict_files)

        if self.context.args.mode == AnonMode.SYNC_STRUCT_DUMP:
            self.metadata.schemas = list(
                dict_obj["schema"] for dict_obj in self.context.prepared_dictionary_obj["dictionary"]
            )
        else:
            self.metadata.files = self._data_dump_files
            self.metadata.sequences_last_values = self._sequences_last_values

            self.metadata.total_tables_size = self._total_tables_size
            self.metadata.total_rows = self._total_rows

            self.metadata.db_size = await get_db_size(
                connection_params=self.context.connection_params,
                server_settings=self.context.server_settings,
                db_name=self.context.args.db_name
            )

        self.metadata.dbg_stage_2_validate_data = self.context.args.dbg_stage_2_validate_data
        self.metadata.dbg_stage_3_validate_full = self.context.args.dbg_stage_3_validate_full

        self.metadata.save_into_file(file_name=self.metadata_file_path)

    async def _run_pg_dump(self, section):
        specific_tables = []
        if self.context.args.mode == AnonMode.SYNC_STRUCT_DUMP:
            tmp_list = []
            for v in self.context.prepared_dictionary_obj["dictionary"]:
                tmp_list.append(["-t", f'"{v["schema"]}"."{v["table"]}"'])
            specific_tables = [item for sublist in tmp_list for item in sublist]

        tmp_list = []
        for v in self.context.exclude_schemas:
            tmp_list.append(["--exclude-schema", v])

        exclude_schemas = [item for sublist in tmp_list for item in sublist]

        command = [
            self.context.pg_dump,
            "-h",
            self.context.args.db_host,
            "-p",
            str(self.context.args.db_port),
            "-v",
            "-w",
            "-U",
            self.context.args.db_user,
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
            os.path.join(self.output_dir, section.replace("-", "_") + ".backup"),
            self.context.args.db_name,
        ]
        if not self.context.args.db_host:
            del command[command.index("-h"): command.index("-h") + 2]

        self.context.logger.debug(str(command))
        proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        # pg_dump put command result into stdout if not using "-f" option, else stdout is empty
        # pg_dump put logs into stderr
        _, pg_dump_logs = proc.communicate()

        for log_line in pg_dump_logs.split("\n"):
            self.context.logger.info(log_line)

        if proc.returncode != 0:
            msg = "ERROR: database schema dump has failed!"
            self.context.logger.error(msg)
            raise RuntimeError(msg)

    async def _dump_data_into_file(self, db_conn: Connection, query: str, file_name: str):
        try:
            if self.context.args.dbg_stage_1_validate_dict:
                return await db_conn.execute(query)

            return await db_conn.copy_from_query(
                query=query,
                output=file_name,
                format="binary",
            )
        except Exception as exc:
            self.context.logger.error(exc)
            raise exc

    async def compress_file(self, file_path: str, remove_origin_file_after_compress: bool = True):
        gzipped_file_path = f'{file_path}.gz'

        self.context.logger.debug(f"Start compressing file: {file_path}")
        with (open(file_path, "rb") as f_in,
              gzip.open(gzipped_file_path, "wb") as f_out):
            f_out.writelines(f_in)
        self.context.logger.debug(f"Compressing has done. Output file: {gzipped_file_path}")

        if remove_origin_file_after_compress:
            self.context.logger.debug(f"Removing origin file: {file_path}")
            os.remove(file_path)

    async def _dump_data_by_query(self, pool: Pool, query: str, transaction_snapshot_id: str, file_name: str, process_name: Optional[str] = None):
        file_path = str(os.path.join(self.output_dir, file_name.split(".")[0]))
        binary_output_file_path = f'{file_path}.bin'

        task_id = uuid.uuid4()
        dump_is_complete = False
        compress_is_complete = False
        self.context.logger.info(f"================> Process [{process_name}] Task [{task_id}] Started task {query} to file {binary_output_file_path}")

        try:
            self.context.logger.debug(f"Process [{process_name}] Task [{task_id}] Connection acquiring")
            async with pool.acquire() as db_conn:
                self.context.logger.debug(f"Process [{process_name}] Task [{task_id}] Connection acquired")
                async with db_conn.transaction(isolation='repeatable_read', readonly=True):
                    await db_conn.execute(f"SET TRANSACTION SNAPSHOT '{transaction_snapshot_id}';")
                    self.context.logger.debug(f"Process [{process_name}] Task [{task_id}] Transaction opened. Starting dump query")
                    result = await self._dump_data_into_file(
                        db_conn=db_conn,
                        query=query,
                        file_name=binary_output_file_path,
                    )
                    self.context.logger.debug(f"Process [{process_name}] Task [{task_id}] Transaction setup to snapshot {transaction_snapshot_id}")

            dump_is_complete = True
            count_rows = re.findall(r"(\d+)", result)[0]
            self.context.logger.debug(f"Process [{process_name}] Task [{task_id}] COPY {count_rows} [rows] Task: {query}")

            if not self.context.args.dbg_stage_1_validate_dict:
                # Processing files no need to keep connection, after receiving data into binary file
                self.context.logger.debug(f"Process [{process_name}] Task [{task_id}] Compressing file start - {binary_output_file_path}")

                await self.compress_file(
                    file_path=binary_output_file_path
                )
                compress_is_complete = True
                self.context.logger.debug(f"Process [{process_name}] Task [{task_id}] Compressing file end - {binary_output_file_path}")

        except Exception as e:
            self.context.logger.error(
                f"Process [{process_name}] Task [{task_id}] Exception in DumpMode._dump_data_by_query:\n"
                + exception_helper()
            )
            if pool.is_closing():
                self.context.logger.debug(f"Process [{process_name}] Task [{task_id}] Pool closed!")

            error_message = "Something went wrong"
            if not dump_is_complete:
                error_message = f"Can't execute query: {query}"
            elif not compress_is_complete:
                error_message = f"Can't compress file: {binary_output_file_path}"

            self.context.logger.debug(f"Process [{process_name}] Task [{task_id}] Error: {error_message}")
            raise Exception(error_message)

        self.context.logger.info(f"<================ Process [{process_name}] Task [{task_id}] Finished task {query}")

        return {hashlib.sha256(query.encode()).hexdigest(): count_rows}

    async def _prepare_dump_queries(self, connection: Connection):
        tables = await get_tables_to_dump(
            connection=connection,
            excluded_schemas=self.context.exclude_schemas,
        )
        self._data_dump_queries = []
        self._data_dump_files = {}

        included_objs = []  # for debug purposes
        excluded_objs = []  # for debug purposes

        for table_schema, table_name in tables:
            table_rule = get_dict_rule_for_table(
                dictionary_rules=self.context.prepared_dictionary_obj["dictionary"],
                schema=table_schema,
                table=table_name,
            )

            query = await get_dump_query(
                ctx=self.context,
                table_schema=table_schema,
                table_name=table_name,
                table_rule=table_rule,
                files=self._data_dump_files,
                included_objs=included_objs,
                excluded_objs=excluded_objs
            )

            if query:
                self.context.logger.info(str(query))
                self._data_dump_queries.append(query)

        if self.context.args.verbose == VerboseOptions.DEBUG:
            self.context.logger.debug("included_objs:\n" + json.dumps(included_objs, indent=4))
            self.context.logger.debug("excluded_objs:\n" + json.dumps(excluded_objs, indent=4))

    def _process_dump_data(self, name: str, queue: AioQueue, query_tasks: List[Tuple[str, str]], transaction_snapshot_id: str):
        tasks_res = []

        status_ratio = 10
        if len(query_tasks) > 1000:
            status_ratio = 100
        if len(query_tasks) > 50000:
            status_ratio = 1000

        async def _process_run():
            self.context.logger.debug(f"================> Process [{name}] Connection pool opening")
            pool = await create_pool(
                connection_params=self.context.connection_params,
                server_settings=self.context.server_settings,
                min_size=self.context.args.db_connections_per_process,
                max_size=self.context.args.db_connections_per_process
            )
            tasks = set()

            try:
                query_tasks_count = len(query_tasks)
                for idx, (file_name, query) in enumerate(query_tasks):
                    if len(tasks) >= self.context.args.db_connections_per_process:
                        self.context.logger.debug(f"================> Process [{name}] Tasks pool is full. Waiting results by already started tasks")
                        # Wait for some dump to finish before adding a new one
                        done, tasks = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                        exception = done.pop().exception()
                        if exception is not None:
                            self.context.logger.error(f"<================ Process [{name}]: {exception}")
                            raise exception

                        self.context.logger.debug(f"Process [{name}] One of tasks has been completed")

                    self.context.logger.debug(f"Process [{name}] Adding new task into pool [{idx + 1}/{query_tasks_count}]")
                    task_res = loop.create_task(
                        self._dump_data_by_query(
                            pool=pool,
                            query=query,
                            transaction_snapshot_id=transaction_snapshot_id,
                            file_name=file_name,
                            process_name=name
                        )
                    )

                    tasks.add(task_res)
                    tasks_res.append(task_res)

                    self.context.logger.debug(f"Process [{name}] New task added. Current tasks in pool: {len(tasks)} / {self.context.args.db_connections_per_process}")

                    if idx % status_ratio:
                        progress_percents = round(float(idx) * 100 / len(query_tasks), 2)
                        self.context.logger.info(f"Process [{name}] Progress {progress_percents}%")

                self.context.logger.debug(f"Process [{name}] All tasks was started")

                if len(tasks) > 0:
                    self.context.logger.debug(f"Process [{name}] Waiting when all tasks will be ended. Current tasks in pool: {len(tasks)} / {self.context.args.db_connections_per_process}")
                    await asyncio.wait(tasks)
            finally:
                self.context.logger.debug(f"<================ Process [{name}] Connection pool closing")

                if pool.is_closing():
                    self.context.logger.debug(f"<================ Process [{name}] Connection pool already has been closed!")
                else:
                    await pool.close()
                    self.context.logger.debug(f"<================ Process [{name}] Connection pool closed")

        self.context.logger.info(f"================> Process [{name}] Started process_dump_impl")
        loop = asyncio.new_event_loop()

        try:
            self.context.logger.debug(f"Process [{name}] Setup event loop")
            asyncio.set_event_loop(loop)

            self.context.logger.debug(f"Process [{name}] Run dump tasks")
            loop.run_until_complete(_process_run())

            self.context.logger.debug(f"Process [{name}] Processing results start")
            tasks_res_final = []
            for task in tasks_res:
                if task.result() is not None and len(task.result()) > 0:
                    tasks_res_final.append(task.result())

            queue.put(tasks_res_final)
            self.context.logger.debug(f"Process [{name}] Processing results end")
        except Exception as ex:
            self.context.logger.error(f"<================ Process [{name}]: {exception_helper()}")
            raise ex
        finally:
            self.context.logger.debug(f"<================ Process [{name}] closing")
            loop.close()
            queue.put(None)  # Shut down the worker
            queue.close()
            self.context.logger.debug(f"<================ Process [{name}] closed")

    async def _dump_data(self):
        if not self._need_dump_data:
            self.context.logger.info("-------------> Skipped dump data")
            return

        self.context.logger.info("-------------> Started dump data")

        connection = await create_connection(
            self.context.connection_params,
            server_settings=self.context.server_settings
        )

        try:
            async with connection.transaction(isolation='repeatable_read', readonly=True):
                transaction_snapshot_id = await connection.fetchval("select pg_export_snapshot()")

                # Preparing dump queries
                await self._prepare_dump_queries(connection)
                if not self._data_dump_queries:
                    raise Exception("No objects for dump!")

                queries_chunks = chunkify(
                    list(zip(self._data_dump_files.keys(), self._data_dump_queries)),
                    self.context.args.processes
                )

                process_tasks = []
                for idx, queries_chunk in enumerate(queries_chunks):
                    process_tasks.append(
                        asyncio.ensure_future(
                            init_process(
                                name=str(idx + 1),
                                ctx=self.context,
                                target_func=self._process_dump_data,
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
                    await connection.execute('SELECT 1')

                await task_group

                self._data_dump_tasks_results = {}
                for process_task in process_tasks:
                    process_task_result = process_task.result()
                    if not process_task_result:
                        raise ValueError("One or more dump queries has been failed!")

                    for res in process_task_result:
                        self._data_dump_tasks_results.update(res)

                # Prepare data for metadata
                await self._count_totals(connection=connection)
                await self._prepare_sequences_last_values(connection=connection)
        finally:
            await connection.close()
            self.context.logger.info("<------------- Finished dump data")

    async def _dump_pre_data(self):
        if self._skip_pre_data_dump:
            self.context.logger.info("-------------> Skipped dump pre-data (pg_dump)")
            return

        self.context.logger.info("-------------> Started dump pre-data (pg_dump)")
        await self._run_pg_dump("pre-data")
        self.context.logger.info("<------------- Finished dump pre-data (pg_dump)")

    async def _dump_post_data(self):
        if self._skip_post_data_dump:
            self.context.logger.info("-------------> Skipped dump post-data (pg_dump)")
            return

        self.context.logger.info("-------------> Started dump post-data (pg_dump)")
        await self._run_pg_dump("post-data")
        self.context.logger.info("<------------- Finished dump post-data (pg_dump)")

    async def run(self) -> None:
        self.context.logger.info("-------------> Started dump")

        try:
            self.context.read_prepared_dict()

            if self.context.args.dbg_stage_1_validate_dict:
                return

            self._prepare_output_dir()

            await self._dump_pre_data()
            await self._dump_post_data()
            await self._dump_data()
            await self._prepare_and_save_metadata()

            self.context.logger.info("<------------- Finished dump")
        except Exception as ex:
            self.context.logger.error("<------------- Dump failed\n" + exception_helper())
            raise ex
