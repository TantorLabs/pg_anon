import asyncio
import gzip
import hashlib
import os
import re
import shutil
import subprocess
import uuid
from datetime import datetime
from pathlib import Path
from typing import List, Tuple, Optional, Dict, Union

from aioprocessing import AioQueue
from asyncpg import Connection, Pool

from pg_anon.common.db_queries import get_relation_size_query, get_sequences_query
from pg_anon.common.db_utils import create_connection, create_pool, get_db_tables, get_db_size, get_dump_query, \
    get_custom_functions_ddl, get_custom_domains_ddl, get_indexes_data, get_views_related_to_tables, get_schemas, \
    get_constraints_to_excluded_tables, get_custom_types_ddl, get_custom_casts_ddl, get_custom_operators_ddl, \
    get_custom_aggregates_ddl, get_extensions
from pg_anon.common.dto import Metadata
from pg_anon.common.enums import AnonMode
from pg_anon.common.multiprocessing_utils import init_process
from pg_anon.common.utils import (
    exception_helper, get_dict_rule_for_table, chunkify, get_pg_util_version, save_dicts_info_file, safe_compile
)
from pg_anon.context import Context


class DumpMode:
    context: Context
    output_dir: Path

    metadata: Metadata
    metadata_file_name: str = 'metadata.json'
    dumped_tables_file_name: str = 'dumped_tables.py'

    _data_dump_queries: Optional[List[str]] = None
    _data_dump_files: Optional[Dict] = None
    _data_dump_tasks_results: Optional[Dict] = None

    _total_tables_size: int = 0
    _total_rows: int = 0
    
    _schemas: List[str] = None
    _sequences_last_values: Dict = None
    _indexes: Dict = None
    _views: Dict = None
    _constraints: Dict = None
    _extensions: Dict = None

    _views_for_including: List[str] = None
    _views_for_excluding: List[str] = None

    _need_dump_pre_and_post_sections: bool = True
    _need_dump_data: bool = True
    _skip_pre_data_dump: bool = False
    _skip_post_data_dump: bool = False

    def __init__(self, context: Context):
        self.context = context
        self.metadata = Metadata()
        os.environ["PGPASSWORD"] = self.context.options.db_user_password

        if not self.context.options.output_dir:
            prepared_dict_name = Path(self.context.options.prepared_sens_dict_files[0]).stem
            self.output_dir = Path.cwd() / prepared_dict_name
        else:
            self.output_dir = Path.cwd() / str(self.context.options.output_dir)

        self.metadata_file_path = self.output_dir / self.metadata_file_name
        self.dumped_tables_file_path = self.output_dir / self.dumped_tables_file_name

        self._need_dump_pre_and_post_sections = self.context.options.mode in (AnonMode.SYNC_STRUCT_DUMP, AnonMode.DUMP)
        self._need_dump_data = self.context.options.mode in (AnonMode.SYNC_DATA_DUMP, AnonMode.DUMP)
        self._skip_pre_data_dump = (
            not self._need_dump_pre_and_post_sections
            or self.context.options.dbg_stage_1_validate_dict
            or self.context.options.dbg_stage_2_validate_data
        )
        self._skip_post_data_dump = (
            not self._need_dump_pre_and_post_sections
            or self.context.options.dbg_stage_1_validate_dict
            or self.context.options.dbg_stage_2_validate_data
            or self.context.options.dbg_stage_3_validate_full
        )

    def _prepare_output_dir(self):
        if self.context.options.dbg_stage_1_validate_dict:
            return

        self.output_dir.mkdir(parents=True, exist_ok=True)

        if self.output_dir_is_empty:
            return
        elif not self.context.options.clear_output_dir:
            msg = f"Output directory {self.output_dir} is not empty!"
            self.context.logger.error(msg)
            raise Exception(msg)
        else:
            self._clear_output_dir()

    def _clear_output_dir(self):
        expected_file_extensions = {
            ".sql",
            ".gz",
            ".json",
            ".backup",
            ".bin",
            ".py",
            ".list",
        }

        for file_path in Path(self.output_dir).rglob("*"):
            if file_path.is_file():
                if file_path.suffix.lower() not in expected_file_extensions:
                    msg = f"Option --clear-output-dir enabled. Unexpected file extension: {file_path}"
                    self.context.logger.error(msg)
                    raise Exception(msg)

                file_path.unlink()

    @property
    def output_dir_is_empty(self) -> bool:
        return not any(self.output_dir.iterdir())

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

        query = get_sequences_query(self.context.exclude_schemas)
        self.context.logger.debug(str(query))
        sequences_data = await connection.fetch(query)

        for table_schema, table_name, _, sequence_schema, sequence_name in sequences_data:
            full_sequence_name = sequence_schema + "." + sequence_name
            sequence_last_value = await connection.fetchval(
                f'select last_value from "{sequence_schema}"."{sequence_name}"'
            )
            if ((self.context.options.dbg_stage_2_validate_data or self.context.options.dbg_stage_3_validate_full)
                    and sequence_last_value > int(self.context.validate_limit.split()[1])):
                sequence_last_value = 100

            for file in self._data_dump_files.values():
                if table_schema == file["schema"] and table_name == file["table"]:
                    self._sequences_last_values[full_sequence_name] = {
                        "schema": sequence_schema,
                        "table": table_name,
                        "seq_name": sequence_name,
                        "value": sequence_last_value,
                        "is_excluded": (table_schema, table_name) not in self.context.tables
                    }

    async def _prepare_indexes(self, connection: Connection):
        self._indexes = {}
        views_list = [(view_data['view_schema'], view_data['view_name']) for view_data in self._views.values() if not view_data['is_excluded']]
        indexes_data = await get_indexes_data(connection, self.context.tables + views_list)
        for schema, table, index_name, is_excluded in indexes_data:
            self._indexes[index_name] = {
                "schema": schema,
                "table": table,
                "index_name": index_name,
                "is_excluded": is_excluded,
            }

    async def _prepare_views(self, connection: Connection):
        self._views = {}
        views_data = await get_views_related_to_tables(connection, self.context.tables)
        for view_schema, view_name, view_type, table_schema, table_name, is_excluded in views_data:
            if view_name in self._views and self._views[view_name]["is_excluded"]:
                continue

            self._views[view_name] = {
                "view_schema": view_schema,
                "view_name": view_name,
                "view_type": view_type,
                "table_schema": table_schema,
                "table_name": table_name,
                "is_excluded": is_excluded,
            }

    async def _prepare_constraints(self, connection: Connection):
        self._constraints = {}
        constraints_data = await get_constraints_to_excluded_tables(connection, self.context.tables)

        for table_schema_from, table_name_from, constraint_name, table_schema_to, table_name_to, is_excluded in constraints_data:
            self._constraints[constraint_name] = {
                "table_schema_from": table_schema_from,
                "table_name_from": table_name_from,
                "constraint_name": constraint_name,
                "table_schema_to": table_schema_to,
                "table_name_to": table_name_to,
                "is_excluded": is_excluded,
            }

    async def _prepare_extensions(self, connection: Connection):
        self._extensions = {}
        extensions_data = await get_extensions(connection)
        for schema, name, version, relocatable in extensions_data:
            self._extensions[name] = {
                'schema': schema,
                'name': name,
                'version': version,
                'relocatable': relocatable,
                'is_excluded_by_schema': schema in self.context.exclude_schemas,
            }

    async def _prepare_and_save_metadata(self):
        if self.context.options.dbg_stage_1_validate_dict:
            return

        self.metadata.created = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        self.metadata.pg_version = self.context.pg_version
        self.metadata.pg_dump_version = get_pg_util_version(self.context.pg_dump)

        self.metadata.dictionary_content_hash = {}
        for dictionary_file_name, dictionary_content in self.context.prepared_dictionary_contents.items():
            self.metadata.dictionary_content_hash[dictionary_file_name] = hashlib.sha256(
                dictionary_content.encode("utf-8")
            ).hexdigest()

        self.metadata.prepared_sens_dict_files = ','.join(self.context.options.prepared_sens_dict_files)

        # Schemas and functions used in constraints need to be preserved only when a table whitelist is applied.
        self.metadata.extensions = self._extensions

        if self.context.white_listed_tables:
            self.metadata.partial_dump_schemas = self._schemas

        if self.context.options.mode != AnonMode.SYNC_STRUCT_DUMP:
            self.metadata.files = self._data_dump_files
            self.metadata.sequences_last_values = self._sequences_last_values
            self.metadata.views = self._views
            self.metadata.indexes = self._indexes
            self.metadata.constraints = self._constraints

            self.metadata.total_tables_size = self._total_tables_size
            self.metadata.total_rows = self._total_rows

            self.metadata.db_size = await get_db_size(
                connection_params=self.context.connection_params,
                server_settings=self.context.server_settings,
                db_name=self.context.options.db_name
            )

        self.metadata.dbg_stage_2_validate_data = self.context.options.dbg_stage_2_validate_data
        self.metadata.dbg_stage_3_validate_full = self.context.options.dbg_stage_3_validate_full

        self.metadata.save_into_file(self.metadata_file_path)
        if self._need_dump_data:
            self.metadata.save_dumped_tables_into_file(self.dumped_tables_file_path)

    async def _run_pg_dump(self, section):
        specific_tables = []

        if self.context.black_listed_tables:
            black_list = [
                ("-T", f'"{table_schema}"."{table_name}"') for table_schema, table_name in self.context.black_listed_tables
            ]
            specific_tables.extend([item for sublist in black_list for item in sublist])

        if self.context.white_listed_tables:
            white_list = [
                ("-t", f'"{table_schema}"."{table_name}"') for table_schema, table_name in self.context.white_listed_tables
            ]
            specific_tables.extend([item for sublist in white_list for item in sublist])

        tmp_list = []
        for v in self.context.exclude_schemas:
            tmp_list.append(["--exclude-schema", v])
        exclude_schemas = [item for sublist in tmp_list for item in sublist]

        command = [
            self.context.pg_dump,
            "-h",
            self.context.options.db_host,
            "-p",
            str(self.context.options.db_port),
            "-v",
            "-w",
            "-U",
            self.context.options.db_user,
            *exclude_schemas,
            *specific_tables,
            "--section",
            section,
            "-E",
            "UTF8",
            "-F",
            "c",
            "-s",
            "--no-owner",
            "-f",
            str((self.output_dir / section.replace("-", "_")).with_suffix(".backup")),
            self.context.options.db_name,
        ]
        if not self.context.options.db_host:
            del command[command.index("-h"): command.index("-h") + 2]

        if self.context.options.ignore_privileges:
            command.append("--no-privileges")

        self.context.logger.debug(str(command))
        proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        # pg_dump put command result into stdout if not using "-f" option, else stdout is empty
        # pg_dump put logs into stderr
        _, pg_dump_logs_bytes = proc.communicate()
        pg_dump_logs = pg_dump_logs_bytes.decode('utf-8', errors='replace')

        for log_line in pg_dump_logs.split("\n"):
            self.context.logger.info(log_line)

        if proc.returncode != 0:
            msg = "ERROR: database schema dump has failed!"
            self.context.logger.error(msg)
            raise RuntimeError(msg)

    async def _dump_data_into_file(self, db_conn: Connection, query: str, file_name: Union[str, Path]):
        try:
            if self.context.options.dbg_stage_1_validate_dict:
                return await db_conn.execute(query)

            return await db_conn.copy_from_query(
                query=query,
                output=file_name,
                format="binary",
            )
        except Exception as exc:
            self.context.logger.error(exc)
            raise exc

    async def compress_file(self, file_path: Path, remove_origin_file_after_compress: bool = True):
        gzipped_file_path = file_path.with_name(file_path.name + ".gz")

        self.context.logger.debug(f"Start compressing file: {file_path}")
        with (open(file_path, "rb") as f_in,
              gzip.open(gzipped_file_path, "wb") as f_out):
            f_out.writelines(f_in)
        self.context.logger.debug(f"Compressing has done. Output file: {gzipped_file_path}")

        if remove_origin_file_after_compress:
            self.context.logger.debug(f"Removing origin file: {file_path}")
            file_path.unlink()

    async def _dump_data_by_query(self, pool: Pool, query: str, transaction_snapshot_id: str, file_name: str, process_name: Optional[str] = None):
        binary_output_file_path = self.output_dir / Path(file_name).stem

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

            if not self.context.options.dbg_stage_1_validate_dict:
                # Processing files no need to keep connection, after receiving data into binary file
                self.context.logger.debug(f"Process [{process_name}] Task [{task_id}] Compressing file start - {binary_output_file_path}")

                await self.compress_file(binary_output_file_path)
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

    async def _prepare_dump_queries(self):
        self._data_dump_queries = []
        self._data_dump_files = {}

        for table_schema, table_name in self.context.tables:
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
            )

            if query:
                self.context.logger.info(str(query))
                self._data_dump_queries.append(query)

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
                min_size=self.context.options.db_connections_per_process,
                max_size=self.context.options.db_connections_per_process
            )
            tasks = set()

            try:
                query_tasks_count = len(query_tasks)
                for idx, (file_name, query) in enumerate(query_tasks):
                    if len(tasks) >= self.context.options.db_connections_per_process:
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

                    self.context.logger.debug(f"Process [{name}] New task added. Current tasks in pool: {len(tasks)} / {self.context.options.db_connections_per_process}")

                    if idx % status_ratio:
                        progress_percents = round(float(idx) * 100 / len(query_tasks), 2)
                        self.context.logger.info(f"Process [{name}] Progress {progress_percents}%")

                self.context.logger.debug(f"Process [{name}] All tasks was started")

                if len(tasks) > 0:
                    self.context.logger.debug(f"Process [{name}] Waiting when all tasks will be ended. Current tasks in pool: {len(tasks)} / {self.context.options.db_connections_per_process}")
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

    async def _dump_data(self, connection: Connection):
        if not self._need_dump_data:
            self.context.logger.info("-------------> Skipped dump data")
            return

        self.context.logger.info("-------------> Started dump data")

        try:
            async with connection.transaction(isolation='repeatable_read', readonly=True):
                transaction_snapshot_id = await connection.fetchval("select pg_export_snapshot()")

                # Preparing dump queries
                await self._prepare_dump_queries()
                if not self._data_dump_queries:
                    raise Exception("No objects for dump!")

                queries_chunks = chunkify(
                    list(zip(self._data_dump_files.keys(), self._data_dump_queries)),
                    self.context.options.processes
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
                await self._prepare_views(connection=connection)
                await self._prepare_indexes(connection=connection)
                await self._prepare_constraints(connection=connection)
                await self._prepare_extensions(connection=connection)
                await self._prepare_objects_ddl_to_metadata(connection)
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

    async def _prepare_tables_lists(self, connection: Connection):
        tables = await get_db_tables(connection, self.context.exclude_schemas)
        self.context.set_tables_lists(tables)

    async def _prepare_schemas_lists(self, connection):
        self._all_db_schemas = await get_schemas(connection)
        excluded_schemas = []

        for rule in self.context.prepared_dictionary_obj.get('dictionary_exclude', []):
            table_mask = rule.get('table_mask')
            if table_mask != '*':
                continue

            schema_mask_pattern = None
            if schema_mask := rule.get("schema_mask"):
                schema_mask_pattern = safe_compile(schema_mask)

            for schema in self._all_db_schemas:
                if rule.get("schema") == schema:
                    excluded_schemas.append(schema)
                    break
                elif schema_mask_pattern and schema_mask_pattern.search(schema):
                    excluded_schemas.append(schema)
                    continue

        self._schemas = list(set(self._all_db_schemas) - set(excluded_schemas))
        self.context.exclude_schemas.extend(excluded_schemas)

    async def _prepare_objects_ddl_to_metadata(self, connection: Connection):
        if self.context.white_listed_tables:
            self.metadata.partial_dump_types = await get_custom_types_ddl(connection, self.context.exclude_schemas)
            self.metadata.partial_dump_domains = await get_custom_domains_ddl(connection, self.context.exclude_schemas)
            self.metadata.partial_dump_functions = await get_custom_functions_ddl(connection, self.context.exclude_schemas)
            self.metadata.partial_dump_casts = await get_custom_casts_ddl(connection, self.context.exclude_schemas)
            self.metadata.partial_dump_operators = await get_custom_operators_ddl(connection, self.context.exclude_schemas)
            self.metadata.partial_dump_aggregates = await get_custom_aggregates_ddl(connection, self.context.exclude_schemas)

    def _save_input_dicts_to_run_dir(self):
        if not self.context.options.save_dicts:
            return

        input_dicts_dir = Path(self.context.options.run_dir) / 'input'
        input_dicts_dir.mkdir(parents=True, exist_ok=True)

        input_dict_files = self.context.options.prepared_sens_dict_files
        if self.context.options.partial_tables_dict_files:
            input_dict_files.extend(self.context.options.partial_tables_dict_files)
        if self.context.options.partial_tables_exclude_dict_files:
            input_dict_files.extend(self.context.options.partial_tables_exclude_dict_files)

        for dict_file in input_dict_files:
            shutil.copy2(dict_file, input_dicts_dir / Path(dict_file).name)

    async def run(self) -> None:
        self.context.logger.info("-------------> Started dump")
        connection = None

        try:
            self._save_input_dicts_to_run_dir()

            connection = await create_connection(
                self.context.connection_params,
                server_settings=self.context.server_settings
            )

            self.context.read_prepared_dict()
            self.context.read_partial_tables_dicts()
            self._prepare_output_dir()

            await self._prepare_schemas_lists(connection)
            await self._prepare_tables_lists(connection)
            await self._dump_pre_data()
            await self._dump_post_data()
            await self._dump_data(connection)
            await self._prepare_and_save_metadata()

            self.context.logger.info("<------------- Finished dump")
        except Exception as ex:
            raise ex
        finally:
            if connection:
                await connection.close()

            if self.context.options.save_dicts:
                save_dicts_info_file(self.context.options)