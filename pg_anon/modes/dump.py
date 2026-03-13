import asyncio
import gzip
import hashlib
import multiprocessing
import os
import re
import shlex
import shutil
import subprocess
import uuid
from datetime import datetime
from pathlib import Path

from aioprocessing import AioQueue
from asyncpg import Connection, Pool

from pg_anon.common.db_queries import get_relation_size_query, get_sequences_query
from pg_anon.common.db_utils import (
    check_required_connections,
    create_connection,
    create_pool,
    get_constraints_to_excluded_tables,
    get_custom_aggregates_ddl,
    get_custom_casts_ddl,
    get_custom_domains_ddl,
    get_custom_functions_ddl,
    get_custom_operators_ddl,
    get_custom_types_ddl,
    get_db_size,
    get_db_tables,
    get_dump_query,
    get_extensions,
    get_indexes_data,
    get_schemas,
    get_views_related_to_tables,
)
from pg_anon.common.dto import Metadata
from pg_anon.common.enums import AnonMode
from pg_anon.common.errors import ErrorCode, PgAnonError
from pg_anon.common.multiprocessing_utils import init_process
from pg_anon.common.utils import (
    chunkify,
    get_dict_rule_for_table,
    get_pg_util_version,
    safe_compile,
    save_dicts_info_file,
)
from pg_anon.context import Context


class DumpMode:
    context: Context
    output_dir: Path

    metadata: Metadata
    metadata_file_name: str = "metadata.json"
    dumped_tables_file_name: str = "dumped_tables.py"

    _data_dump_queries: list[str] | None = None
    _data_dump_files: dict | None = None
    _data_dump_tasks_results: dict | None = None

    _total_tables_size: int = 0
    _total_rows: int = 0

    _schemas: list[str] = None
    _sequences_data: list[tuple[str]] | None = None
    _sequences_last_values: dict = None
    _indexes: dict = None
    _views: dict = None
    _constraints: dict = None
    _extensions: dict = None

    _views_for_including: list[str] = None
    _views_for_excluding: list[str] = None

    _need_dump_pre_and_post_sections: bool = True
    _need_dump_data: bool = True
    _skip_pre_data_dump: bool = False
    _skip_post_data_dump: bool = False

    def __init__(self, context: Context) -> None:
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

    def _prepare_output_dir(self) -> None:
        if self.context.options.dbg_stage_1_validate_dict:
            return

        self.output_dir.mkdir(parents=True, exist_ok=True)

        if self.output_dir_is_empty:
            return
        if not self.context.options.clear_output_dir:
            msg = f"Output directory {self.output_dir} is not empty!"
            self.context.logger.error(msg)
            raise PgAnonError(ErrorCode.OUTPUT_DIR_NOT_EMPTY, msg)
        self._clear_output_dir()

    def _clear_output_dir(self) -> None:
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
                    raise PgAnonError(ErrorCode.INVALID_OUTPUT_DIR, msg)

                file_path.unlink()

    @property
    def output_dir_is_empty(self) -> bool:
        return not any(self.output_dir.iterdir())

    async def _count_totals(self, connection: Connection) -> None:
        for query, file_key in zip(self._data_dump_queries, self._data_dump_files, strict=True):
            file = self._data_dump_files[file_key]

            result_key = hashlib.sha256(query.encode()).hexdigest()
            file.update({"rows": self._data_dump_tasks_results[result_key]})

            schema = file["schema"].replace("'", "''")
            table = file["table"].replace("'", "''")

            self._total_tables_size += await connection.fetchval(get_relation_size_query(schema=schema, table=table))
            self._total_rows += int(file["rows"])

    async def _prepare_sequences_last_values(self, connection: Connection) -> None:
        self._sequences_last_values = {}

        for table_schema, table_name, _, sequence_schema, sequence_name in self._sequences_data:
            full_sequence_name = sequence_schema + "." + sequence_name
            sequence_last_value = await connection.fetchval(
                f'select last_value from "{sequence_schema}"."{sequence_name}"'
            )
            if (
                self.context.options.dbg_stage_2_validate_data or self.context.options.dbg_stage_3_validate_full
            ) and sequence_last_value > int(self.context.validate_limit.split()[1]):
                sequence_last_value = 100

            for file in self._data_dump_files.values():
                if table_schema == file["schema"] and table_name == file["table"]:
                    self._sequences_last_values[full_sequence_name] = {
                        "schema": sequence_schema,
                        "table": table_name,
                        "seq_name": sequence_name,
                        "value": sequence_last_value,
                        "is_excluded": (table_schema, table_name) not in self.context.tables,
                    }

    async def _prepare_indexes(self, connection: Connection) -> None:
        self._indexes = {}
        views_list = [
            (view_data["view_schema"], view_data["view_name"])
            for view_data in self._views.values()
            if not view_data["is_excluded"]
        ]
        indexes_data = await get_indexes_data(connection, self.context.tables + views_list)
        for schema, table, index_name, is_excluded in indexes_data:
            self._indexes[index_name] = {
                "schema": schema,
                "table": table,
                "index_name": index_name,
                "is_excluded": is_excluded,
            }

    async def _prepare_views(self, connection: Connection) -> None:
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

    async def _prepare_constraints(self, connection: Connection) -> None:
        self._constraints = {}
        constraints_data = await get_constraints_to_excluded_tables(connection, self.context.tables)

        for (
            table_schema_from,
            table_name_from,
            constraint_name,
            table_schema_to,
            table_name_to,
            is_excluded,
        ) in constraints_data:
            self._constraints[constraint_name] = {
                "table_schema_from": table_schema_from,
                "table_name_from": table_name_from,
                "constraint_name": constraint_name,
                "table_schema_to": table_schema_to,
                "table_name_to": table_name_to,
                "is_excluded": is_excluded,
            }

    async def _prepare_extensions(self, connection: Connection) -> None:
        self._extensions = {}
        extensions_data = await get_extensions(connection)
        for schema, name, version, relocatable in extensions_data:
            self._extensions[name] = {
                "schema": schema,
                "name": name,
                "version": version,
                "relocatable": relocatable,
                "is_excluded_by_schema": schema in self.context.exclude_schemas,
            }

    async def _prepare_and_save_metadata(self) -> None:
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

        self.metadata.prepared_sens_dict_files = ",".join(self.context.options.prepared_sens_dict_files)

        self.metadata.extensions = self._extensions

        if self.context.white_listed_tables or self.context.black_listed_tables:
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
                db_name=self.context.options.db_name,
            )

        self.metadata.dbg_stage_2_validate_data = self.context.options.dbg_stage_2_validate_data
        self.metadata.dbg_stage_3_validate_full = self.context.options.dbg_stage_3_validate_full

        self.metadata.save_into_file(self.metadata_file_path)
        if self._need_dump_data:
            self.metadata.save_dumped_tables_into_file(self.dumped_tables_file_path)

    async def _run_pg_dump(self, section: str) -> None:
        specific_tables = []

        if self.context.black_listed_tables:
            black_list = [
                ("-T", f'"{table_schema}"."{table_name}"')
                for table_schema, table_name in self.context.black_listed_tables
            ]
            specific_tables.extend([item for sublist in black_list for item in sublist])

        if self.context.white_listed_tables:
            white_list = [
                ("-t", f'"{table_schema}"."{table_name}"')
                for table_schema, table_name in self.context.white_listed_tables
            ]
            specific_tables.extend([item for sublist in white_list for item in sublist])

            if self._sequences_data:
                seq_list = [
                    ("-t", f'"{seq_schema}"."{seq_name}"')
                    for table_schema, table_name, _, seq_schema, seq_name in self._sequences_data
                    if (table_schema, table_name) in self.context.white_listed_tables
                ]
                specific_tables.extend([item for sublist in seq_list for item in sublist])

        exclude_schemas = [item for v in self.context.exclude_schemas for item in ["--exclude-schema", v]]

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
        ]
        if not self.context.options.db_host:
            del command[command.index("-h") : command.index("-h") + 2]

        if self.context.options.ignore_privileges:
            command.append("--no-privileges")

        if self.context.options.pg_dump_options:
            command.extend(shlex.split(self.context.options.pg_dump_options))

        command.append(self.context.options.db_name)
        self.context.logger.debug(str(command))
        proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        # pg_dump put command result into stdout if not using "-f" option, else stdout is empty
        # pg_dump put logs into stderr
        _, pg_dump_logs_bytes = proc.communicate()
        pg_dump_logs = pg_dump_logs_bytes.decode("utf-8", errors="replace")

        for log_line in pg_dump_logs.split("\n"):
            self.context.logger.info(log_line)

        if proc.returncode != 0:
            msg = "ERROR: database schema dump has failed!"
            self.context.logger.error(msg)
            raise PgAnonError(ErrorCode.DUMP_FAILED, msg)

    async def _dump_data_into_file(self, db_conn: Connection, query: str, file_name: str | Path) -> str:
        try:
            if self.context.options.dbg_stage_1_validate_dict:
                return await db_conn.execute(query)

            return await db_conn.copy_from_query(
                query=query,
                output=file_name,
                format="binary",
            )
        except Exception:
            self.context.logger.exception("Exception in _dump_data_into_file")
            raise

    async def compress_file(self, file_path: Path, remove_origin_file_after_compress: bool = True) -> None:
        return await asyncio.to_thread(
            self._compress_file,
            file_path,
            remove_origin_file_after_compress=remove_origin_file_after_compress,
        )

    def _compress_file(self, file_path: Path, remove_origin_file_after_compress: bool = True) -> None:
        gzipped_file_path = file_path.with_name(file_path.name + ".gz")

        self.context.logger.debug("Start compressing file: %s", file_path)
        with file_path.open("rb") as f_in, gzip.open(gzipped_file_path, "wb") as f_out:
            f_out.writelines(f_in)
        self.context.logger.debug("Compressing has done. Output file: %s", gzipped_file_path)

        if remove_origin_file_after_compress:
            self.context.logger.debug("Removing origin file: %s", file_path)
            file_path.unlink()

    async def _dump_data_by_query(
        self, pool: Pool, query: str, transaction_snapshot_id: str, file_name: str, process_name: str | None = None
    ) -> dict:
        binary_output_file_path = self.output_dir / Path(file_name).stem

        task_id = uuid.uuid4()
        dump_is_complete = False
        compress_is_complete = False
        self.context.logger.info(
            "================> Process [%s] Task [%s] Started task %s to file %s",
            process_name, task_id, query, binary_output_file_path,
        )

        try:
            self.context.logger.debug("Process [%s] Task [%s] Connection acquiring", process_name, task_id)
            async with pool.acquire() as db_conn:
                self.context.logger.debug("Process [%s] Task [%s] Connection acquired", process_name, task_id)
                async with db_conn.transaction(isolation="repeatable_read", readonly=True):
                    await db_conn.execute(f"SET TRANSACTION SNAPSHOT '{transaction_snapshot_id}';")
                    self.context.logger.debug(
                        "Process [%s] Task [%s] Transaction opened. Starting dump query",
                        process_name, task_id,
                    )
                    result = await self._dump_data_into_file(
                        db_conn=db_conn,
                        query=query,
                        file_name=binary_output_file_path,
                    )
                    self.context.logger.debug(
                        "Process [%s] Task [%s] Transaction setup to snapshot %s",
                        process_name, task_id, transaction_snapshot_id,
                    )

            dump_is_complete = True
            count_rows = re.findall(r"(\d+)", result)[0]
            self.context.logger.debug(
                "Process [%s] Task [%s] COPY %s [rows] Task: %s",
                process_name, task_id, count_rows, query,
            )

            if not self.context.options.dbg_stage_1_validate_dict:
                # Processing files no need to keep connection, after receiving data into binary file
                self.context.logger.debug(
                    "Process [%s] Task [%s] Compressing file start - %s",
                    process_name, task_id, binary_output_file_path,
                )

                await self.compress_file(binary_output_file_path)
                compress_is_complete = True
                self.context.logger.debug(
                    "Process [%s] Task [%s] Compressing file end - %s",
                    process_name, task_id, binary_output_file_path,
                )

        except Exception as ex:
            self.context.logger.exception(
                "Process [%s] Task [%s] Exception in DumpMode._dump_data_by_query",
                process_name, task_id
            )
            if pool.is_closing():
                self.context.logger.debug("Process [%s] Task [%s] Pool closed!", process_name, task_id)

            error_message = f"Something went wrong: {ex}"
            if not dump_is_complete:
                error_message = f"Can't execute query: {query}"
            elif not compress_is_complete:
                error_message = f"Can't compress file: {binary_output_file_path}"

            self.context.logger.debug(
                "Process [%s] Task [%s] Error: %s", process_name, task_id, error_message,
            )
            raise PgAnonError(ErrorCode.DUMP_FAILED, error_message) from ex

        self.context.logger.info(
            "<================ Process [%s] Task [%s] Finished task %s",
            process_name, task_id, query,
        )

        return {hashlib.sha256(query.encode()).hexdigest(): count_rows}

    async def _prepare_dump_queries(self) -> None:
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

    def _process_dump_data(  # noqa: C901, PLR0915
        self,
        name: str,
        queue: AioQueue,
        query_tasks: list[tuple[str, str]],
        stop_event: multiprocessing.Event,
        transaction_snapshot_id: str | None = None,
    ) -> None:
        tasks_res = []

        status_ratio = 10
        if len(query_tasks) > 1000:  # noqa: PLR2004
            status_ratio = 100
        if len(query_tasks) > 50000:  # noqa: PLR2004
            status_ratio = 1000

        def _should_stop() -> bool:
            return stop_event is not None and stop_event.is_set()

        async def _wait_and_check(tasks: set) -> set | None:
            done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)

            if _should_stop():
                self.context.logger.info("Process [%s] received stop signal, terminating", name)
                return None

            for done_task in done:
                if exc := done_task.exception():
                    self.context.logger.error("<================ Process [%s]: %s", name, exc)
                    raise exc

            return pending

        async def _process_run() -> None:
            self.context.logger.debug("================> Process [%s] Connection pool opening", name)

            pool = await create_pool(
                connection_params=self.context.connection_params,
                server_settings=self.context.server_settings,
                min_size=self.context.options.db_connections_per_process,
                max_size=self.context.options.db_connections_per_process,
            )
            tasks = set()

            try:
                query_tasks_count = len(query_tasks)
                for idx, (file_name, query) in enumerate(query_tasks):
                    if _should_stop():
                        self.context.logger.info("Process [%s] received stop signal, terminating", name)
                        return

                    while len(tasks) >= self.context.options.db_connections_per_process:
                        tasks = await _wait_and_check(tasks)
                        if tasks is None:
                            return

                    self.context.logger.debug(
                        "Process [%s] Adding new task into pool [%s/%s]",
                        name, idx + 1, query_tasks_count,
                    )
                    task_res = loop.create_task(
                        self._dump_data_by_query(
                            pool=pool,
                            query=query,
                            transaction_snapshot_id=transaction_snapshot_id,
                            file_name=file_name,
                            process_name=name,
                        )
                    )

                    tasks.add(task_res)
                    tasks_res.append(task_res)

                    self.context.logger.debug(
                        "Process [%s] New task added. Current tasks in pool: %s / %s",
                        name, len(tasks), self.context.options.db_connections_per_process,
                    )

                    if idx % status_ratio == 0:
                        progress_percents = round(float(idx) * 100 / query_tasks_count, 2)
                        self.context.logger.info("Process [%s] Progress %s%%", name, progress_percents)

                while tasks:
                    tasks = await _wait_and_check(tasks)
                    if tasks is None:
                        return
            finally:
                self.context.logger.debug("<================ Process [%s] Connection pool closing", name)

                if pool.is_closing():
                    self.context.logger.debug(
                        "<================ Process [%s] Connection pool already has been closed!", name,
                    )
                else:
                    await pool.close()
                    self.context.logger.debug("<================ Process [%s] Connection pool closed", name)

        self.context.logger.info("================> Process [%s] Started process_dump_impl", name)
        loop = asyncio.new_event_loop()

        try:
            self.context.logger.debug("Process [%s] Setup event loop", name)
            asyncio.set_event_loop(loop)

            self.context.logger.debug("Process [%s] Run dump tasks", name)
            loop.run_until_complete(_process_run())

            self.context.logger.debug("Process [%s] Processing results start", name)
            tasks_res_final = [r for task in tasks_res if (r := task.result()) is not None and len(r) > 0]

            queue.put(tasks_res_final)
            self.context.logger.debug("Process [%s] Processing results end", name)
        except Exception as ex:
            self.context.logger.exception("<================ Process [%s]", name)
            queue.put([ex])  # Send exception to parent process
        finally:
            self.context.logger.debug("<================ Process [%s] closing", name)
            loop.close()
            queue.put(None)  # Shut down the worker
            queue.close()
            self.context.logger.debug("<================ Process [%s] closed", name)

    async def _dump_data(self, connection: Connection) -> None:  # noqa: C901
        if not self._need_dump_data:
            self.context.logger.info("-------------> Skipped dump data")
            return

        self.context.logger.info("-------------> Started dump data")

        try:
            async with connection.transaction(isolation="repeatable_read", readonly=True):
                transaction_snapshot_id = await connection.fetchval("select pg_export_snapshot()")

                # Preparing dump queries
                await self._prepare_dump_queries()
                if not self._data_dump_queries:
                    raise PgAnonError(ErrorCode.NO_OBJECTS_FOR_DUMP, "No objects for dump!")

                queries_chunks = chunkify(
                    list(zip(self._data_dump_files.keys(), self._data_dump_queries, strict=True)),
                    self.context.options.processes,
                )

                # Shared event to signal all processes to stop on error
                stop_event = multiprocessing.Event()

                process_tasks = []
                for idx, queries_chunk in enumerate(queries_chunks):
                    process_tasks.append(
                        asyncio.ensure_future(
                            init_process(
                                name=str(idx + 1),
                                ctx=self.context,
                                target_func=self._process_dump_data,
                                tasks=queries_chunk,
                                stop_event=stop_event,
                                transaction_snapshot_id=transaction_snapshot_id,
                            )
                        )
                    )

                # Wait with immediate error detection while keeping transaction alive
                remaining = process_tasks
                while remaining:
                    # Use timeout to periodically run SELECT 1 for keeping transaction alive
                    done, pending = await asyncio.wait(remaining, timeout=5, return_when=asyncio.FIRST_EXCEPTION)

                    # Keep main transaction active (needed for large databases with long dumps)
                    await connection.execute("SELECT 1")

                    for task in done:
                        if task.exception():
                            # Signal all processes to stop
                            stop_event.set()
                            # Wait for remaining processes to finish (with timeout)
                            if pending:
                                await asyncio.wait(pending, timeout=10)
                            raise task.exception()

                    remaining = list(pending)

                self._data_dump_tasks_results = {}
                for process_task in process_tasks:
                    process_task_result = process_task.result()
                    if not process_task_result:
                        raise PgAnonError(ErrorCode.DUMP_QUERY_FAILED, "One or more dump queries has been failed!")

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

    async def _dump_pre_data(self) -> None:
        if self._skip_pre_data_dump:
            self.context.logger.info("-------------> Skipped dump pre-data (pg_dump)")
            return

        self.context.logger.info("-------------> Started dump pre-data (pg_dump)")
        await self._run_pg_dump("pre-data")
        self.context.logger.info("<------------- Finished dump pre-data (pg_dump)")

    async def _dump_post_data(self) -> None:
        if self._skip_post_data_dump:
            self.context.logger.info("-------------> Skipped dump post-data (pg_dump)")
            return

        self.context.logger.info("-------------> Started dump post-data (pg_dump)")
        await self._run_pg_dump("post-data")
        self.context.logger.info("<------------- Finished dump post-data (pg_dump)")

    async def _fetch_sequences_data(self, connection: Connection) -> None:
        """Fetch sequences data and cache for reuse in pg_dump and metadata."""
        query = get_sequences_query(self.context.exclude_schemas)
        self.context.logger.debug(str(query))
        self._sequences_data = [tuple(row) for row in await connection.fetch(query)]

    async def _prepare_tables_lists(self, connection: Connection) -> None:
        tables = await get_db_tables(connection, self.context.exclude_schemas)
        self.context.set_tables_lists(tables)

    async def _prepare_schemas_lists(self, connection: Connection) -> None:
        self._all_db_schemas = await get_schemas(connection)
        excluded_schemas = []

        for rule in self.context.prepared_dictionary_obj.get("dictionary_exclude", []):
            table_mask = rule.get("table_mask")
            if table_mask != "*":
                continue

            schema_mask_pattern = None
            if schema_mask := rule.get("schema_mask"):
                schema_mask_pattern = safe_compile(schema_mask)

            for schema in self._all_db_schemas:
                if rule.get("schema") == schema:
                    excluded_schemas.append(schema)
                    break
                if schema_mask_pattern and schema_mask_pattern.search(schema):
                    excluded_schemas.append(schema)
                    continue

        self._schemas = list(set(self._all_db_schemas) - set(excluded_schemas))
        self.context.exclude_schemas.extend(excluded_schemas)

    async def _prepare_objects_ddl_to_metadata(self, connection: Connection) -> None:
        if self.context.white_listed_tables or self.context.black_listed_tables:
            self.metadata.partial_dump_types = await get_custom_types_ddl(connection, self.context.exclude_schemas)
            self.metadata.partial_dump_domains = await get_custom_domains_ddl(connection, self.context.exclude_schemas)
            self.metadata.partial_dump_functions = await get_custom_functions_ddl(
                connection, self.context.exclude_schemas
            )
            self.metadata.partial_dump_casts = await get_custom_casts_ddl(connection, self.context.exclude_schemas)
            self.metadata.partial_dump_operators = await get_custom_operators_ddl(
                connection, self.context.exclude_schemas
            )
            self.metadata.partial_dump_aggregates = await get_custom_aggregates_ddl(
                connection, self.context.exclude_schemas
            )

    def _save_input_dicts_to_run_dir(self) -> None:
        if not self.context.options.save_dicts:
            return

        input_dicts_dir = Path(self.context.options.run_dir) / "input"
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
                self.context.connection_params, server_settings=self.context.server_settings
            )

            required_connections = self.context.options.processes * self.context.options.db_connections_per_process
            await check_required_connections(connection, required_connections)

            self.context.read_prepared_dict()
            self.context.read_partial_tables_dicts()
            self._prepare_output_dir()

            await self._prepare_schemas_lists(connection)
            await self._prepare_tables_lists(connection)
            await self._fetch_sequences_data(connection)
            await self._dump_pre_data()
            await self._dump_post_data()
            await self._dump_data(connection)
            await self._prepare_and_save_metadata()

            self.context.logger.info("<------------- Finished dump")
        finally:
            if connection:
                await connection.close()

            if self.context.options.save_dicts:
                save_dicts_info_file(self.context.options)
