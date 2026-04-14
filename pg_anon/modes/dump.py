import asyncio
import gzip
import hashlib
import os
import re
import shlex
import shutil
import subprocess
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

from asyncpg import Connection, Pool

from pg_anon.common.db_queries import get_relation_size_query, get_sequences_query
from pg_anon.common.db_utils import (
    check_required_connections,
    create_connection,
    create_pool,
    get_all_fields_list,
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
from pg_anon.common.utils import get_dict_rule_for_table, get_pg_util_version, safe_compile, save_dicts_info_file
from pg_anon.context import Context


class DumpMode:
    def __init__(self, context: Context) -> None:
        self.context: Context = context
        self.metadata: Metadata = Metadata()
        self.metadata_file_name: str = "metadata.json"
        self.dumped_tables_file_name: str = "dumped_tables.py"

        self._data_dump_queries: list[str] = []
        self._data_dump_files: dict[str, dict[str, Any]] = {}
        self._data_dump_tasks_results: dict[str, int] = {}

        self._total_tables_size: int = 0
        self._total_rows: int = 0

        self._schemas: list[str] = []
        self._sequences_data: list[tuple[str, ...]] = []
        self._sequences_last_values: dict | None = None
        self._indexes: dict | None = None
        self._views: dict | None = None
        self._constraints: dict | None = None
        self._extensions: dict | None = None

        self._views_for_including: list[str] = []
        self._views_for_excluding: list[str] = []
        self._all_db_schemas: list[str] = []

        if self.context.options.db_user_password:
            os.environ["PGPASSWORD"] = self.context.options.db_user_password

        if not self.context.options.output_dir:
            if not self.context.options.prepared_sens_dict_files:
                raise PgAnonError(ErrorCode.NO_DICT_FILES, "No prepared sens dict files specified")
            prepared_dict_name = Path(self.context.options.prepared_sens_dict_files[0]).stem
            self.output_dir: Path = Path.cwd() / prepared_dict_name
        else:
            self.output_dir = Path.cwd() / self.context.options.output_dir

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
        """Check whether the output directory is empty."""
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
        if not self._sequences_data:
            return
        for table_schema, table_name, _, sequence_schema, sequence_name in self._sequences_data:
            full_sequence_name = sequence_schema + "." + sequence_name
            sequence_last_value = await connection.fetchval(
                f'select last_value from "{sequence_schema}"."{sequence_name}"'
            )
            if (
                self.context.options.dbg_stage_2_validate_data or self.context.options.dbg_stage_3_validate_full
            ) and sequence_last_value > int(self.context.validate_limit.split()[1]):
                sequence_last_value = 100

            for file in (self._data_dump_files or {}).values():
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
            for view_data in (self._views or {}).values()
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

        self.metadata.prepared_sens_dict_files = ",".join(self.context.options.prepared_sens_dict_files or [])

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
        specific_tables: list[str] = []

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
        """Compress the specified file asynchronously."""
        await asyncio.to_thread(
            self._compress_file,
            file_path,
            remove_origin_file_after_compress=remove_origin_file_after_compress,
        )

    def _compress_file(self, file_path: Path, remove_origin_file_after_compress: bool = True) -> None:
        gzipped_file_path = file_path.with_name(file_path.name + ".gz")

        self.context.logger.debug("Start compressing file: %s", file_path)
        with (
            file_path.open("rb") as f_in,
            gzip.open(gzipped_file_path, "wb", compresslevel=1) as f_out,
        ):
            shutil.copyfileobj(f_in, f_out, length=1024 * 1024)
        self.context.logger.debug("Compressing has done. Output file: %s", gzipped_file_path)

        if remove_origin_file_after_compress:
            self.context.logger.debug("Removing origin file: %s", file_path)
            file_path.unlink()

    async def _dump_data_by_query(
        self,
        pool: Pool,
        query: str,
        transaction_snapshot_id: str,
        file_name: str,
    ) -> tuple[dict[str, str], Path]:
        binary_output_file_path = self.output_dir / Path(file_name).stem

        task_id = uuid.uuid4()
        self.context.logger.info(
            "================> Task [%s] Started task %s to file %s",
            task_id, query, binary_output_file_path,
        )

        try:
            self.context.logger.debug("Task [%s] Connection acquiring", task_id)
            async with pool.acquire() as db_conn:
                self.context.logger.debug("Task [%s] Connection acquired", task_id)
                async with db_conn.transaction(isolation="repeatable_read", readonly=True):
                    await db_conn.execute(f"SET TRANSACTION SNAPSHOT '{transaction_snapshot_id}';")
                    self.context.logger.debug("Task [%s] Transaction opened. Starting dump query", task_id)
                    result = await self._dump_data_into_file(
                        db_conn=db_conn,
                        query=query,
                        file_name=binary_output_file_path,
                    )
                    self.context.logger.debug(
                        "Task [%s] Transaction setup to snapshot %s", task_id, transaction_snapshot_id
                    )

            count_rows = re.findall(r"(\d+)", result)[0]
            self.context.logger.debug("Task [%s] COPY %s [rows] Task: %s", task_id, count_rows, query)

        except Exception as exc:
            self.context.logger.exception("Task [%s] Exception in DumpMode._dump_data_by_query", task_id)
            raise PgAnonError(ErrorCode.DUMP_FAILED, f"Can't execute query: {query}") from exc

        self.context.logger.info("<================ Task [%s] Finished task %s", task_id, query)

        result_hash = hashlib.sha256(query.encode()).hexdigest()
        return {result_hash: count_rows}, binary_output_file_path

    async def _compress_with_semaphore(self, file_path: Path, semaphore: asyncio.Semaphore) -> None:
        try:
            async with semaphore:
                await self.compress_file(file_path)
        except Exception as exc:
            self.context.logger.exception("Can't compress file: %s", file_path)
            raise PgAnonError(ErrorCode.DUMP_FAILED, f"Can't compress file: {file_path}") from exc

    async def _prepare_dump_queries(self) -> None:
        self._data_dump_queries = []
        self._data_dump_files = {}

        fields_cache = await get_all_fields_list(
            connection_params=self.context.connection_params,
            exclude_schemas=self.context.exclude_schemas,
            server_settings=self.context.server_settings,
        )

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
                fields_cache=fields_cache,
            )

            if query:
                self.context.logger.info(str(query))
                self._data_dump_queries.append(query)

    async def _run_dump_tasks(  # noqa: C901
        self,
        query_tasks: list[tuple[str, str]],
        transaction_snapshot_id: str,
        compression_semaphore: asyncio.Semaphore,
    ) -> dict:
        pool = await create_pool(
            connection_params=self.context.connection_params,
            server_settings=self.context.server_settings,
            min_size=self.context.options.db_connections_per_process,
            max_size=self.context.options.db_connections_per_process,
        )

        results: dict[str, str] = {}
        dump_tasks: set[asyncio.Task] = set()
        compress_tasks: set[asyncio.Task] = set()

        status_ratio = 10
        if len(query_tasks) > 1000:  # noqa: PLR2004
            status_ratio = 100
        if len(query_tasks) > 50000:  # noqa: PLR2004
            status_ratio = 1000

        def _collect_dump_result(done_task: asyncio.Task) -> None:
            result_dict, file_path = done_task.result()
            results.update(result_dict)
            if not self.context.options.dbg_stage_1_validate_dict:
                compress_tasks.add(
                    asyncio.create_task(self._compress_with_semaphore(file_path, compression_semaphore))
                )

        try:
            query_tasks_count = len(query_tasks)
            for idx, (file_name, query) in enumerate(query_tasks):
                while len(dump_tasks) >= self.context.options.db_connections_per_process:
                    done, dump_tasks = await asyncio.wait(dump_tasks, return_when=asyncio.FIRST_COMPLETED)
                    for done_task in done:
                        if exc := done_task.exception():
                            for t in dump_tasks:
                                t.cancel()
                            raise exc
                        _collect_dump_result(done_task)

                self.context.logger.debug("Adding new task [%s/%s]", idx + 1, query_tasks_count)
                task = asyncio.create_task(
                    self._dump_data_by_query(
                        pool=pool,
                        query=query,
                        transaction_snapshot_id=transaction_snapshot_id,
                        file_name=file_name,
                    )
                )
                dump_tasks.add(task)

                self.context.logger.debug(
                    "New task added. Current dump tasks: %s / %s",
                    len(dump_tasks),
                    self.context.options.db_connections_per_process,
                )

                if idx % status_ratio == 0:
                    progress_percents = round(float(idx) * 100 / query_tasks_count, 2)
                    self.context.logger.info("Progress %s%%", progress_percents)

            # Wait remaining dump tasks
            while dump_tasks:
                done, dump_tasks = await asyncio.wait(dump_tasks, return_when=asyncio.FIRST_COMPLETED)
                for done_task in done:
                    if exc := done_task.exception():
                        for t in dump_tasks:
                            t.cancel()
                        raise exc
                    _collect_dump_result(done_task)

            # Wait remaining compress tasks
            if compress_tasks:
                done, _ = await asyncio.wait(compress_tasks)
                for done_task in done:
                    if exc := done_task.exception():
                        raise exc
        finally:
            await pool.close()

        return results

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

                self.context.logger.info(
                    "Using %s concurrent connections",
                    self.context.options.db_connections_per_process,
                )

                compression_semaphore = asyncio.Semaphore(self.context.options.processes)
                all_query_tasks = list(zip(self._data_dump_files.keys(), self._data_dump_queries, strict=False))

                dump_task = asyncio.create_task(
                    self._run_dump_tasks(
                        query_tasks=all_query_tasks,
                        transaction_snapshot_id=transaction_snapshot_id,
                        compression_semaphore=compression_semaphore,
                    )
                )

                # Keep main transaction active while dump tasks run
                try:
                    while not dump_task.done():
                        await asyncio.wait({dump_task}, timeout=5)
                        if not dump_task.done():
                            await connection.execute("SELECT 1")
                except Exception:
                    dump_task.cancel()
                    raise

                self._data_dump_tasks_results = dump_task.result()

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

        input_dict_files: list[str] = list(self.context.options.prepared_sens_dict_files or [])
        if self.context.options.partial_tables_dict_files:
            input_dict_files.extend(self.context.options.partial_tables_dict_files)
        if self.context.options.partial_tables_exclude_dict_files:
            input_dict_files.extend(self.context.options.partial_tables_exclude_dict_files)

        for dict_file in input_dict_files:
            shutil.copy2(dict_file, input_dicts_dir / Path(dict_file).name)

    async def run(self) -> None:
        """Run the dump mode to export anonymized database data."""
        self.context.logger.info("-------------> Started dump")
        connection = None

        try:
            self._save_input_dicts_to_run_dir()

            connection = await create_connection(
                self.context.connection_params, server_settings=self.context.server_settings
            )

            await check_required_connections(connection, self.context.options.db_connections_per_process)

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
