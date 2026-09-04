import asyncio
import gzip
import hashlib
import re
import shlex
import shutil
import subprocess
import uuid
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any

from asyncpg import Connection, Pool

from pg_anon.common.db_queries import get_sequences_query
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
    get_custom_ranges_ddl,
    get_custom_types_ddl,
    get_db_size,
    get_db_tables,
    get_dump_query,
    get_event_triggers_in_schemas,
    get_extensions,
    get_foreign_servers_count,
    get_indexes_data,
    get_legacy_inheritance_parents,
    get_partition_ancestors_map,
    get_partitioned_ancestors,
    get_schemas,
    get_user_routines_and_triggers_count,
    get_views_related_to_tables,
    get_visible_user_mappings,
)
from pg_anon.common.dto import Metadata
from pg_anon.common.enums import AnonMode
from pg_anon.common.errors import ErrorCode, PgAnonError
from pg_anon.common.utils import (
    build_pg_util_env,
    get_dict_rule_for_table,
    get_major_version,
    get_pg_util_version,
    safe_compile,
    save_dicts_info_file,
)
from pg_anon.context import Context


class _DumpFlagPos(Enum):
    NORMAL = "normal"  # before user --pg-dump-options
    LAST = "last"  # after them, before positional db_name


# pg_dump hardening flags applied by default, gated by the pg_dump binary major version.
_HARDENING_DUMP_FLAGS: list[tuple[str, int, int | None, _DumpFlagPos, str]] = [
    (
        "--no-subscriptions",
        10,
        None,
        _DumpFlagPos.NORMAL,
        "excludes replication subscriptions, whose connection string may contain a password",
    ),
    (
        "--no-statistics",
        18,
        None,
        _DumpFlagPos.LAST,
        "excludes planner statistics, which may contain real column values",
    ),
]


def _applicable_hardening_dump_flags(pg_dump_major: int) -> list[tuple[str, _DumpFlagPos, str]]:
    result: list[tuple[str, _DumpFlagPos, str]] = []
    for flag, min_major, max_major, position, reason in _HARDENING_DUMP_FLAGS:
        if pg_dump_major < min_major:
            continue
        if max_major is not None and pg_dump_major > max_major:
            continue
        result.append((flag, position, reason))
    return result


class DumpMode:
    def __init__(self, context: Context) -> None:
        self.context: Context = context
        self.metadata: Metadata = Metadata()
        self.metadata_file_name: str = "metadata.json"
        self.dumped_tables_file_name: str = "dumped_tables.py"

        self._data_dump_queries: list[str] = []
        self._data_dump_files: dict[str, dict[str, Any]] = {}
        self._data_dump_tasks_results: dict[str, int] = {}

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
        self._pg_dump_partitioned_ancestors: set[tuple[str, str]] = set()
        self._partition_ancestors_map: dict[tuple[str, str], list[tuple[str, str]]] = {}
        self._pg_dump_major: int | None = None

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

    def _count_totals(self) -> None:
        for query, file_key in zip(self._data_dump_queries, self._data_dump_files, strict=True):
            file = self._data_dump_files[file_key]

            result_key = hashlib.sha256(query.encode()).hexdigest()
            file.update({"rows": self._data_dump_tasks_results[result_key]})

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
        for row in indexes_data:
            self._indexes[row["index_name"]] = {
                "schema": row["schema"],
                "table": row["table"],
                "index_name": row["index_name"],
                "is_excluded": row["is_excluded"],
                "parent_index_schema": row["parent_index_schema"],
                "parent_index_name": row["parent_index_name"],
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

        for row in constraints_data:
            self._constraints[row["constraint_name"]] = {
                "table_schema_from": row["table_schema_from"],
                "table_name_from": row["table_name_from"],
                "constraint_name": row["constraint_name"],
                "table_schema_to": row["table_schema_to"],
                "table_name_to": row["table_name_to"],
                "is_excluded": row["is_excluded"],
                "referenced_relkind": row["referenced_relkind"],
                "referrer_relkind": row["referrer_relkind"],
                "referenced_partition_leaves": row["referenced_partition_leaves"],
                "referrer_partition_leaves": row["referrer_partition_leaves"],
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

    def _get_pg_dump_major(self) -> int:
        if self._pg_dump_major is None:
            self._pg_dump_major = int(get_major_version(get_pg_util_version(self.context.pg_dump)))
        return self._pg_dump_major

    async def _run_pg_dump(self, section: str) -> None:
        specific_tables: list[str] = []

        if self.context.black_listed_tables:
            black_list = [
                ("-T", f'"{table_schema}"."{table_name}"')
                for table_schema, table_name in self.context.black_listed_tables
            ]
            specific_tables.extend([item for sublist in black_list for item in sublist])

        if self.context.white_listed_tables:
            full_whitelist = set(self.context.white_listed_tables) | self._pg_dump_partitioned_ancestors
            white_list = [("-t", f'"{table_schema}"."{table_name}"') for table_schema, table_name in full_whitelist]
            specific_tables.extend([item for sublist in white_list for item in sublist])

            if self._sequences_data:
                seq_list = [
                    ("-t", f'"{seq_schema}"."{seq_name}"')
                    for table_schema, table_name, _, seq_schema, seq_name in self._sequences_data
                    if (table_schema, table_name) in self.context.white_listed_tables
                ]
                specific_tables.extend([item for sublist in seq_list for item in sublist])

        exclude_schemas = [
            item
            for v in self.context.exclude_schemas
            for item in ["--exclude-schema", '"' + v.replace('"', '""') + '"']
        ]

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

        normal_flags: list[str] = []
        last_flags: list[str] = []
        for flag, position, reason in _applicable_hardening_dump_flags(self._get_pg_dump_major()):
            self.context.logger.info("Passing %s to pg_dump (%s)", flag, reason)
            (last_flags if position is _DumpFlagPos.LAST else normal_flags).append(flag)

        command.extend(normal_flags)
        if self.context.options.pg_dump_options:
            command.extend(shlex.split(self.context.options.pg_dump_options))

        # LAST flags go after user --pg-dump-options so --no-statistics wins over a user --with-statistics.
        command.extend(last_flags)
        command.append(self.context.options.db_name)
        self.context.logger.debug(str(command))
        proc = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=build_pg_util_env(self.context.options),
        )
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

    async def _dump_data_into_file(self, db_conn: Connection, query: str, file_path: Path) -> str:
        try:
            if self.context.options.dbg_stage_1_validate_dict:
                return await db_conn.execute(query)

            with gzip.open(file_path, "wb", compresslevel=1) as output_file:
                return await db_conn.copy_from_query(
                    query=query,
                    output=output_file,
                    format="binary",
                )
        except Exception:
            self.context.logger.exception("Exception in _dump_data_into_file")
            raise

    async def _dump_data_by_query(
        self,
        pool: Pool,
        query: str,
        transaction_snapshot_id: str,
        file_name: str,
    ) -> dict[str, str]:
        output_file_path = self.output_dir / file_name

        task_id = uuid.uuid4()
        self.context.logger.info(
            "================> Task [%s] Started task %s to file %s",
            task_id,
            query,
            output_file_path,
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
                        file_path=output_file_path,
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
        return {result_hash: count_rows}

    def _resolve_table_rule(self, table_schema: str, table_name: str) -> dict | None:
        dictionary_rules = self.context.prepared_dictionary_obj["dictionary"]

        own_rule = get_dict_rule_for_table(
            dictionary_rules=dictionary_rules,
            schema=table_schema,
            table=table_name,
        )
        if own_rule is not None:
            return own_rule

        for ancestor_schema, ancestor_table in self._partition_ancestors_map.get((table_schema, table_name), []):
            ancestor_rule = get_dict_rule_for_table(
                dictionary_rules=dictionary_rules,
                schema=ancestor_schema,
                table=ancestor_table,
            )
            if ancestor_rule is not None:
                return ancestor_rule

        return None

    async def _prepare_dump_queries(self) -> None:
        self._data_dump_queries = []
        self._data_dump_files = {}

        fields_cache = await get_all_fields_list(
            connection_params=self.context.connection_params,
            exclude_schemas=self.context.exclude_schemas,
            server_settings=self.context.server_settings,
        )

        legacy_inherits_parents = await get_legacy_inheritance_parents(
            connection_params=self.context.connection_params,
            exclude_schemas=self.context.exclude_schemas,
            server_settings=self.context.server_settings,
        )

        for table_schema, table_name in self.context.tables:
            table_rule = self._resolve_table_rule(table_schema, table_name)

            query = await get_dump_query(
                ctx=self.context,
                table_schema=table_schema,
                table_name=table_name,
                table_rule=table_rule,
                files=self._data_dump_files,
                fields_cache=fields_cache,
                legacy_inherits_parents=legacy_inherits_parents,
            )

            if query:
                self.context.logger.info(str(query))
                self._data_dump_queries.append(query)

    @staticmethod
    def _collect_completed(done: set[asyncio.Task], pending: set[asyncio.Task], results: dict[str, str]) -> None:
        """Merge finished task results, re-raising the first failure and cancelling the rest."""
        for done_task in done:
            if exc := done_task.exception():
                for pending_task in pending:
                    pending_task.cancel()
                raise exc
            results.update(done_task.result())

    async def _run_dump_tasks(
        self,
        query_tasks: list[tuple[str, str]],
        transaction_snapshot_id: str,
    ) -> dict:
        pool = await create_pool(
            connection_params=self.context.connection_params,
            server_settings=self.context.server_settings,
            min_size=self.context.options.db_connections_per_process,
            max_size=self.context.options.db_connections_per_process,
        )

        results: dict[str, str] = {}
        dump_tasks: set[asyncio.Task] = set()

        status_ratio = 10
        if len(query_tasks) > 1000:  # noqa: PLR2004
            status_ratio = 100
        if len(query_tasks) > 50000:  # noqa: PLR2004
            status_ratio = 1000

        try:
            query_tasks_count = len(query_tasks)
            for idx, (file_name, query) in enumerate(query_tasks):
                while len(dump_tasks) >= self.context.options.db_connections_per_process:
                    done, dump_tasks = await asyncio.wait(dump_tasks, return_when=asyncio.FIRST_COMPLETED)
                    self._collect_completed(done, dump_tasks, results)

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
                self._collect_completed(done, dump_tasks, results)
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

                all_query_tasks = list(zip(self._data_dump_files.keys(), self._data_dump_queries, strict=False))

                dump_task = asyncio.create_task(
                    self._run_dump_tasks(
                        query_tasks=all_query_tasks,
                        transaction_snapshot_id=transaction_snapshot_id,
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
                self._count_totals()
                await self._prepare_sequences_last_values(connection=connection)
                await self._prepare_views(connection=connection)
                await self._prepare_indexes(connection=connection)
                await self._prepare_constraints(connection=connection)
                await self._prepare_extensions(connection=connection)
                await self._prepare_objects_ddl_to_metadata(connection)
        finally:
            await connection.close()
            self.context.logger.info("<------------- Finished dump data")

    async def _check_fdw_credentials_leak(self, connection: Connection) -> None:
        """Block the dump when FDW user-mapping credentials are visible; --allow-fdw-credentials downgrades to a warning."""
        visible_mappings = await get_visible_user_mappings(connection)
        if not visible_mappings:
            return

        if self.context.options.allow_fdw_credentials:
            self.context.logger.warning(
                "FDW credentials will be written to the dump: %d user mapping(s) with visible "
                "OPTIONS. This was allowed explicitly via --allow-fdw-credentials.",
                len(visible_mappings),
            )
            return

        msg = (
            f"Refusing to dump: {len(visible_mappings)} FDW user mapping(s) expose credentials "
            "(OPTIONS) visible to the current role, which pg_dump would leak into the dump. "
            "Either dump with a less-privileged role that cannot see the mapping OPTIONS, "
            "or pass --allow-fdw-credentials to include them intentionally."
        )
        self.context.logger.error(msg)
        raise PgAnonError(ErrorCode.CREDENTIALS_LEAK, msg)

    async def _warn_infra_leaks(self, connection: Connection) -> None:
        """Warn about leaks that can't be auto-sanitized without breaking structure (FDW servers, routine/trigger bodies)."""
        foreign_servers_count = await get_foreign_servers_count(connection)
        if foreign_servers_count:
            self.context.logger.warning(
                "FDW is in use: %d foreign server(s) will be dumped, exposing remote host/port (SERVER OPTIONS)",
                foreign_servers_count,
            )

        routines_and_triggers_count = await get_user_routines_and_triggers_count(
            connection, self.context.exclude_schemas
        )
        if routines_and_triggers_count:
            self.context.logger.warning(
                "%d user-defined function(s)/procedure(s)/trigger(s) will be dumped as-is; their "
                "bodies may embed secrets or personal data.",
                routines_and_triggers_count,
            )

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
        self._sequences_data = [tuple(row) for row in await connection.fetch(query)]

    async def _prepare_tables_lists(self, connection: Connection) -> None:
        tables = await get_db_tables(connection, self.context.exclude_schemas)
        self.context.set_tables_lists(tables)

        self._partition_ancestors_map = await get_partition_ancestors_map(connection, self.context.tables)

        if self.context.white_listed_tables:
            self._pg_dump_partitioned_ancestors = await get_partitioned_ancestors(
                connection, list(self.context.white_listed_tables)
            )
        else:
            self._pg_dump_partitioned_ancestors = set()

    async def _prepare_schemas_lists(self, connection: Connection) -> None:  # noqa: C901
        self._all_db_schemas = await get_schemas(connection)
        excluded_schemas = []

        protected_schemas: set[str] = set()
        for rule in self.context.prepared_dictionary_obj.get("dictionary", []):
            if schema := rule.get("schema"):
                protected_schemas.add(schema)
        for rule in self.context.prepared_dictionary_obj.get("validate_tables", []):
            if schema := rule.get("schema"):
                protected_schemas.add(schema)

        for rule in self.context.prepared_dictionary_obj.get("dictionary_exclude", []):
            table_mask = rule.get("table_mask")
            if table_mask != "*":
                continue

            schema_mask_pattern = None
            if schema_mask := rule.get("schema_mask"):
                schema_mask_pattern = safe_compile(schema_mask)

            for schema in self._all_db_schemas:
                if schema in protected_schemas:
                    continue
                if rule.get("schema") == schema:
                    excluded_schemas.append(schema)
                    break
                if schema_mask_pattern and schema_mask_pattern.search(schema):
                    excluded_schemas.append(schema)
                    continue

        self._schemas = list(set(self._all_db_schemas) - set(excluded_schemas))
        self.context.exclude_schemas.extend(excluded_schemas)

        if excluded_schemas:
            self.metadata.excluded_event_triggers = await get_event_triggers_in_schemas(connection, excluded_schemas)

    async def _prepare_objects_ddl_to_metadata(self, connection: Connection) -> None:
        if self.context.white_listed_tables or self.context.black_listed_tables:
            self.metadata.partial_dump_types = await get_custom_types_ddl(connection, self.context.exclude_schemas)
            self.metadata.partial_dump_domains = await get_custom_domains_ddl(connection, self.context.exclude_schemas)
            self.metadata.partial_dump_ranges = await get_custom_ranges_ddl(connection, self.context.exclude_schemas)
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

            if not self.context.options.disable_checks:
                # dump pool plus this connection, which holds the snapshot transaction
                await check_required_connections(connection, self.context.options.db_connections_per_process + 1)

            await self._check_fdw_credentials_leak(connection)

            self.context.read_prepared_dict()
            self.context.read_partial_tables_dicts()
            self._prepare_output_dir()

            await self._prepare_schemas_lists(connection)
            await self._prepare_tables_lists(connection)
            await self._fetch_sequences_data(connection)
            await self._warn_infra_leaks(connection)
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
