import asyncio
import gzip
import hashlib
import logging
import os
import re
import shlex
import shutil
import subprocess
import uuid
from datetime import datetime
from enum import Enum
from graphlib import CycleError, TopologicalSorter
from pathlib import Path
from typing import Any

from asyncpg import Connection, Pool

from pg_anon.common.db_queries import get_sequences_query
from pg_anon.common.db_utils import (
    check_required_connections,
    create_connection,
    create_pool,
    describe_db_objects,
    get_all_fields_list,
    get_all_tables,
    get_constraints_to_excluded_tables,
    get_db_size,
    get_db_tables,
    get_dependency_graph,
    get_dump_query,
    get_event_triggers,
    get_extension_schemas,
    get_extension_tables,
    get_extensions,
    get_foreign_descendants_map,
    get_foreign_servers_count,
    get_indexes_data,
    get_legacy_inheritance_parents,
    get_partition_ancestors_map,
    get_schemas,
    get_table_ancestors_map,
    get_user_routines_and_triggers_count,
    get_visible_user_mappings,
)
from pg_anon.common.dependencies import (
    dependency_error,
    DependencyGraph,
    describe_objects,
    EXCLUDABLE_RELATION_KINDS,
    Node,
    tables_of_owners,
)
from pg_anon.common.dto import Metadata
from pg_anon.common.enums import AnonMode
from pg_anon.common.errors import ErrorCode, PgAnonError
from pg_anon.common.utils import (
    build_pg_util_env,
    filter_db_tables,
    format_names_summary,
    get_dict_rule_for_table,
    get_major_version,
    get_pg_util_version,
    is_max_locks_error,
    log_rules_for_parent_tables,
    resolve_schemas,
    save_dicts_info_file,
    warn_rules_without_tables,
)
from pg_anon.context import Context
from pg_anon.version import __version__, DUMP_MIN_PG_ANON_VERSION

POINTER_SIZE = 8  # bytes per argv/envp pointer on 64-bit systems


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


def _dependency_order(dependencies: dict[str, set[str]], logger: logging.Logger) -> list[str]:
    """Sort extension names so that every extension follows the ones it depends on."""
    try:
        return list(TopologicalSorter(dependencies).static_order())
    except CycleError:
        # Not reachable through PostgreSQL, which rejects circular extension dependencies
        logger.warning("Circular dependency between extensions, falling back to alphabetical order")
        return sorted(dependencies)


class DumpMode:
    def __init__(self, context: Context) -> None:
        self.context: Context = context
        self.metadata: Metadata = Metadata()
        self.metadata_file_name: str = "metadata.json"
        self.structure_file_name: str = "structure.json"
        self.dumped_tables_file_name: str = "dumped_tables.py"

        self._data_dump_queries: list[str] = []
        self._data_dump_files: dict[str, dict[str, Any]] = {}
        self._data_dump_tasks_results: dict[str, int] = {}

        self._total_rows: int = 0

        self._schemas: list[str] = []
        self._sequences_data: list[tuple[str, ...]] = []
        self._sequences_last_values: dict | None = None
        self._indexes: dict | None = None
        self._constraints: dict | None = None
        self._extensions: dict | None = None

        self._all_db_schemas: list[str] = []
        self._graph: DependencyGraph | None = None
        self._dump_tables: set[Node] = set()
        self._excluded_relations: list[tuple[str, str]] = []
        self._extension_tables: set[tuple[str, str]] = set()
        self._partition_ancestors_map: dict[tuple[str, str], list[tuple[str, str]]] = {}
        self._table_ancestors: dict[tuple[str, str], list[tuple[str, str]]] = {}
        self._pg_dump_major: int | None = None

        if not self.context.options.output_dir:
            if not self.context.options.prepared_sens_dict_files:
                raise PgAnonError(ErrorCode.NO_DICT_FILES, "No prepared sens dict files specified")
            prepared_dict_name = Path(self.context.options.prepared_sens_dict_files[0]).stem
            self.output_dir: Path = Path.cwd() / prepared_dict_name
        else:
            self.output_dir = Path.cwd() / self.context.options.output_dir

        self.metadata_file_path = self.output_dir / self.metadata_file_name
        self.structure_file_path = self.output_dir / self.structure_file_name
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
                    }

    async def _prepare_indexes(self, connection: Connection) -> None:
        self._indexes = {}
        indexes_data = await get_indexes_data(connection, self.context.tables)
        for row in indexes_data:
            if row["is_excluded"]:
                continue
            self._indexes[row["index_name"]] = {
                "schema": row["schema"],
                "table": row["table"],
                "index_name": row["index_name"],
                "parent_index_schema": row["parent_index_schema"],
                "parent_index_name": row["parent_index_name"],
            }

    async def _prepare_constraints(self, connection: Connection) -> None:
        self._constraints = {}
        constraints_data = await get_constraints_to_excluded_tables(connection, self.context.tables)

        kept_tables = {self._graph.relation_name(node) for node in self._dump_tables} if self._graph else set()
        dumped_tables = set(self.context.tables)

        for row in constraints_data:
            tables = {
                (row["table_schema_from"], row["table_name_from"]),
                (row["table_schema_to"], row["table_name_to"]),
            }
            # a key between kept tables stays in metadata even when some of their partitions are excluded
            if row["is_excluded"] and not (row["contype"] == "f" and tables <= kept_tables):
                continue
            # a constraint name is unique only inside its table
            key = f"{row['table_schema_from']}.{row['table_name_from']}.{row['constraint_name']}"
            self._constraints[key] = {
                "table_schema_from": row["table_schema_from"],
                "table_name_from": row["table_name_from"],
                "constraint_name": row["constraint_name"],
                "contype": row["contype"],
                "definition": row["definition"],
                "table_schema_to": row["table_schema_to"],
                "table_name_to": row["table_name_to"],
                "referenced_relkind": row["referenced_relkind"],
                "referrer_relkind": row["referrer_relkind"],
                "referenced_partition_leaves": row["referenced_partition_leaves"],
                "referrer_partition_leaves": row["referrer_partition_leaves"],
            }
            # a structure dump does not say how the rows come, so it only writes the key down
            needs_mark = self.context.options.mode == AnonMode.DUMP
            if needs_mark and any(
                tuple(leaf) not in dumped_tables for leaf in row["referenced_partition_leaves"] or []
            ):
                # pg_restore does not create this key, the restore creates it itself
                self._constraints[key]["partition_excluded"] = True
            # names of excluded partitions must not get into the dump
            for leaves_key in ("referenced_partition_leaves", "referrer_partition_leaves"):
                if row[leaves_key]:
                    self._constraints[key][leaves_key] = [
                        leaf for leaf in row[leaves_key] if tuple(leaf) in dumped_tables
                    ]

    async def _prepare_extensions(self, connection: Connection) -> None:
        extensions = {}
        dependencies = {}

        for row in await get_extensions(connection):
            name = row["name"]
            extensions[name] = {
                "schema": row["schema"],
                "name": name,
                "version": row["version"],
                "relocatable": row["relocatable"],
                "is_excluded_by_schema": row["schema"] in self.context.exclude_schemas,
            }
            dependencies[name] = set(row["requires"] or [])

        # sorted for correct extension installation order
        self._extensions = {name: extensions[name] for name in _dependency_order(dependencies, self.context.logger)}

    async def _prepare_and_save_metadata(self) -> None:
        if self.context.options.dbg_stage_1_validate_dict:
            return

        self.metadata.pg_anon_version = __version__
        self.metadata.min_pg_anon_version = DUMP_MIN_PG_ANON_VERSION
        self.metadata.created = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        self.metadata.pg_version = self.context.pg_version
        self.metadata.pg_dump_version = get_pg_util_version(self.context.pg_dump)
        self.metadata.dbg_stage_2_validate_data = self.context.options.dbg_stage_2_validate_data
        self.metadata.dbg_stage_3_validate_full = self.context.options.dbg_stage_3_validate_full

        if self._need_dump_pre_and_post_sections:
            self.metadata.extensions = self._extensions
            self.metadata.schemas = sorted(self._schemas)
            self.metadata.constraints = self._constraints
            self.metadata.indexes = self._indexes

            self.metadata.save_structure_into_file(self.structure_file_path)

        if self._need_dump_data:
            self.metadata.prepared_sens_dict_files = ",".join(self.context.options.prepared_sens_dict_files or [])

            self.metadata.files = self._data_dump_files
            self.metadata.sequences_last_values = self._sequences_last_values
            self.metadata.total_rows = self._total_rows

            self.metadata.db_size = await get_db_size(
                connection_params=self.context.connection_params,
                server_settings=self.context.server_settings,
                db_name=self.context.options.db_name,
            )

            self.metadata.save_into_file(self.metadata_file_path)
            self.metadata.save_dumped_tables_into_file(self.dumped_tables_file_path)

    def _get_pg_dump_major(self) -> int:
        if self._pg_dump_major is None:
            self._pg_dump_major = int(get_major_version(get_pg_util_version(self.context.pg_dump)))
        return self._pg_dump_major

    def _check_command_size(self, command: list[str], env: dict[str, str]) -> None:
        """Stop before pg_dump when its command line and environment do not fit into ARG_MAX."""
        entries = [*command, *(f"{key}={value}" for key, value in env.items())]
        size = sum(len(entry.encode("utf-8")) + 1 + POINTER_SIZE for entry in entries)
        limit = os.sysconf("SC_ARG_MAX")
        if size <= limit:
            return
        msg = (
            f"ERROR: the pg_dump command is too long to start: {len(self._excluded_relations)} excluded tables "
            f"make it {size} bytes, but the system limit is {limit} bytes. "
            "Exclude whole schemas instead of single tables, or raise the limit with `ulimit -s`."
        )
        self.context.logger.error(msg)
        raise PgAnonError(ErrorCode.TOO_MANY_EXCLUDED_TABLES, msg)

    async def _run_pg_dump(self, section: str) -> None:
        excluded_relations = [
            item for schema, name in self._excluded_relations for item in ("-T", f'"{schema}"."{name}"')
        ]

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
            *excluded_relations,
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
        env = build_pg_util_env(self.context.options)
        if section == "pre-data":
            self._check_command_size(command, env)
        proc = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
        )
        # pg_dump put command result into stdout if not using "-f" option, else stdout is empty
        # pg_dump put logs into stderr
        _, pg_dump_logs_bytes = proc.communicate()
        pg_dump_logs = pg_dump_logs_bytes.decode("utf-8", errors="replace")

        for log_line in pg_dump_logs.split("\n"):
            self.context.logger.info(log_line)

        if proc.returncode != 0:
            if is_max_locks_error(pg_dump_logs):
                msg = (
                    f"ERROR: pg_dump ran out of locks: it locks every table of the dump, "
                    f"and this dump has {len(self._dump_tables)} tables. "
                    "Increase max_locks_per_transaction on the server."
                )
                self.context.logger.error(msg)
                raise PgAnonError(ErrorCode.MAX_LOCKS_EXCEEDED, msg)
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

        for file in self._data_dump_files.values():
            # the restore applies its lists to parent tables by this key, also without structure.json
            ancestors = self._table_ancestors.get((file["schema"], file["table"]), [])
            file["ancestors"] = [list(ancestor) for ancestor in ancestors]
            if (file["schema"], file["table"]) in self._extension_tables:
                # CREATE EXTENSION makes this table, so the restore must not look for it in the dump structure
                file["extension_table"] = True

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

    async def _warn_foreign_parents(self, connection: Connection) -> None:
        """Warn about the dumped tables with foreign child tables: without user mappings they fail on the target."""
        if self._skip_pre_data_dump or not self._graph:
            return

        kept_tables = {self._graph.relation_name(node) for node in self._dump_tables}
        foreign_parents = [
            f"{schema}.{table}"
            for (schema, table), foreign_tables in (await get_foreign_descendants_map(connection)).items()
            if (schema, table) in kept_tables and foreign_tables & kept_tables
        ]
        if foreign_parents:
            self.context.logger.warning(
                "Foreign child tables found under %s. In the target database, queries to these tables without ONLY "
                "go to the foreign tables and fail without a user mapping. To avoid this, exclude the foreign tables "
                "with an exclude list, restore with --keep-fdw-user-mappings, or create the user mappings by hand.",
                format_names_summary(foreign_parents, "table"),
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

    async def _exclude_extension_tables(
        self, connection: Connection, tables: list[tuple[str, str]]
    ) -> list[tuple[str, str]]:
        """Drop tables owned by extensions, keeping the configuration ones as pg_dump does."""
        conditions: dict[tuple[str, str], str] = {}
        internal_tables: set[tuple[str, str]] = set()

        extension_tables = await get_extension_tables(connection)
        for table, condition in extension_tables.items():
            if condition is None:
                internal_tables.add(table)
            elif condition:
                conditions[table] = condition

        self.context.extension_table_conditions = conditions
        kept_tables = [table for table in tables if table not in internal_tables]
        self._extension_tables = {table for table in kept_tables if table in extension_tables}

        skipped_count = len(tables) - len(kept_tables)
        if skipped_count:
            self.context.logger.info("Skipping data of %d table(s) owned by extensions", skipped_count)

        return kept_tables

    async def _fetch_sequences_data(self, connection: Connection) -> None:
        """Fetch sequences data and cache for reuse in pg_dump and metadata."""
        query = get_sequences_query(self.context.exclude_schemas)
        self._sequences_data = [tuple(row) for row in await connection.fetch(query)]

    async def _prepare_tables_lists(self, connection: Connection) -> None:
        tables = await get_db_tables(connection, self.context.exclude_schemas)
        tables = await self._exclude_extension_tables(connection, tables)
        self._partition_ancestors_map = await get_partition_ancestors_map(connection, tables)
        # the lists follow INHERITS parents too, the masking rules follow only partitioned tables
        self._table_ancestors = await get_table_ancestors_map(connection)
        self.context.set_tables_lists(tables, self._table_ancestors)
        if self._skip_pre_data_dump and self._skip_post_data_dump:
            # no structure is dumped, so _prepare_exclusions does not check the lists
            all_tables = await get_all_tables(connection)
            warn_rules_without_tables(self.context.excluded_tables_rules, all_tables, "Exclude list", "the database")
            warn_rules_without_tables(self.context.included_tables_rules, all_tables, "Include list", "the database")

    async def _prepare_exclusions(self, connection: Connection) -> None:
        """Find the tables to exclude and what depends on them; stop when a kept table depends on them."""
        if self._skip_pre_data_dump and self._skip_post_data_dump:
            return

        self._graph = await get_dependency_graph(connection)
        kept_tables, excluded_tables = self._split_tables()
        self._dump_tables = self._graph.table_nodes(kept_tables)

        dependents = set()
        for roots, error_code in (
            (self._graph.schema_nodes(self.context.exclude_schemas), ErrorCode.EXCLUDED_SCHEMA_DEPENDENCY),
            (self._graph.table_nodes(excluded_tables), ErrorCode.EXCLUDED_TABLE_DEPENDENCY),
        ):
            owners, kept_dependents = self._graph.walk(roots, self._dump_tables)
            if kept_dependents:
                raise dependency_error(
                    error_code, [".".join(self._graph.relation_name(table)) for table in kept_dependents]
                )
            dependents |= owners

        dependent_relations = {
            self._graph.relation_name(node)
            for node in dependents
            if self._graph.relation_kind(node) in EXCLUDABLE_RELATION_KINDS
        }
        self._excluded_relations = sorted(
            set(excluded_tables)
            | {relation for relation in dependent_relations if relation[0] not in self.context.exclude_schemas}
        )
        skipped_owners = {
            node for node in dependents if self._graph.relation_kind(node) not in EXCLUDABLE_RELATION_KINDS
        }
        # the restore finds these objects in the TOC of the dump by their OIDs
        self.metadata.dependencies, self.metadata.dump_dependents_to_skip = describe_objects(
            self._graph, tables_of_owners(self._graph, self._dump_tables), skipped_owners
        )

        await self._log_lost_dependents(connection, dependents, skipped_owners, excluded_tables)
        if self.context.included_tables_rules or self.context.excluded_tables_rules or self.context.exclude_schemas:
            self.context.logger.info("Dump includes %s of %s tables", len(self._dump_tables), len(self._graph.tables()))

    async def _log_lost_dependents(
        self,
        connection: Connection,
        dependents: set[Node],
        skipped_owners: set[Node],
        excluded_tables: list[tuple[str, str]],
    ) -> None:
        """Name the objects that depend on excluded tables or schemas; their own indexes, keys and the like are left out."""
        graph = self._graph
        if graph is None:
            return
        schema_nodes = graph.schema_nodes(self.context.exclude_schemas)
        in_schemas = schema_nodes.union(*(graph.dependents.get(schema, set()) for schema in schema_nodes))
        relations = {node for node in dependents if graph.relation_kind(node) in EXCLUDABLE_RELATION_KINDS}
        dropped = in_schemas | relations | graph.table_nodes(excluded_tables)

        lost = {node for node in skipped_owners if not graph.is_part_of(node, dropped)}
        # views go to the exclusion list of pg_dump, but they are lost all the same
        lost |= {
            node
            for node in relations
            if graph.relation_kind(node) in ("v", "m") and not graph.is_part_of(node, in_schemas)
        }
        if not lost:
            return

        excluded = "excluded tables and schemas" if self.context.exclude_schemas else "excluded tables"
        names = await describe_db_objects(connection, sorted(lost))
        self.context.logger.info(
            "Objects that depend on %s and will not be restored: %s",
            excluded,
            format_names_summary(names, "object", limit=5),
        )

    def _split_tables(self) -> tuple[set[tuple[str, str]], list[tuple[str, str]]]:
        """Split the tables into kept and excluded ones; a list entry for a parent table covers its child tables."""
        tables = {table for table in self._graph.tables().values() if table[0] not in self.context.exclude_schemas}
        partitioned = {(schema, name) for kind, schema, name in self._graph.relations.values() if kind == "p"}
        ancestors = {table: self._table_ancestors[table] for table in tables if table in self._table_ancestors}
        white_rules, black_rules = self.context.included_tables_rules, self.context.excluded_tables_rules
        kept, black_listed, white_listed = filter_db_tables(sorted(tables), white_rules, black_rules, ancestors)
        kept_tables = set(kept)
        # a partition or a child table cannot be created without its parents
        parents = {parent for table in white_listed for parent in ancestors.get(table, [])}
        kept_tables |= parents & tables

        all_tables = set(self._graph.tables().values())
        warn_rules_without_tables(black_rules, all_tables, "Exclude list", "the database")
        warn_rules_without_tables(white_rules, all_tables, "Include list", "the database")
        log_rules_for_parent_tables(black_rules, tables, ancestors, "excluded")
        log_rules_for_parent_tables(white_rules, tables - black_listed, ancestors, "included")

        # the restore applies its lists to parent tables by these two descriptions
        self.metadata.partitions = []
        self.metadata.inherits = []
        for table in sorted(kept_tables):
            table_ancestors = [list(ancestor) for ancestor in ancestors.get(table, [])]
            if table in partitioned or (table_ancestors and tuple(table_ancestors[0]) in partitioned):
                self.metadata.partitions.append(
                    {
                        "schema": table[0],
                        "table": table[1],
                        "is_partitioned": table in partitioned,
                        "ancestors": table_ancestors,
                    }
                )
            elif table_ancestors:
                self.metadata.inherits.append({"schema": table[0], "table": table[1], "ancestors": table_ancestors})

        return kept_tables, sorted(tables - kept_tables)

    async def _prepare_schemas_lists(self, connection: Connection) -> None:
        # CREATE EXTENSION makes these schemas itself; their tables follow the extension rules, like in pg_dump
        extension_schemas = await get_extension_schemas(connection)
        self._all_db_schemas = [schema for schema in await get_schemas(connection) if schema not in extension_schemas]
        options = self.context.options
        schema_filters = (
            options.schema_names,
            options.schema_masks,
            options.exclude_schema_names,
            options.exclude_schema_masks,
        )

        self._schemas = resolve_schemas(self._all_db_schemas, *schema_filters)
        self.context.exclude_schemas = [schema for schema in self._all_db_schemas if schema not in self._schemas]

        if any(schema_filters):
            self.context.logger.info("Dump includes %s of %s schemas", len(self._schemas), len(self._all_db_schemas))

        # an event trigger has no schema, so pg_dump keeps it and the restore drops it by the schema of its function
        self.metadata.event_triggers = {
            name: {
                "trigger_name": name,
                "function_schema": schema,
                "is_excluded": schema in self.context.exclude_schemas,
            }
            for name, schema in (await get_event_triggers(connection)).items()
        }
        if self.context.exclude_schemas:
            # older pg_anon versions read only this key
            self.metadata.excluded_event_triggers = [
                name for name, trigger in self.metadata.event_triggers.items() if trigger["is_excluded"]
            ]

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
            await self._prepare_exclusions(connection)
            await self._fetch_sequences_data(connection)
            await self._warn_infra_leaks(connection)
            await self._warn_foreign_parents(connection)
            await self._dump_pre_data()
            await self._dump_post_data()
            if self._need_dump_pre_and_post_sections:
                await self._prepare_constraints(connection=connection)
                await self._prepare_indexes(connection=connection)
                await self._prepare_extensions(connection=connection)
            await self._dump_data(connection)
            await self._prepare_and_save_metadata()

            self.context.logger.info("<------------- Finished dump")
        finally:
            if connection:
                await connection.close()

            if self.context.options.save_dicts:
                save_dicts_info_file(self.context.options)
