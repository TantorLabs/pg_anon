import asyncio
import gzip
import json
import os
import shlex
import re
import shutil
import subprocess
from copy import copy
from pathlib import Path
from typing import Optional, List

import asyncpg
from asyncpg import Connection

from pg_anon.common.db_queries import get_check_constraint_query, get_sequences_max_value_init_query, get_db_params
from pg_anon.common.db_utils import create_connection, create_pool, check_db_is_empty, run_query_in_pool, \
    get_available_extensions_map, check_required_connections
from pg_anon.common.dto import Metadata
from pg_anon.common.enums import AnonMode
from pg_anon.common.errors import PgAnonError, ErrorCode
from pg_anon.common.utils import (
    exception_helper,
    get_major_version,
    get_pg_util_version,
    pretty_size, save_dicts_info_file, resolve_dependencies
)
from pg_anon.context import Context


class RestoreMode:
    context: Context
    input_dir: Path

    metadata: Metadata
    metadata_file_name: str = 'metadata.json'
    metadata_file_path: Path

    _toc_list_pre_data_file_name: str = 'toc_pre_data_filtered.list'
    _toc_list_pre_data_file_path: Optional[Path] = None
    _toc_list_post_data_file_name: str = 'toc_post_data_filtered.list'
    _toc_list_post_data_file_path: Optional[Path] = None

    _db_must_be_empty: bool
    _skip_pre_data_restore: bool = False
    _skip_post_data_restore: bool = False

    _restored_schemas: List[str]

    def __init__(self, context: Context):
        self.context = context
        self.input_dir = Path.cwd() / self.context.options.input_dir

        if not self.input_dir.exists():
            msg = f"ERROR: input directory {self.input_dir} does not exists"
            self.context.logger.error(msg)
            raise PgAnonError(ErrorCode.INPUT_DIR_NOT_FOUND, msg)

        self._load_metadata()

        self._db_must_be_empty = (
                self.context.options.mode in (AnonMode.RESTORE, AnonMode.SYNC_STRUCT_RESTORE)
                and not (self.context.options.clean_db or self.context.options.drop_db)
        )
        self._skip_pre_data_restore = (
                self.context.options.mode == AnonMode.SYNC_DATA_RESTORE
                or self.metadata.dbg_stage_2_validate_data
        )
        self._skip_post_data_restore = (
                self.context.options.mode == AnonMode.SYNC_DATA_RESTORE
                or self.metadata.dbg_stage_2_validate_data
                or self.metadata.dbg_stage_3_validate_full
        )

    def _load_metadata(self):
        self.metadata_file_path = self.input_dir / self.metadata_file_name
        self.metadata = Metadata()
        self.metadata.load_from_file(self.metadata_file_path)

    def _generate_analyze_queries(self):
        analyze_queries = []
        for file_name, target in self.metadata.files.items():
            schema = target["schema"]
            table = target["table"]
            if self.context.black_listed_tables and (schema, table) in self.context.black_listed_tables:
                continue

            if self.context.white_listed_tables and (schema, table) not in self.context.white_listed_tables:
                continue

            analyze_queries.append(
                f'analyze "{schema}"."{table}"'
            )
        return analyze_queries

    def _check_utils_version_for_dump(self):
        if self.context.options.disable_checks:
            return

        target_postgres_version = get_major_version(self.context.pg_version)
        source_postgres_version = get_major_version(self.metadata.pg_version)

        target_pg_restore_version = get_major_version(get_pg_util_version(self.context.pg_restore))
        source_pg_dump_version = get_major_version(self.metadata.pg_dump_version)

        if target_postgres_version < source_postgres_version:
            raise PgAnonError(
                ErrorCode.VERSION_INCOMPATIBLE,
                f"Target PostgreSQL major version {target_postgres_version} is below than source {source_postgres_version}!"
            )

        if target_pg_restore_version < source_pg_dump_version:
            raise PgAnonError(
                ErrorCode.VERSION_INCOMPATIBLE,
                f"pg_restore major version {target_pg_restore_version} is below than source pg_dump version {source_pg_dump_version}!"
            )

    async def _check_db_is_empty(self, connection: Connection):
        if not self._db_must_be_empty:
            return

        if not await check_db_is_empty(connection=connection):
            raise PgAnonError(ErrorCode.DB_NOT_EMPTY, f"Target DB {self.context.connection_params.database} is not empty!")

    async def _check_free_disk_space(self, connection: Connection):
        data_directory_location = await connection.fetchval(
            """
            SELECT setting
            FROM pg_settings
            WHERE name = 'data_directory'
            """
        )
        disk_size = shutil.disk_usage(data_directory_location)
        free_disk_space = pretty_size(disk_size.free)
        required_disk_space = pretty_size(int(self.metadata.total_tables_size) * 1.5)

        self.context.logger.info(f"Free disk space: {free_disk_space}")
        self.context.logger.info(f"Required disk space: {required_disk_space}")

        if disk_size.free < int(self.metadata.total_tables_size) * 1.5:
            raise PgAnonError(
                ErrorCode.INSUFFICIENT_DISK_SPACE,
                f"Not enough freed disk space! Free {free_disk_space}, Required {required_disk_space}"
            )

    def _make_filtered_toc_list(self):
        whitelist = []
        blacklist = []

        for schema, table in self.context.black_listed_tables:
            # Make blacklist for TABLE, DEFAULT, TRIGGER, RULE
            blacklist.append(re.compile(fr".*(TABLE|DEFAULT|TRIGGER|RULE) {re.escape(schema)} {re.escape(table)}"))
            # Update blacklist for CONSTRAINT
            blacklist.append(re.compile(fr"(?<!FK )CONSTRAINT {re.escape(schema)} {re.escape(table)}"))

        for schema, table in self.context.white_listed_tables:
            # Make whitelist for TABLE, DEFAULT, CONSTRAINT, FK CONSTRAINT, TRIGGER, RULE
            whitelist.append(re.compile(fr".*(TABLE|DEFAULT|TRIGGER|RULE) {re.escape(schema)} {re.escape(table)}"))
            # Update whitelist for CONSTRAINT
            whitelist.append(re.compile(fr"(?<!FK )CONSTRAINT {re.escape(schema)} {re.escape(table)}"))

        if self.metadata.sequences_last_values:
            for seq in self.metadata.sequences_last_values.values():
                # Update blacklist for SEQUENCE, SEQUENCE OWNED BY
                if seq['is_excluded'] or (seq['schema'], seq['table']) in self.context.black_listed_tables:
                    blacklist.append(
                        re.compile(fr".*SEQUENCE.*{re.escape(seq['schema'])} {re.escape(seq['seq_name'])}")
                    )

                # Update whitelist for SEQUENCE, SEQUENCE OWNED BY
                if not seq['is_excluded'] and (seq['schema'], seq['table']) in self.context.white_listed_tables:
                    whitelist.append(
                        re.compile(fr".*SEQUENCE.*{re.escape(seq['schema'])} {re.escape(seq['seq_name'])}")
                    )

        if self.metadata.indexes:
            for index in self.metadata.indexes.values():
                # Update blacklist for INDEX
                if index['is_excluded'] or (index['schema'], index['table']) in self.context.black_listed_tables:
                    blacklist.append(
                        re.compile(fr".*INDEX {re.escape(index['schema'])} {re.escape(index['index_name'])}")
                    )

                # Update blacklist for INDEX
                if not index['is_excluded'] and (index['schema'], index['table']) in self.context.white_listed_tables:
                    whitelist.append(
                        re.compile(fr".*INDEX {re.escape(index['schema'])} {re.escape(index['index_name'])}")
                    )

        if self.metadata.views:
            for view in self.metadata.views.values():
                # Update blacklist for VIEW, MATERIALIZED VIEW
                if view['is_excluded'] or (view['table_schema'], view['table_name']) in self.context.black_listed_tables:
                    blacklist.append(
                        re.compile(fr".*VIEW {re.escape(view['view_schema'])} {re.escape(view['view_name'])}")
                    )

                # Update blacklist for VIEW, MATERIALIZED VIEW
                if not view['is_excluded'] and (view['table_schema'], view['table_name']) in self.context.white_listed_tables:
                    whitelist.append(
                        re.compile(fr".*VIEW {re.escape(view['view_schema'])} {re.escape(view['view_name'])}")
                    )

        if self.metadata.constraints:
            for constraint in self.metadata.constraints.values():
                constraint_table_to = (constraint['table_schema_to'], constraint['table_name_to'])
                constraint_table_from = (constraint['table_schema_from'], constraint['table_name_from'])
                one_of_constraint_tables_in_black_list = constraint_table_to in self.context.black_listed_tables or constraint_table_from in self.context.black_listed_tables
                if constraint['is_excluded'] or one_of_constraint_tables_in_black_list:
                    # Update blacklist for FK CONSTRAINT
                    blacklist.extend([
                        re.compile(fr".*FK CONSTRAINT {re.escape(constraint['table_schema_from'])} {re.escape(constraint['table_name_from'])} {re.escape(constraint['constraint_name'])}")
                    ])

                constraint_tables_both_in_white_list = constraint_table_to in self.context.white_listed_tables and constraint_table_from in self.context.white_listed_tables
                if not constraint['is_excluded'] and constraint_tables_both_in_white_list:
                    # Update blacklist for FK CONSTRAINT
                    whitelist.extend([
                        re.compile(fr".*FK CONSTRAINT {re.escape(constraint['table_schema_from'])} {re.escape(constraint['table_name_from'])} {re.escape(constraint['constraint_name'])}")
                    ])

        for section in ["pre_data", "post_data"]:
            command = [self.context.pg_restore, "-l", str(self.input_dir / f"{section}.backup")]
            if section == "pre_data":
                self._toc_list_pre_data_file_path = self.input_dir / self._toc_list_pre_data_file_name
                toc_file_path = self._toc_list_pre_data_file_path
            else:
                self._toc_list_post_data_file_path = self.input_dir / self._toc_list_post_data_file_name
                toc_file_path = self._toc_list_post_data_file_path

            proc = subprocess.Popen(command, stdout=subprocess.PIPE)
            toc_bytes, _ = proc.communicate()
            toc_lines = toc_bytes.decode('utf-8', errors='replace')
            with open(toc_file_path, "w", encoding="utf-8") as f:
                for toc_line in toc_lines.split("\n"):
                    if toc_line.startswith(';'):
                        continue

                    if blacklist and any(p.search(toc_line) for p in blacklist):
                        self.context.logger.debug(f'PARTIAL RESTORE MODE. TOC: Skip by blacklist - "{toc_line}" ')
                        continue

                    if whitelist and not any(p.search(toc_line) for p in whitelist):
                        self.context.logger.debug(f'PARTIAL RESTORE MODE. TOC: Skip by whitelist - "{toc_line}" ')
                        continue

                    f.write(f'{toc_line}\n')

    def _remove_toc_lists(self):
        if self.context.options.debug:
            return

        self._toc_list_pre_data_file_path.unlink(missing_ok=True)
        self._toc_list_post_data_file_path.unlink(missing_ok=True)

    async def _run_pg_restore(self, section: str):
        os.environ["PGPASSWORD"] = self.context.options.db_user_password

        command = [
            self.context.pg_restore,
            "-h",
            self.context.options.db_host,
            "-p",
            str(self.context.options.db_port),
            "-v",
            "-w",
            "-U",
            self.context.options.db_user,
            "-d",
            self.context.options.db_name,
            "-j",
            str(self.context.options.db_connections_per_process),
            "--no-owner",
            str((self.input_dir / section.replace("-", "_")).with_suffix(".backup")),
        ]
        if not self.context.options.db_host:
            del command[command.index("-h"): command.index("-h") + 2]

        if not self.context.options.db_user:
            del command[command.index("-U"): command.index("-U") + 2]

        if self.context.options.clean_db:
            command.extend([
                "--clean",
                "--if-exists",
            ])

        if self.context.options.ignore_privileges:
            command.append("--no-privileges")

        if self._toc_list_pre_data_file_path:
            command.extend([
                '-L',
                self._toc_list_pre_data_file_path if section == 'pre-data' else self._toc_list_post_data_file_path
            ])

        if self.context.options.pg_restore_options:
            command.extend(shlex.split(self.context.options.pg_restore_options))

        self.context.logger.debug(str(command))
        proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        # pg_restore put command result into stdout if not using "-f" option, else stdout is empty
        # pg_restore put logs into stderr
        _, pg_restore_logs_bytes = proc.communicate()
        pg_restore_logs = pg_restore_logs_bytes.decode('utf-8', errors='replace')

        for log_line in pg_restore_logs.split("\n"):
            self.context.logger.info(log_line)

        if proc.returncode != 0:
            msg = "ERROR: database restore has failed!"
            self.context.logger.error(msg)
            raise PgAnonError(ErrorCode.RESTORE_FAILED, msg)

    async def _sequences_init(self, connection: Connection):
        if self.context.options.mode == AnonMode.SYNC_STRUCT_RESTORE:
            return

        if self.context.options.seq_init_by_max_value:
            query = get_sequences_max_value_init_query()
            self.context.logger.debug(query)
            await connection.execute(query)
        else:
            for sequence_data in self.metadata.sequences_last_values.values():
                if sequence_data['is_excluded']:
                    continue

                sequence_relation = (sequence_data['schema'], sequence_data['table'])

                if self.context.black_listed_tables and sequence_relation in self.context.black_listed_tables:
                    continue

                if self.context.white_listed_tables and sequence_relation not in self.context.white_listed_tables:
                    continue

                schema = sequence_data["schema"].replace("'", "''")
                sequence_name = sequence_data["seq_name"].replace("'", "''")
                value = sequence_data["value"]
                query = f"""
                    SET search_path = '{schema}';
                    SELECT setval(quote_ident('{sequence_name}'), {value} + 1);
                """
                self.context.logger.info(query)
                await connection.execute(query)

    async def _create_schemas_for_partial_mode(self, connection: Connection):
        self._restored_schemas = list()
        if self.context.black_listed_tables or self.context.white_listed_tables:
            self._restored_schemas = list({schema for schema, _ in self.context.tables})

        if self.metadata.partial_dump_schemas:
            self._restored_schemas = copy(self.metadata.partial_dump_schemas)

        if not self._restored_schemas:
            return

        for schema in self._restored_schemas:
            query = f'CREATE SCHEMA IF NOT EXISTS "{schema}"'
            self.context.logger.info("PARTIAL RESTORE MODE: " + query)
            await connection.execute(query)

    async def _create_extensions_for_partial_mode(self, connection: Connection):
        # Manual extension creating only in case of whitelist using
        if not self.metadata.extensions or not self._restored_schemas:
            return

        available_extensions = await get_available_extensions_map(connection)
        available_schemas = ['pg_catalog', *self._restored_schemas]
        for extension_name, extension_data in self.metadata.extensions.items():
            query_parts = [f'CREATE EXTENSION IF NOT EXISTS {extension_name}']

            # Check necessary schemas exists or extension can be relocatable
            if extension_data['schema'] not in available_schemas or extension_data['is_excluded_by_schema']:
                if not extension_data['relocatable']:
                    raise PgAnonError(ErrorCode.EXTENSION_ERROR, f'Can not restore EXTENSION "{extension_name}", cause SCHEMA "{extension_data["schema"]}" is not exists')
                self.context.logger.warn(f'EXTENSION "{extension_name}" will restored into default schema, cause SCHEMA "{extension_data["schema"]}" is not exists')
            else:
                query_parts.append(f'SCHEMA {extension_data["schema"]}')

            # Check extension exists in system
            available_extension_versions = available_extensions.get(extension_name)
            if not available_extension_versions:
                raise PgAnonError(ErrorCode.EXTENSION_ERROR, f'Required EXTENSION "{extension_name}" is not available for creating')

            extension_already_installed = False
            version_specified = None
            for available_extension_version in available_extension_versions:
                if available_extension_version['installed']:
                    extension_already_installed = True
                    break

                if available_extension_version['version'] == extension_data['version']:
                    version_specified = available_extension_version
                    break

            if extension_already_installed:
                continue

            if not version_specified:
                version_specified = available_extension_versions[0]
                self.context.logger.warn(
                    f'EXTENSION "{extension_name}" will restored by default version "{version_specified["default_version"]}", cause target version "{extension_data["version"]}" is not exists'
                )

            query_parts.append(f"VERSION '{version_specified['default_version']}'")

            queries = []
            if version_specified['requires']:
                for dependencies_extension in version_specified['requires']:
                    queries.extend([
                        f'CREATE EXTENSION IF NOT EXISTS {extension}'
                        for extension in resolve_dependencies(dependencies_extension, available_extensions)
                    ])

            queries.append(' '.join(query_parts))
            for extension_dependency_query in queries:
                self.context.logger.info("PARTIAL RESTORE MODE: " + extension_dependency_query)
                await connection.execute(extension_dependency_query)

    async def _create_objects_from_ddl_for_partial_mode(self, connection: Connection):
        ddl_list = []

        if self.metadata.partial_dump_types:
            ddl_list.extend(self.metadata.partial_dump_types)

        if self.metadata.partial_dump_domains:
            ddl_list.extend(self.metadata.partial_dump_domains)

        if self.metadata.partial_dump_functions:
            ddl_list.extend(self.metadata.partial_dump_functions)

        if self.metadata.partial_dump_casts:
            ddl_list.extend(self.metadata.partial_dump_casts)

        if self.metadata.partial_dump_operators:
            ddl_list.extend(self.metadata.partial_dump_operators)

        if self.metadata.partial_dump_aggregates:
            ddl_list.extend(self.metadata.partial_dump_aggregates)

        remaining = list(ddl_list)
        while remaining:
            failed = []
            for query in remaining:
                try:
                    self.context.logger.info("PARTIAL RESTORE MODE: " + query)
                    await connection.execute(query)
                except Exception as ex:
                    self.context.logger.warning(
                        f"PARTIAL RESTORE MODE: DDL failed, will retry: {ex}"
                    )
                    failed.append((query, ex))

            if not failed:
                break

            if len(failed) == len(remaining):
                error_details = "; ".join(f"{ex}" for _, ex in failed)
                raise PgAnonError(
                    ErrorCode.RESTORE_FAILED,
                    f"Failed to execute {len(failed)} DDL statement(s): {error_details}"
                )

            remaining = [query for query, _ in failed]
            self.context.logger.info(
                f"PARTIAL RESTORE MODE: Retrying {len(remaining)} failed DDL(s)"
            )

    async def _drop_constraints(self, connection: Connection):
        """
        Drop all CHECK constrains containing user-defined procedures to avoid
        performance degradation at the data loading stage
        :param connection: Active database connection
        """
        if not self.context.options.drop_custom_check_constr:
            return

        check_constraints = await connection.fetch(
            get_check_constraint_query()
        )

        if not check_constraints:
            return

        for check_constraint in check_constraints:
            schema = check_constraint[0]
            table = check_constraint[1]
            constraint = check_constraint[2]

            self.context.logger.info(f"Removing constraints: {schema}.{table} -> {constraint}")
            query = f'ALTER TABLE "{schema}"."{table}" DROP CONSTRAINT IF EXISTS "{constraint}" CASCADE'
            await connection.execute(query)

    async def _restore_table_data(
            self,
            pool: asyncpg.Pool,
            dump_file: Path,
            schema_name: str,
            table_name: str,
            transaction_snapshot_id: str,
    ):
        self.context.logger.info(f"{'>':=>20} Started task copy_to_table {schema_name}.{table_name}")
        extracted_file = Path(dump_file.stem)

        with gzip.open(dump_file, "rb") as src_file, open(extracted_file, "wb") as trg_file:
            trg_file.writelines(src_file)

        try:
            async with pool.acquire() as connection:
                async with connection.transaction(isolation='repeatable_read'):
                    await connection.execute(f"SET TRANSACTION SNAPSHOT '{transaction_snapshot_id}';")

                    result = await connection.copy_to_table(
                        schema_name=schema_name,
                        table_name=table_name,
                        source=extracted_file,
                        format="binary",
                    )
                    self.context.total_rows += int(re.findall(r"(\d+)", result)[0])
                    await connection.execute("COMMIT;")
        except Exception as exc:
            self.context.logger.error(
                f"Exception in RestoreMode._restore_table_data:"
                f" {schema_name=}"
                f" {table_name=}"
                f" {extracted_file=}"
                f"\n{exc=}"
            )
        finally:
            extracted_file.unlink()

        self.context.logger.info(f"{'>':=>20} Finished task {schema_name}.{str(table_name)}")

    async def _process_restore_data(self, transaction_snapshot_id: str):
        pool = await create_pool(
            connection_params=self.context.connection_params,
            server_settings=self.context.server_settings,
            min_size=self.context.options.db_connections_per_process,
            max_size=self.context.options.db_connections_per_process
        )

        try:
            loop = asyncio.get_event_loop()
            tasks = set()
            for file_name, target in self.metadata.files.items():
                table_name_full = f'"{target["schema"]}"."{target["table"]}"'

                # black list has the highest priority for pg_dump / pg_restore
                if self.context.black_listed_tables and (target["schema"], target["table"]) in self.context.black_listed_tables:
                    self.context.logger.info("Skipping restore data of table: " + str(table_name_full))
                    continue

                # white list has the second priority for pg_dump / pg_restore
                if self.context.white_listed_tables and (target["schema"], target["table"]) not in self.context.white_listed_tables:
                    self.context.logger.info("Skipping restore data of table: " + str(table_name_full))
                    continue

                full_path = self.input_dir / file_name
                if len(tasks) >= self.context.options.db_connections_per_process:
                    # Wait for some restore to finish before adding a new one
                    done, tasks = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                    exception = done.pop().exception()
                    if exception is not None:
                        await pool.close()
                        raise exception
                tasks.add(
                    loop.create_task(
                        self._restore_table_data(
                            pool=pool,
                            dump_file=full_path,
                            schema_name=target["schema"],
                            table_name=target["table"],
                            transaction_snapshot_id=transaction_snapshot_id,
                        )
                    )
                )

            # Wait for the remaining restores to finish
            await asyncio.wait(tasks)
        finally:
            await pool.close()

    async def _restore_data(self, connection: Connection):
        if self.context.options.mode == AnonMode.SYNC_STRUCT_RESTORE:
            return

        async with connection.transaction(isolation='repeatable_read'):
            await connection.execute("SET CONSTRAINTS ALL DEFERRED;")
            transaction_snapshot_id = await connection.fetchval("select pg_export_snapshot()")
            await self._process_restore_data(transaction_snapshot_id)

        self._compare_rows_count()

    def _compare_rows_count(self):
        if self.context.black_listed_tables or self.context.white_listed_tables:
            dumped_rows = 0

            for table_data in self.metadata.files.values():
                table_name = (table_data['schema'], table_data['table'])

                if self.context.black_listed_tables and table_name in self.context.black_listed_tables:
                    continue

                if self.context.white_listed_tables and table_name not in self.context.white_listed_tables:
                    continue

                dumped_rows += int(table_data['rows'])
        else:
            dumped_rows = int(self.metadata.total_rows)

        restored_rows = self.context.total_rows
        if restored_rows != dumped_rows:
            raise PgAnonError(
                ErrorCode.ROW_COUNT_MISMATCH,
                f"The number of restored rows ({restored_rows}) is different from the metadata ({dumped_rows})"
            )

    async def _restore_pre_data(self):
        if self._skip_pre_data_restore:
            self.context.logger.info("-------------> Skipped restore pre-data (pg_restore)")
            return

        self.context.logger.info("-------------> Started restore pre-data (pg_restore)")
        await self._run_pg_restore("pre-data")
        self.context.logger.info("<------------- Finished restore pre-data (pg_restore)")

    async def _restore_post_data(self):
        if self._skip_post_data_restore:
            self.context.logger.info("-------------> Skipped restore post-data (pg_restore)")
            return

        self.context.logger.info("-------------> Started restore post-data (pg_restore)")
        await self._run_pg_restore("post-data")
        self.context.logger.info("<------------- Finished restore post-data (pg_restore)")

    async def _drop_database(self):
        if not self.context.options.drop_db:
            return

        connection_params = copy(self.context.connection_params)
        connection_params.database = "template1"
        connection = await create_connection(connection_params, server_settings=self.context.server_settings)
        db_params = await connection.fetchrow(get_db_params(self.context.options.db_name))

        await connection.execute(
            f"""
            DROP DATABASE "{self.context.options.db_name}";
            """
        )

        await connection.execute(
            f"""
            CREATE DATABASE "{self.context.options.db_name}"
            WITH TEMPLATE template0
                 OWNER {db_params[1]}
                 ENCODING '{db_params[2]}'
                 LC_COLLATE '{db_params[3]}'
                 LC_CTYPE '{db_params[4]}';
            """
        )

        await connection.close()

    def _prepare_tables_lists(self):
        if self.context.options.mode == AnonMode.SYNC_STRUCT_RESTORE:
            return

        tables = [(table_info["schema"], table_info["table"]) for table_info in self.metadata.files.values()]
        self.context.set_tables_lists(tables)

    def _save_input_dicts_to_run_dir(self):
        if not self.context.options.save_dicts:
            return

        input_dicts_dir = Path(self.context.options.run_dir) / 'input'
        input_dicts_dir.mkdir(parents=True, exist_ok=True)

        input_dict_files = []
        if self.context.options.partial_tables_dict_files:
            input_dict_files.extend(self.context.options.partial_tables_dict_files)
        if self.context.options.partial_tables_exclude_dict_files:
            input_dict_files.extend(self.context.options.partial_tables_exclude_dict_files)

        for dict_file in input_dict_files:
            shutil.copy2(dict_file, input_dicts_dir / Path(dict_file).name)


    async def run_analyze(self):
        if (self.context.options.mode == AnonMode.SYNC_STRUCT_RESTORE
                or self.metadata.dbg_stage_2_validate_data
                or self.metadata.dbg_stage_3_validate_full):

            self.context.logger.info("-------------> Skipped analyze")
            return

        self.context.logger.info("-------------> Started analyze")
        pool = await create_pool(
            connection_params=self.context.connection_params,
            server_settings=self.context.server_settings,
            min_size=self.context.options.db_connections_per_process,
            max_size=self.context.options.db_connections_per_process
        )

        queries = self._generate_analyze_queries()
        loop = asyncio.get_event_loop()
        tasks = set()
        for query in queries:
            if len(tasks) >= self.context.options.db_connections_per_process:
                done, tasks = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                exception = done.pop().exception()
                if exception is not None:
                    await pool.close()
                    raise exception

            tasks.add(
                loop.create_task(
                    run_query_in_pool(pool, query)
                )
            )

        # Wait for the remaining queries to finish
        await asyncio.wait(tasks)
        await pool.close()
        self.context.logger.info("<------------- Finished analyze")

    @staticmethod
    async def validate_restore(context: Context):
        context.logger.info("-------------> Started validate_restore")

        try:
            context.read_prepared_dict()
        except:
            context.logger.error(exception_helper(show_traceback=True))
            return [], {}

        if "validate_tables" in context.prepared_dictionary_obj:
            connection = await create_connection(context.connection_params, server_settings=context.server_settings)
            db_objs = await connection.fetch(
                """
                select n.nspname, c.relname --, c.reltuples
                from pg_class c
                join pg_namespace n on c.relnamespace = n.oid
                where
                    c.relkind = 'r' and
                    n.nspname not in ('pg_catalog', 'information_schema') and
                    c.reltuples > 0
            """
            )
            await connection.close()
            tables_in_target_db = []
            for item in db_objs:
                tables_in_target_db.append(item[0] + "." + item[1])

            tables_in_dict = []
            for d in context.prepared_dictionary_obj["validate_tables"]:
                tables_in_dict.append(d["schema"] + "." + d["table"])

            diff_l = list(set(tables_in_target_db) - set(tables_in_dict))
            diff_r = list(set(tables_in_dict) - set(tables_in_target_db))

            if len(diff_r) > 0:
                msg = (
                    "validate_tables: in target DB not found tables:\n%s"
                    % json.dumps(diff_r, indent=4)
                )
                context.logger.error(msg)
                raise PgAnonError(ErrorCode.VALIDATION_FAILED, msg)

            if len(diff_l) > 0:
                msg = (
                    """validate_tables: non-empty tables were found in target database that
                    are not described in the dictionary:\n%s"""
                    % json.dumps(diff_l, indent=4)
                )
                context.logger.error(msg)
                raise PgAnonError(ErrorCode.VALIDATION_FAILED, msg)
        else:
            msg = "Section validate_tables is not found in dictionary!"
            context.logger.error(msg)
            raise PgAnonError(ErrorCode.VALIDATION_FAILED, msg)

        context.logger.info("<------------- Finished validate_restore")

    async def run(self) -> None:
        self.context.logger.info("-------------> Started restore")
        connection = None

        try:
            self._save_input_dicts_to_run_dir()

            await self._drop_database()
            connection = await create_connection(
                self.context.connection_params,
                server_settings=self.context.server_settings
            )

            await check_required_connections(connection, self.context.options.db_connections_per_process)

            await self._check_db_is_empty(connection)
            self._check_utils_version_for_dump()

            self.context.read_partial_tables_dicts()
            self._prepare_tables_lists()

            await self._create_schemas_for_partial_mode(connection)
            await self._create_extensions_for_partial_mode(connection)
            await self._create_objects_from_ddl_for_partial_mode(connection)
            self._make_filtered_toc_list()

            await self._restore_pre_data()
            await self._drop_constraints(connection)

            await self._restore_data(connection)

            await self._restore_post_data()
            await self._sequences_init(connection)

            await self.run_analyze()
            self._remove_toc_lists()

            self.context.logger.info("<------------- Finished restore")
        except Exception as ex:
            raise ex
        finally:
            if connection:
                await connection.close()

            if self.context.options.save_dicts:
                save_dicts_info_file(self.context.options)
