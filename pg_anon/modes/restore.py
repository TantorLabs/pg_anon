import asyncio
import gzip
import json
import os
import re
import shutil
import subprocess
from copy import copy
from pathlib import Path
from typing import Optional

import asyncpg
from asyncpg import Connection

from pg_anon.common.constants import BASE_DIR
from pg_anon.common.db_queries import get_check_constraint_query, get_sequences_max_value_init_query, get_db_params
from pg_anon.common.db_utils import create_connection, create_pool, check_db_is_empty, run_query_in_pool
from pg_anon.common.dto import Metadata
from pg_anon.common.enums import AnonMode
from pg_anon.common.utils import (
    exception_helper,
    get_major_version,
    get_pg_util_version,
    pretty_size, save_dicts_info_file,
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

    def __init__(self, context: Context):
        self.context = context
        self.input_dir = Path.cwd() / self.context.options.input_dir

        if not self.input_dir.exists():
            msg = f"ERROR: input directory {self.input_dir} does not exists"
            self.context.logger.error(msg)
            raise RuntimeError(msg)

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
            raise Exception(
                f"Target PostgreSQL major version {target_postgres_version} is below than source {source_postgres_version}!"
            )

        if target_pg_restore_version < source_pg_dump_version:
            raise Exception(
                f"pg_restore major version {target_pg_restore_version} is below than source pg_dump version {source_pg_dump_version}!"
            )

    async def _check_db_is_empty(self, connection: Connection):
        if not self._db_must_be_empty:
            return

        if not await check_db_is_empty(connection=connection):
            raise Exception(f"Target DB {self.context.connection_params.database} is not empty!")

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
            raise Exception(
                f"Not enough freed disk space! Free {free_disk_space}, Required {required_disk_space}"
            )

    def _make_filtered_toc_list(self):
        if not (self.context.black_listed_tables or self.context.white_listed_tables):
            return

        # Make blacklist for tables
        blacklist = [
            re.compile(fr".*{re.escape(schema)} {re.escape(table)}")
            for schema, table in self.context.black_listed_tables
        ]
        # Update blacklist for sequences
        blacklist.extend([
            re.compile(fr".*SEQUENCE.*{re.escape(seq['schema'])} {re.escape(seq['seq_name'])}")
            for seq in self.metadata.sequences_last_values.values()
            if (seq['schema'], seq['table']) in self.context.black_listed_tables
        ])

        # Make whitelist for tables
        whitelist = [
            re.compile(fr".*{re.escape(schema)} {re.escape(table)}")
            for schema, table in self.context.white_listed_tables
        ]
        # Update whitelist for sequences
        whitelist.extend([
            re.compile(fr".*SEQUENCE.*{re.escape(seq['schema'])} {re.escape(seq['seq_name'])}")
            for seq in self.metadata.sequences_last_values.values()
            if (seq['schema'], seq['table']) in self.context.white_listed_tables
        ])

        for section in ["pre_data", "post_data"]:
            command = ["pg_restore", "-l", str(self.input_dir / f"{section}.backup")]
            if section == "pre_data":
                self._toc_list_pre_data_file_path = self.input_dir / self._toc_list_pre_data_file_name
                toc_file_path = self._toc_list_pre_data_file_path
            else:
                self._toc_list_post_data_file_path = self.input_dir / self._toc_list_post_data_file_name
                toc_file_path = self._toc_list_post_data_file_path

            proc = subprocess.Popen(command, stdout=subprocess.PIPE, text=True)
            toc_lines, _ = proc.communicate()
            with open(toc_file_path, "w", encoding="utf-8") as f:
                for toc_line in toc_lines.split("\n"):
                    if toc_line.startswith(';'):
                        continue

                    if blacklist and any(p.search(toc_line) for p in blacklist):
                        continue

                    if whitelist and not any(p.search(toc_line) for p in whitelist):
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

        if self._toc_list_pre_data_file_path:
            command.extend([
                '-L',
                self._toc_list_pre_data_file_path if section == 'pre-data' else self._toc_list_post_data_file_path
            ])

        self.context.logger.debug(str(command))
        proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        # pg_restore put command result into stdout if not using "-f" option, else stdout is empty
        # pg_restore put logs into stderr
        _, pg_restore_logs = proc.communicate()

        for log_line in pg_restore_logs.split("\n"):
            self.context.logger.info(log_line)

        if proc.returncode != 0:
            msg = "ERROR: database restore has failed!"
            self.context.logger.error(msg)
            raise RuntimeError(msg)

    async def _sequences_init(self, connection: Connection):
        if self.context.options.mode == AnonMode.SYNC_STRUCT_RESTORE:
            return

        if self.context.options.seq_init_by_max_value:
            query = get_sequences_max_value_init_query()
            self.context.logger.debug(query)
            await connection.execute(query)
        else:
            for sequence_data in self.metadata.sequences_last_values.values():
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
        schemas = None
        if self.context.black_listed_tables or self.context.white_listed_tables:
            schemas = list({schema for schema, _ in self.context.tables})

        if self.metadata.partial_dump_schemas:
            schemas = copy(self.metadata.partial_dump_schemas)

        if not schemas:
            return

        for schema in schemas:
            query = f'CREATE SCHEMA IF NOT EXISTS "{schema}"'
            self.context.logger.info("PARTIAL RESTORE MODE: " + query)
            await connection.execute(query)

    async def _create_functions_for_partial_mode(self, connection: Connection):
        if not self.metadata.partial_dump_functions:
            return

        for create_function_query in self.metadata.partial_dump_functions:
            self.context.logger.info("PARTIAL RESTORE MODE: " + create_function_query)
            await connection.execute(create_function_query)

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

    def compare_rows_count(self):
        if self.context.black_listed_tables or self.context.white_listed_tables:
            dumped_rows = 0

            for table_data in self.context.metadata.files.values():
                if (table_data['schema'], table_data['name']) in self.context.black_listed_tables:
                    dumped_rows += int(table_data['rows'])
        else:
            dumped_rows = int(self.metadata.total_rows)

        restored_rows = self.context.total_rows
        if restored_rows != dumped_rows:
            raise ValueError(
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
            DROP DATABASE {self.context.options.db_name};
            """
        )

        await connection.execute(
            f"""
            CREATE DATABASE {self.context.options.db_name}
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
                raise RuntimeError(msg)

            if len(diff_l) > 0:
                msg = (
                    """validate_tables: non-empty tables were found in target database that
                    are not described in the dictionary:\n%s"""
                    % json.dumps(diff_l, indent=4)
                )
                context.logger.error(msg)
                raise RuntimeError(msg)
        else:
            msg = "Section validate_tables is not found in dictionary!"
            context.logger.error(msg)
            raise RuntimeError(msg)

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

            await self._check_db_is_empty(connection)
            self._check_utils_version_for_dump()

            self.context.read_partial_tables_dicts()
            self._prepare_tables_lists()

            await self._create_schemas_for_partial_mode(connection)
            await self._create_functions_for_partial_mode(connection)
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
            self.context.logger.error("<------------- Restore failed\n" + exception_helper())
            raise ex
        finally:
            if connection:
                await connection.close()

            if self.context.options.save_dicts:
                save_dicts_info_file(self.context.options)
