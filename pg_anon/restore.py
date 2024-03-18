import asyncio
import gzip
import json
import os
import shutil
import subprocess

import re
from logging import getLogger
import asyncpg
from typing import List, Optional, Any, Dict

from pg_anon.common import (
    AnonMode,
    PgAnonResult,
    ResultCode,
    exception_helper,
    get_major_version,
    get_pg_util_version,
    pretty_size,
)
from pg_anon.context import Context

logger = getLogger(__name__)


class Restore:
    result: PgAnonResult = PgAnonResult()

    def __init__(
        self,
        pg_restore_path: str,
        db_host: str,
        db_port: int,
        db_user: str,
        db_name: str,
        db_user_password: str,
        threads: int,
        input_dir: str,
        copy_options: str,
        seq_init_by_max_value: bool,
        disable_checks: bool,
        drop_custom_check_constr: bool,
        mode: AnonMode,
        conn_params: dict,
        metadata: dict,
        current_dir: str,
        total_rows: int,
        pg_version: str,
    ):
        self.db_user_password = db_user_password
        self.pg_restore_path = pg_restore_path
        self.db_host = db_host
        self.db_port = db_port
        self.db_user = db_user
        self.db_name = db_name
        self.threads = threads
        self.input_dir = input_dir
        self.current_dir = current_dir
        self.copy_options = copy_options
        self.seq_init_by_max_value = seq_init_by_max_value
        self.disable_checks = disable_checks
        self.drop_custom_check_constr = drop_custom_check_constr
        self.mode = mode
        self.conn_params = conn_params
        self.metadata = metadata
        self.total_rows = total_rows
        self.pg_version = pg_version
        self.result.result_code = ResultCode.DONE

    async def run_pg_restore(self, section: str):
        os.environ["PGPASSWORD"] = self.db_user_password
        command = [
            self.pg_restore_path,
            "-h",
            self.db_host,
            "-p",
            str(self.db_port),
            "-v",
            "-w",
            "-U",
            self.db_user,
            "-d",
            self.db_name,
            "-j",
            str(self.threads),
            os.path.join(self.input_dir, section.replace("-", "_") + ".backup"),
        ]
        if not self.db_host:
            del command[command.index("-h") : command.index("-h") + 2]

        if not self.db_user:
            del command[command.index("-U") : command.index("-U") + 2]

        logger.debug(str(command))
        proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        err, out = proc.communicate()
        for v in out.decode("utf-8").split("\n"):
            logger.info(v)

    async def seq_init(self):
        db_conn = await asyncpg.connect(**self.conn_params)
        if self.seq_init_by_max_value:
            query = """
                DO $$
                DECLARE
                    cmd text;
                    schema text;
                BEGIN
                    FOR cmd, schema IN (
                        select
                           ('SELECT setval(''' || T.seq_name || ''', max("' || T.column_name || '") + 1) 
                           FROM "' || T.table_name || '"') as cmd,
                           T.table_schema as schema
                        FROM (
                                select
                                   substring(t.column_default from 10 for length(t.column_default) - 21) as seq_name,
                                   t.table_schema,
                                   t.table_name,
                                   t.column_name
                                   FROM (
                                       SELECT table_schema, table_name, column_name, column_default
                                       FROM information_schema.columns
                                       WHERE column_default LIKE 'nextval%'
                                   ) T
                        ) T
                    ) LOOP
                        EXECUTE 'SET search_path = ''' || schema || ''';';
                        -- EXECUTE cmd;
                        raise notice '%', cmd;
                    END LOOP;
                    SET search_path = 'public';
                END$$;"""
            logger.debug(query)
            await db_conn.execute(query)
        else:
            for v in self.metadata["seq_lastvals"].values():
                query = """
                    SET search_path = '%s';
                    SELECT setval(quote_ident('%s'), %s + 1);
                """ % (
                    v["schema"].replace("'", "''"),
                    v["seq_name"].replace("'", "''"),
                    v["value"],
                )
                logger.info(query)
                await db_conn.execute(query)

        await db_conn.close()


def generate_analyze_queries(ctx):
    analyze_queries = []
    for file_name, target in ctx.metadata["files"].items():
        schema = target["schema"]
        table = target["table"]
        analyze_query = 'analyze "%s"."%s"' % (schema, table)
        analyze_queries.append(analyze_query)
    return analyze_queries


async def restore_table_data(
    ctx: Context,
    pool: asyncpg.Pool,
    dump_file: str,
    schema_name: str,
    table_name: str,
    sn_id: str,
):
    ctx.logger.info(f"{'>':=>20} Started task copy_to_table {table_name}")
    extracted_file = f"{dump_file.removesuffix('.bin.gz')}.bin"

    with gzip.open(dump_file, "rb") as src_file, open(extracted_file, "wb") as trg_file:
        trg_file.writelines(src_file)
    db_conn = await pool.acquire()
    try:
        await db_conn.execute("BEGIN ISOLATION LEVEL REPEATABLE READ;")
        await db_conn.execute(f"SET TRANSACTION SNAPSHOT '{sn_id}';")

        result = await db_conn.copy_to_table(
            schema_name=schema_name,
            table_name=table_name,
            source=extracted_file,
            format="binary",
        )
        ctx.total_rows += int(re.findall(r"(\d+)", result)[0])
        await db_conn.execute("COMMIT;")
    except Exception as exc:
        ctx.logger.error(
            f"Exception in restore_obj_func:"
            f" {schema_name = }"
            f" {table_name = }"
            f"\n{exc.query = }"
            f"\n{exc.position = }"
            f"\n{exc = }"
        )

    finally:
        os.remove(extracted_file)
        await pool.release(db_conn)

        logger.info(f"{'>':=>20} Finished task {str(table_name)}")

    async def make_restore_impl(self, sn_id):
        pool = await asyncpg.create_pool(
            **self.conn_params, min_size=self.threads, max_size=self.threads
        )

    loop = asyncio.get_event_loop()
    tasks = set()
    for file_name, target in ctx.metadata["files"].items():
        full_path = os.path.join(
            ctx.current_dir, "output", ctx.args.input_dir, file_name
        )
        if len(tasks) >= ctx.args.threads:
            # Wait for some restore to finish before adding a new one
            done, tasks = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            exception = done.pop().exception()
            if exception is not None:
                await pool.close()
                raise exception
        tasks.add(
            loop.create_task(
                restore_table_data(
                    ctx=ctx,
                    pool=pool,
                    dump_file=str(full_path),
                    schema_name=target["schema"],
                    table_name=target["table"],
                    sn_id=sn_id,
                )
            )
        )

        # Wait for the remaining restores to finish
        await asyncio.wait(tasks)
        await pool.close()

    async def check_version(self, db_conn):
        if get_major_version(self.pg_version) < get_major_version(
            self.metadata["pg_version"]
        ):
            await db_conn.close()
            raise Exception(
                "Target PostgreSQL major version %s is below than source %s!"
                % (self.pg_version, self.metadata["pg_version"])
            )
        if get_major_version(
            get_pg_util_version(self.pg_restore_path)
        ) < get_major_version(self.metadata["pg_dump_version"]):
            await db_conn.close()
            raise Exception(
                "pg_restore major version %s is below than source pg_dump version %s!"
                % (
                    get_pg_util_version(self.pg_restore_path),
                    self.metadata["pg_dump_version"],
                )
            )

    async def check_free_disk_space(self, db_conn):
        data_directory_location = await db_conn.fetchval(
            """
            SELECT setting
            FROM pg_settings
            WHERE name = 'data_directory'
            """
        )
        disk_size = shutil.disk_usage(data_directory_location)
        logger.info("Free disk space: " + pretty_size(disk_size.free))
        logger.info(
            "Required disk space: "
            + pretty_size(int(self.metadata["total_tables_size"]) * 1.5)
        )
        if disk_size.free < int(self.metadata["total_tables_size"]) * 1.5:
            raise Exception(
                "Not enough freed disk space! Free %s, Required %s"
                % (
                    pretty_size(disk_size.free),
                    pretty_size(int(self.metadata["total_tables_size"]) * 1.5),
                )
            )

    @staticmethod
    def read_metadata(current_dir: str, input_dir: str) -> Dict:
        metadata_file = open(
            os.path.join(current_dir, "dict", input_dir, "metadata.json"), "r"
        )
        metadata_content = metadata_file.read()
        metadata_file.close()
        metadata = eval(metadata_content)
        return metadata

    @staticmethod
    async def check_db_empty(db_conn, mode: AnonMode):
        db_is_empty = await db_conn.fetchval(
            """
            SELECT NOT EXISTS(
                SELECT table_schema, table_name
                FROM information_schema.tables
                WHERE table_schema not in (
                        'pg_catalog',
                        'information_schema',
                        'anon_funcs'
                    ) AND table_type = 'BASE TABLE'
            )"""
        )

        if not db_is_empty and mode != AnonMode.SYNC_DATA_RESTORE:
            db_conn.close()
        raise Exception(f"Target DB {ctx.conn_params['database']} is not empty!")

    @staticmethod
    def get_dir(current_dir: str, input_dir: str):
        if input_dir.find("""/""") == -1 and input_dir.find("""\\""") == -1:
            input_dir = os.path.join(current_dir, "output", input_dir)

        if not os.path.exists(input_dir):
            msg = "ERROR: input directory %s does not exists" % input_dir
            logger.error(msg)
            raise RuntimeError(msg)  # end
        return input_dir

    @staticmethod
    async def drop_custom_check_constrains(db_conn):
        # drop all CHECK constrains containing user-defined procedures to avoid
        # performance degradation at the data loading stage
        check_constraints = await db_conn.fetch(
            """
            SELECT nsp.nspname,  cl.relname, pc.conname, pg_get_constraintdef(pc.oid)
            -- pc.consrc removed in 12 version
            FROM (
                SELECT substring(T.v FROM position(' ' in T.v) + 1 for length(T.v) )::bigint as func_oid, t.conoid
                from (
                    SELECT T.v as v, t.conoid
                    FROM (
                            SELECT ((SELECT regexp_matches(t.v, '(:funcid\s\d+)', 'g'))::text[])[1] as v, t.conoid
                            FROM (
                                SELECT conbin::text as v, oid as conoid
                                FROM pg_constraint
                                WHERE contype = 'c'
                            ) T
                    ) T WHERE length(T.v) > 0
                ) T
            ) T
            INNER JOIN pg_constraint pc on T.conoid = pc.oid
            INNER JOIN pg_class cl on cl.oid = pc.conrelid
            INNER JOIN pg_namespace nsp on cl.relnamespace = nsp.oid
            WHERE T.func_oid in (
                SELECT  p.oid
                FROM    pg_namespace n
                INNER JOIN pg_proc p ON p.pronamespace = n.oid
                WHERE   n.nspname not in ( 'pg_catalog', 'information_schema' )
            )
        """
        )

        if check_constraints is not None:
            for conn in check_constraints:
                logger.info("Removing constraints: " + conn[2])
                query = 'ALTER TABLE "{0}"."{1}" DROP CONSTRAINT IF EXISTS "{2}" CASCADE'.format(
                    conn[0], conn[1], conn[2]
                )
                await db_conn.execute(query)

    async def execute_database_restore_transaction(self, db_conn):
        tr = db_conn.transaction()
        await tr.start()
        try:
            await db_conn.execute("BEGIN ISOLATION LEVEL REPEATABLE READ;")
            await db_conn.execute("SET CONSTRAINTS ALL DEFERRED;")
            sn_id = await db_conn.fetchval("select pg_export_snapshot()")
            await self.make_restore_impl(sn_id)
        except:
            logger.error("<------------- make_restore failed\n" + exception_helper())
            self.result.result_code = ResultCode.FAIL
        finally:
            await tr.commit()
            await db_conn.close()

        if self.total_rows != int(self.metadata["total_rows"]):
            logger.error(
                "The number of restored rows (%s) is different from the metadata (%s)"
                % (str(self.total_rows), self.metadata["total_rows"])
            )
            self.result.result_code = ResultCode.FAIL

    @staticmethod
    async def run_custom_query(pool, query):
        # in single tx
        logger.info("================> Started query %s" % str(query))

        db_conn = await pool.acquire()
        try:
            await db_conn.execute(query)
        except Exception as e:
            logger.error("Exception in dump_obj_func:\n" + exception_helper())
            raise Exception("Can't execute query: %s" % query)
        finally:
            await db_conn.close()
            await pool.release(db_conn)

        logger.info("<================ Finished query %s" % str(query))

    async def run_analyze(self):
        logger.info("-------------> Started analyze")
        pool = await asyncpg.create_pool(
            **self.conn_params, min_size=self.threads, max_size=self.threads
        )

        queries = self.generate_analyze_queries()
        loop = asyncio.get_event_loop()
        tasks = set()
        for v in queries:
            if len(tasks) >= self.threads:
                done, tasks = await asyncio.wait(
                    tasks, return_when=asyncio.FIRST_COMPLETED
                )
                exception = done.pop().exception()
                if exception is not None:
                    await pool.close()
                    raise exception
            tasks.add(loop.create_task(self.run_custom_query(pool, v)))

        # Wait for the remaining queries to finish
        await asyncio.wait(tasks)
        await pool.close()
        logger.info("<------------- Finished analyze")

    async def run_mode_restore_asy(self):
        logger.info("-------------> Started restore")

        self.input_dir = self.get_dir(
            current_dir=self.current_dir, input_dir=self.input_dir
        )

        db_conn = await asyncpg.connect(**self.conn_params)
        await self.check_db_empty(db_conn, self.mode)

        self.metadata = self.read_metadata(self.current_dir, self.input_dir)

        if not self.disable_checks:
            await self.check_version(db_conn)
            await self.check_free_disk_space(db_conn)  # FixME: no roots to watch

        await self.run_pg_restore("pre-data")

        if self.drop_custom_check_constr:
            await self.drop_custom_check_constrains(db_conn)

        await self.execute_database_restore_transaction(db_conn)

        await self.run_pg_restore("post-data")

        await self.seq_init()

        await db_conn.close()

        logger.info("<------------- Finished restore")

    async def run_mode_sync_struct(self):
        logger.info("-------------> Started restore")

        self.input_dir = self.get_dir(
            current_dir=self.current_dir, input_dir=self.input_dir
        )

        db_conn = await asyncpg.connect(**self.conn_params)
        await self.check_db_empty(db_conn, self.mode)

        self.metadata = self.read_metadata(self.current_dir, self.input_dir)

        if not self.disable_checks:
            await self.check_version(db_conn)
            await self.check_free_disk_space(db_conn)

        for v in self.metadata["schemas"]:
            query = 'CREATE SCHEMA IF NOT EXISTS "%s"' % v
            logger.info("AnonMode.SYNC_STRUCT_RESTORE: " + query)
            await db_conn.execute(query)

        await self.run_pg_restore("pre-data")

        if self.drop_custom_check_constr:
            await self.drop_custom_check_constrains(db_conn)

        await self.run_pg_restore("post-data")

        await db_conn.close()

        logger.info("<------------- Finished restore")

    async def run_mode_sync_data(self):
        logger.info("-------------> Started restore")

        self.input_dir = self.get_dir(
            current_dir=self.current_dir, input_dir=self.input_dir
        )

        db_conn = await asyncpg.connect(**self.conn_params)
        await self.check_db_empty(db_conn=db_conn, mode=self.mode)

        self.metadata = self.read_metadata(
            current_dir=self.current_dir, input_dir=self.input_dir
        )

        if not self.disable_checks:
            await self.check_version(db_conn)
            await self.check_free_disk_space(db_conn)

        if self.drop_custom_check_constr:
            await self.drop_custom_check_constrains(db_conn)

        await self.execute_database_restore_transaction(db_conn)

        await self.seq_init()

        await db_conn.close()

        logger.info("<------------- Finished restore")

    async def make_restore(self):
        if self.mode == AnonMode.RESTORE:
            await self.run_mode_restore_asy()
        elif self.mode == AnonMode.SYNC_STRUCT_RESTORE:
            await self.run_mode_sync_struct()
        elif self.mode == AnonMode.SYNC_DATA_RESTORE:
            await self.run_mode_sync_data()
        return self.result
