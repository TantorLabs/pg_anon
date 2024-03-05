import asyncio
import gzip
import json
import os
import shutil
import subprocess

import re
from logging import getLogger
from multiprocessing import Pool
import asyncpg

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

    def __init__(self, ctx):
        self.result.result_code = ResultCode.DONE
        self.ctx = ctx

    async def run_pg_restore(self, section):
        os.environ["PGPASSWORD"] = self.ctx.args.db_user_password
        command = [
            self.ctx.args.pg_restore,
            "-h",
            self.ctx.args.db_host,
            "-p",
            str(self.ctx.args.db_port),
            "-v",
            "-w",
            "-U",
            self.ctx.args.db_user,
            "-d",
            self.ctx.args.db_name,
            "-j",
            str(self.ctx.args.threads),
            os.path.join(
                self.ctx.args.input_dir, section.replace("-", "_") + ".backup"
            ),
        ]
        if not self.ctx.args.db_host:
            del command[command.index("-h") : command.index("-h") + 2]

        if not self.ctx.args.db_user:
            del command[command.index("-U") : command.index("-U") + 2]

        self.ctx.logger.debug(str(command))
        proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        err, out = proc.communicate()
        for v in out.decode("utf-8").split("\n"):
            self.ctx.logger.info(v)

    async def seq_init(self):
        db_conn = await asyncpg.connect(**self.ctx.conn_params)
        if self.ctx.args.seq_init_by_max_value:
            query = """
                DO $$
                DECLARE
                    cmd text;
                    schema text;
                BEGIN
                    FOR cmd, schema IN (
                        select
                           ('SELECT setval(''' || T.seq_name || ''', max("' || T.column_name || '") + 1) FROM "' || T.table_name || '"') as cmd,
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
            self.ctx.logger.debug(query)
            await db_conn.execute(query)
        else:
            for v in self.ctx.metadata["seq_lastvals"].values():
                query = """
                    SET search_path = '%s';
                    SELECT setval(quote_ident('%s'), %s + 1);
                """ % (
                    v["schema"].replace("'", "''"),
                    v["seq_name"].replace("'", "''"),
                    v["value"],
                )
                self.ctx.logger.info(query)
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

        self.ctx.logger.info(f"{'>':=>20} Finished task {str(table_name)}")

    async def make_restore_impl(self, sn_id):
        pool = await asyncpg.create_pool(
            **self.ctx.conn_params,
            min_size=self.ctx.args.threads,
            max_size=self.ctx.args.threads
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
        if get_major_version(self.ctx.pg_version) < get_major_version(
            self.ctx.metadata["pg_version"]
        ):
            await db_conn.close()
            raise Exception(
                "Target PostgreSQL major version %s is below than source %s!"
                % (self.ctx.pg_version, self.ctx.metadata["pg_version"])
            )
        if get_major_version(
            get_pg_util_version(self.ctx.args.pg_restore)
        ) < get_major_version(self.ctx.metadata["pg_dump_version"]):
            await db_conn.close()
            raise Exception(
                "pg_restore major version %s is below than source pg_dump version %s!"
                % (
                    get_pg_util_version(self.ctx.args.pg_restore),
                    self.ctx.metadata["pg_dump_version"],
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
        self.ctx.logger.info("Free disk space: " + pretty_size(disk_size.free))
        self.ctx.logger.info(
            "Required disk space: "
            + pretty_size(int(self.ctx.metadata["total_tables_size"]) * 1.5)
        )
        if disk_size.free < int(self.ctx.metadata["total_tables_size"]) * 1.5:
            raise Exception(
                "Not enough freed disk space! Free %s, Required %s"
                % (
                    pretty_size(disk_size.free),
                    pretty_size(int(self.ctx.metadata["total_tables_size"]) * 1.5),
                )
            )

    @staticmethod
    def read_metadata(current_dir, input_dir):
        metadata_file = open(
            os.path.join(current_dir, "dict", input_dir, "metadata.json"), "r"
        )
        metadata_content = metadata_file.read()
        metadata_file.close()
        metadata = eval(metadata_content)
        return metadata

    @staticmethod
    async def check_db_empty(db_conn, mode):
        # db_conn = await pool.acquire()
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

        # await db_conn.close()

        if not db_is_empty and mode != AnonMode.SYNC_DATA_RESTORE:
            db_conn.close()
        raise Exception(f"Target DB {ctx.conn_params['database']} is not empty!")

    @staticmethod
    def get_dir(current_dir, input_dir):
        if input_dir.find("""/""") == -1 and input_dir.find("""\\""") == -1:
            input_dir = os.path.join(current_dir, "output", input_dir)

        if not os.path.exists(input_dir):
            msg = "ERROR: input directory %s does not exists" % input_dir
            logger.error(msg)
            raise RuntimeError(msg)  # end
        return input_dir

    @staticmethod
    async def drop_custom_check_constr(db_conn):
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
            self.ctx.logger.error(
                "<------------- make_restore failed\n" + exception_helper()
            )
            self.result.result_code = ResultCode.FAIL
        finally:
            await tr.commit()
            await db_conn.close()

        if self.ctx.total_rows != int(self.ctx.metadata["total_rows"]):
            self.ctx.logger.error(
                "The number of restored rows (%s) is different from the metadata (%s)"
                % (str(self.ctx.total_rows), self.ctx.metadata["total_rows"])
            )
            self.result.result_code = ResultCode.FAIL

    async def run_custom_query(self, pool, query):
        # in single tx
        self.ctx.logger.info("================> Started query %s" % str(query))

        db_conn = await pool.acquire()
        try:
            await db_conn.execute(query)
        except Exception as e:
            self.ctx.logger.error("Exception in dump_obj_func:\n" + exception_helper())
            raise Exception("Can't execute query: %s" % query)
        finally:
            await db_conn.close()
            await pool.release(db_conn)

        self.ctx.logger.info("<================ Finished query %s" % str(query))

    async def run_analyze(self):
        self.ctx.logger.info("-------------> Started analyze")
        pool = await asyncpg.create_pool(
            **self.ctx.conn_params,
            min_size=self.ctx.args.threads,
            max_size=self.ctx.args.threads
        )

        queries = self.generate_analyze_queries()
        loop = asyncio.get_event_loop()
        tasks = set()
        for v in queries:
            if len(tasks) >= self.ctx.args.threads:
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
        self.ctx.logger.info("<------------- Finished analyze")

    async def run_mode_restore_asy(self):
        self.ctx.logger.info("-------------> Started restore")

        self.ctx.args.input_dir = self.get_dir(
            current_dir=self.ctx.current_dir, input_dir=self.ctx.args.input_dir
        )

        # pool = await asyncpg.create_pool(
        #     **self.ctx.conn_params,
        #     min_size=self.ctx.args.threads,
        #     max_size=self.ctx.args.threads,
        # )
        db_conn = await asyncpg.connect(**self.ctx.conn_params)
        await self.check_db_empty(db_conn, self.ctx.args.mode)

        self.ctx.metadata = self.read_metadata(
            self.ctx.current_dir, self.ctx.args.input_dir
        )

        if not self.ctx.args.disable_checks:
            await self.check_version(db_conn)  # Закрывается ли соединение?
            await self.check_free_disk_space(db_conn)

        # SYNC_STRUCT_RESTORE or SYNC_DATA_RESTORE
        await self.run_pg_restore(
            "pre-data"
        )  # ctx.args.mode != AnonMode.SYNC_DATA_RESTORE

        if self.ctx.args.drop_custom_check_constr:
            await self.drop_custom_check_constr(db_conn)

        # SYNC_STRUCT_RESTORE or SYNC_DATA_RESTORE
        # there is `db_conn.close` in finally
        await self.execute_database_restore_transaction(
            db_conn
        )  # ctx.args.mode != AnonMode.SYNC_STRUCT_RESTORE

        # SYNC_STRUCT_RESTORE or SYNC_DATA_RESTORE
        await self.run_pg_restore(
            "post-data"
        )  # ctx.args.mode != AnonMode.SYNC_DATA_RESTORE

        await self.seq_init()  # ctx.args.mode != AnonMode.SYNC_STRUCT_RESTORE

        await db_conn.close()

        logger.info("<------------- Finished restore")

    async def run_mode_sync_struct(self):
        self.ctx.logger.info("-------------> Started restore")

        self.ctx.args.input_dir = self.get_dir(
            current_dir=self.ctx.current_dir, input_dir=self.ctx.args.input_dir
        )

        # pool = await asyncpg.create_pool(
        #     **self.ctx.conn_params,
        #     min_size=self.ctx.args.threads,
        #     max_size=self.ctx.args.threads,
        # )
        db_conn = await asyncpg.connect(**self.ctx.conn_params)
        await self.check_db_empty(db_conn, self.ctx.args.mode)

        self.ctx.metadata = self.read_metadata(
            self.ctx.current_dir, self.ctx.args.input_dir
        )

        if not self.ctx.args.disable_checks:
            await self.check_version(db_conn)  # Закрывается ли соединение?
            await self.check_free_disk_space(db_conn)

        for v in self.ctx.metadata["schemas"]:
            query = 'CREATE SCHEMA IF NOT EXISTS "%s"' % v
            self.ctx.logger.info("AnonMode.SYNC_STRUCT_RESTORE: " + query)
            await db_conn.execute(query)

        # SYNC_STRUCT_RESTORE or SYNC_DATA_RESTORE
        await self.run_pg_restore(
            "pre-data"
        )  # ctx.args.mode != AnonMode.SYNC_DATA_RESTORE

        if self.ctx.args.drop_custom_check_constr:
            await self.drop_custom_check_constr(db_conn)

        # SYNC_STRUCT_RESTORE or SYNC_DATA_RESTORE
        await self.run_pg_restore(
            "post-data"
        )  # ctx.args.mode != AnonMode.SYNC_DATA_RESTORE

        await db_conn.close()

        logger.info("<------------- Finished restore")

    async def run_mode_sync_data(self):
        self.ctx.logger.info("-------------> Started restore")

        self.ctx.args.input_dir = self.get_dir(
            current_dir=self.ctx.current_dir, input_dir=self.ctx.args.input_dir
        )

        # pool = await asyncpg.create_pool(
        #     **self.ctx.conn_params,
        #     min_size=self.ctx.args.threads,
        #     max_size=self.ctx.args.threads,
        # )
        db_conn = await asyncpg.connect(**self.ctx.conn_params)
        await self.check_db_empty(db_conn, self.ctx.args.mode)

        self.ctx.metadata = self.read_metadata(
            self.ctx.current_dir, self.ctx.args.input_dir
        )

        if not self.ctx.args.disable_checks:
            await self.check_version(db_conn)  # Закрывается ли соединение?
            await self.check_free_disk_space(db_conn)

        if self.ctx.args.drop_custom_check_constr:
            await self.drop_custom_check_constr(db_conn)

        # SYNC_STRUCT_RESTORE or SYNC_DATA_RESTORE
        # there is `db_conn.close` in finally
        await self.execute_database_restore_transaction(
            db_conn
        )  # ctx.args.mode != AnonMode.SYNC_STRUCT_RESTORE

        await self.seq_init()  # ctx.args.mode != AnonMode.SYNC_STRUCT_RESTORE

        await db_conn.close()

        logger.info("<------------- Finished restore")

    async def make_restore(self):
        if self.ctx.args.mode == AnonMode.RESTORE:
            await self.run_mode_restore_asy()
        elif self.ctx.args.mode == AnonMode.SYNC_STRUCT_RESTORE:
            await self.run_mode_sync_struct()
        elif self.ctx.args.mode == AnonMode.SYNC_DATA_RESTORE:
            await self.run_mode_sync_data()
        return self.result


async def validate_restore(ctx):
    result = PgAnonResult()
    result.result_code = ResultCode.DONE
    ctx.logger.info("-------------> Started validate_restore")

    try:
        dictionary_file = open(
            os.path.join(ctx.current_dir, "dict", ctx.args.dict_file), "r"
        )
        ctx.dictionary_content = dictionary_file.read()
        dictionary_file.close()
        dictionary_obj = eval(ctx.dictionary_content)
    except:
        ctx.logger.error(exception_helper(show_traceback=True))
        return [], {}

    if "validate_tables" in dictionary_obj:
        db_conn = await asyncpg.connect(**ctx.conn_params)
        db_objs = await db_conn.fetch(
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
        await db_conn.close()
        tables_in_target_db = []
        for item in db_objs:
            tables_in_target_db.append(item[0] + "." + item[1])

        tables_in_dict = []
        for d in dictionary_obj["validate_tables"]:
            tables_in_dict.append(d["schema"] + "." + d["table"])

        diff_l = list(set(tables_in_target_db) - set(tables_in_dict))
        diff_r = list(set(tables_in_dict) - set(tables_in_target_db))

        if len(diff_r) > 0:
            ctx.logger.error(
                "validate_tables: in target DB not found tables:\n%s"
                % json.dumps(diff_r, indent=4)
            )
            result.result_code = ResultCode.FAIL

        if len(diff_l) > 0:
            ctx.logger.error(
                """validate_tables: non-empty tables were found in target database that
                are not described in the dictionary:\n%s"""
                % json.dumps(diff_l, indent=4)
            )
            result.result_code = ResultCode.FAIL
    else:
        ctx.logger.error("Section validate_tables is not found in dictionary!")
        result.result_code = ResultCode.FAIL

    ctx.logger.info("<------------- Finished validate_restore")
    return result
