import copy
import json
import os
import re
import sys
import unittest
from decimal import Decimal
from typing import Dict, Set

from pg_anon import PgAnonApp
from pg_anon.cli import build_run_options
from pg_anon.common.db_utils import get_scan_fields_count, create_connection
from pg_anon.common.dto import PgAnonResult, ConnectionParams
from pg_anon.common.enums import ResultCode
from pg_anon.common.utils import (
    exception_helper,
    recordset_to_list_flat,
    to_json, get_dict_rule_for_table,
    get_file_name_from_path,
)
from pg_anon.context import Context
from pg_anon.modes.view_data import ViewDataMode
from pg_anon.modes.view_fields import ViewFieldsMode

passed_stages = []
rows_in_init_env = 1512
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))


class TestParams:
    test_db_user = "anon_test_user"  # default value
    test_db_user_password = "mYy5RexGsZ"
    test_db_host = "127.0.0.1"
    test_db_port = "5432"
    test_source_db = "test_source_db"
    test_target_db = "test_target_db"
    test_scale = "10"
    db_connections_per_process = 4
    test_processes = 4

    def __init__(self):
        config_path = os.path.dirname(os.path.abspath(__file__)) + '/config.yml'

        if os.environ.get("TEST_DB_USER") is not None:
            self.test_db_user = os.environ["TEST_DB_USER"]
        if os.environ.get("PGPASSWORD") is not None:
            self.test_db_user_password = os.environ["TEST_DB_USER_PASSWORD"]
        if os.environ.get("TEST_DB_USER_PASSWORD") is not None:
            self.test_db_user_password = os.environ["TEST_DB_USER_PASSWORD"]
        if os.environ.get("TEST_DB_HOST") is not None:
            self.test_db_host = os.environ["TEST_DB_HOST"]
        if os.environ.get("TEST_DB_PORT") is not None:
            self.test_db_port = os.environ["TEST_DB_PORT"]
        if os.environ.get("TEST_SOURCE_DB") is not None:
            self.test_source_db = os.environ["TEST_SOURCE_DB"]
        if os.environ.get("TEST_TARGET_DB") is not None:
            self.test_target_db = os.environ["TEST_TARGET_DB"]
        if os.environ.get("TEST_SCALE") is not None:
            self.test_scale = os.environ["TEST_SCALE"]
        if os.environ.get("TEST_DB_CONNECTIONS_PER_PROCESS") is not None:
            self.db_connections_per_process = os.environ["TEST_DB_CONNECTIONS_PER_PROCESS"]
        if os.environ.get("TEST_PROCESSES") is not None:
            self.test_processes = os.environ["TEST_PROCESSES"]
        self.test_config = os.environ.get("TEST_CONFIG", config_path)


params = TestParams()


class DBOperations:
    @staticmethod
    async def init_db(db_conn, db_name):
        try:
            await db_conn.execute(
                """
                SELECT pg_terminate_backend(pid)
                FROM pg_stat_activity
                WHERE pid <> pg_backend_pid()
                    AND datname = '%s'
                """
                % db_name
            )

            print("""DROP DATABASE IF EXISTS %s and CREATE DATABASE""" % db_name)
            await db_conn.execute("""DROP DATABASE IF EXISTS %s""" % db_name)
            await db_conn.execute(
                """
                CREATE DATABASE %s
                    WITH
                    OWNER = %s
                    ENCODING = 'UTF8'
                    LC_COLLATE = 'en_US.UTF-8'
                    LC_CTYPE = 'en_US.UTF-8'
                    template = template0
                """
                % (db_name, params.test_db_user)
            )
        except:
            print(exception_helper(show_traceback=True))

    @staticmethod
    async def init_db_once(db_conn, db_name):
        try:
            db_exists = await db_conn.fetch(
                """select datname from pg_database where datname = '%s'""" % db_name
            )
            if len(db_exists) == 0:
                await db_conn.execute(
                    """
                    CREATE DATABASE %s
                        WITH
                        OWNER = %s
                        ENCODING = 'UTF8'
                        LC_COLLATE = 'en_US.UTF-8'
                        LC_CTYPE = 'en_US.UTF-8'
                        template = template0
                    """
                    % (db_name, params.test_db_user)
                )
        except:
            print(exception_helper(show_traceback=True))

    @staticmethod
    async def init_env(db_conn, env_sql_file, scale=1):
        current_dir = os.path.dirname(os.path.realpath(__file__))
        with open(os.path.join(current_dir, "sql", env_sql_file), "r", encoding="utf-8") as f:
            data = f.read()
        if int(scale) != 1:
            data = data.replace(
                str(rows_in_init_env), str(rows_in_init_env * int(scale))
            )
        await db_conn.execute(data)


class BasicUnitTest:
    async def init_env(self):
        if "init_env" in passed_stages:
            print("init_env already called")
            res = PgAnonResult()
            res.result_code = ResultCode.DONE
            return res

        options = build_run_options([
            f"--db-host={params.test_db_host}",
            f"--db-name=postgres",
            f"--db-user={params.test_db_user}",
            f"--db-port={params.test_db_port}",
            f"--db-user-password={params.test_db_user_password}",
            f"--config={params.test_config}",
            "--mode=init",
            "--debug",
        ])

        ctx = Context(options)

        db_conn = await create_connection(ctx.connection_params)
        await DBOperations.init_db(db_conn, params.test_source_db)
        await DBOperations.init_db(db_conn, params.test_source_db + "_2")
        await DBOperations.init_db(db_conn, params.test_target_db)
        await DBOperations.init_db(db_conn, params.test_target_db + "_2")
        await DBOperations.init_db(db_conn, params.test_target_db + "_3")
        await DBOperations.init_db(db_conn, params.test_target_db + "_4")
        await DBOperations.init_db(db_conn, params.test_target_db + "_5")
        await DBOperations.init_db(db_conn, params.test_target_db + "_6")
        await DBOperations.init_db(db_conn, params.test_target_db + "_7")  # for PGAnonValidateUnitTest 04
        await DBOperations.init_db(db_conn, params.test_target_db + "_8")  # for PGAnonValidateUnitTest 05
        await DBOperations.init_db(db_conn, params.test_target_db + "_9")  # for PGAnonUnitTest 11
        await db_conn.close()

        source_db_params = copy.copy(ctx.connection_params)
        source_db_params.database = params.test_source_db

        print("============> Started init_env")
        db_conn = await create_connection(source_db_params)
        await DBOperations.init_env(db_conn, "init_env.sql", params.test_scale)
        await db_conn.close()
        print("<============ Finished init_env")

        options = build_run_options([
            f"--db-host={params.test_db_host}",
            f"--db-name={params.test_source_db}",
            f"--db-user={params.test_db_user}",
            f"--db-port={params.test_db_port}",
            f"--db-user-password={params.test_db_user_password}",
            f"--config={params.test_config}",
            "--mode=init",
            "--debug",
        ])

        res = await PgAnonApp(options).run()
        if res.result_code != ResultCode.DONE:
            raise ValueError("Init env failed")
        passed_stages.append("init_env")
        return res

    async def init_stress_env(self):
        if "init_stress_env" in passed_stages:
            print("init_stress_env already called")
            res = PgAnonResult()
            res.result_code = ResultCode.DONE
            return res

        options = build_run_options([
            f"--db-host={params.test_db_host}",
            "--db-name=postgres",
            f"--db-user={params.test_db_user}",
            f"--db-port={params.test_db_port}",
            f"--db-user-password={params.test_db_user_password}",
            f"--config={params.test_config}",
            "--mode=init",
            "--debug",
        ])

        ctx = Context(options)

        db_conn = await create_connection(ctx.connection_params)
        await DBOperations.init_db_once(db_conn, params.test_source_db + "_stress")

        source_db_params = copy.copy(ctx.connection_params)
        source_db_params.database = params.test_source_db + "_stress"

        options = build_run_options([
            f"--db-host={params.test_db_host}",
            f"--db-name={params.test_source_db}_stress",
            f"--db-user={params.test_db_user}",
            f"--db-port={params.test_db_port}",
            f"--db-user-password={params.test_db_user_password}",
            f"--config={params.test_config}",
            "--mode=init",
            "--debug",
        ])

        res = await PgAnonApp(options).run()

        schema_exists = await db_conn.fetch(
            """select nspname from pg_namespace where nspname = 'stress'"""
        )
        await db_conn.close()
        if len(schema_exists) == 0:
            print("============> Started init_stress_env")
            db_conn = await create_connection(source_db_params)
            await DBOperations.init_env(db_conn, "init_stress_env.sql", params.test_scale)
            await db_conn.close()
            print("<============ Finished init_stress_env")
        else:
            print("============> Schema 'stress' already exists")

        if res.result_code != ResultCode.DONE:
            raise ValueError("Init stress env failed")

        passed_stages.append("init_stress_env")
        return res

    async def check_rows(self, options, schema, table, fields, rows):
        ctx = Context(options)
        db_conn = await create_connection(ctx.connection_params)
        if fields is None:
            db_rows = await db_conn.fetch(
                """select * from "%s"."%s" limit 10000""" % (schema, table)
            )
        else:
            db_rows = await db_conn.fetch(
                """select %s from "%s"."%s" limit 10000"""
                % (", ".join(fields), schema, table)
            )
        db_rows_prepared = []
        for db_row in db_rows:
            db_row_prepared = []
            for _, v in dict(db_row).items():
                db_row_prepared.append(v)
            db_rows_prepared.append(db_row_prepared)

        def cmp_two_rows(row_a, row_b):
            result = True
            if len(db_row) == len(v):
                for i in range(len(db_row)):
                    if row_a[i] != row_b[i] and row_a[i] != "*":
                        return False
            return result

        result = True
        for v in rows:
            found = False
            for db_row in db_rows_prepared:
                if cmp_two_rows(v, db_row):
                    found = True
                    break
            if not found:
                print("check_rows: row %s not found" % str(v))
                await db_conn.close()
                result = False

        if not result:
            print("========================================")
            print("Following data exists:")
            for i, v in enumerate(db_rows_prepared):
                if i < 10:
                    print(str(v))

        await db_conn.close()
        return result

    async def check_list_tables_and_fields(self, source_options, target_options):
        query = """
        SELECT 
            n.nspname,
            c.relname,
            a.attname AS column_name
        FROM pg_class c
        JOIN pg_namespace n on c.relnamespace = n.oid
        JOIN pg_attribute a ON a.attrelid = c.oid
        JOIN pg_type t ON a.atttypid = t.oid
        LEFT JOIN pg_index i ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE
            a.attnum > 0
            AND c.relkind IN ('r', 'p')
            AND a.atttypid = t.oid
            AND n.nspname not in ('pg_catalog', 'information_schema', 'pg_toast')
        ORDER BY 1, 2, a.attnum
        """

        ctx = Context(source_options)
        db_conn = await create_connection(ctx.connection_params)
        db_source_rows = recordset_to_list_flat(await db_conn.fetch(query))
        await db_conn.close()

        ctx = Context(target_options)
        db_conn = await create_connection(ctx.connection_params)
        db_target_rows = recordset_to_list_flat(await db_conn.fetch(query))
        await db_conn.close()

        not_found_in_target = [x for x in db_source_rows if x not in db_target_rows]
        # not_found_in_source = [x for x in db_target_rows if x not in db_source_rows]

        return not_found_in_target

    async def check_rows_count(self, options, objs) -> bool:
        failed_objs = []
        ctx = Context(options)
        db_conn = await create_connection(ctx.connection_params)

        for obj in objs:
            try:
                db_rows = await db_conn.fetch(
                    """select count(1) from "%s"."%s" """ % (obj[0], obj[1])
                )
                if db_rows[0][0] != obj[2]:
                    failed_objs.append(obj)
                    print(
                        "check_rows_count: failed check %s, count is %d"
                        % (str(obj), db_rows[0][0])
                    )
            except:
                print(exception_helper(show_traceback=True))
                failed_objs.append(obj)
                print("check_rows_count: failed check %s" % (str(obj)))

        await db_conn.close()
        return len(failed_objs) == 0

    async def check_list_tables(self, options, expected_tables_list) -> bool:
        query = """
            SELECT 
                n.nspname,
                c.relname
            FROM pg_class c
            JOIN pg_namespace n on c.relnamespace = n.oid
            WHERE
                c.relkind IN ('r', 'p')
                AND n.nspname not in ('pg_catalog', 'information_schema', 'pg_toast')
            ORDER BY 1, 2
        """

        ctx = Context(options)
        db_conn = await create_connection(ctx.connection_params)
        db_rows = recordset_to_list_flat(await db_conn.fetch(query))
        await db_conn.close()

        not_found_tables = [x for x in expected_tables_list if x not in db_rows]
        for v in not_found_tables:
            print("check_list_tables: Table %s not found!" % str(v))
        for v in [x for x in db_rows if x not in expected_tables_list]:
            print("check_list_tables: Found unexpected table %s!" % str(v))

        return len(not_found_tables) == 0 and len(db_rows) == len(expected_tables_list)

    async def get_list_tables_with_diff_data(self, source_options, target_options):
        query = """
        SELECT 
            n.nspname,
            c.relname,
            a.attname AS column_name
        FROM pg_class c
        JOIN pg_namespace n on c.relnamespace = n.oid
        JOIN pg_attribute a ON a.attrelid = c.oid
        JOIN pg_type t ON a.atttypid = t.oid
        LEFT JOIN pg_index i ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE
            a.attnum > 0
            AND c.relkind IN ('r', 'p')
            AND a.atttypid = t.oid
            AND n.nspname not in ('pg_catalog', 'information_schema', 'pg_toast')
        ORDER BY 1, 2, a.attnum
        """

        ctx = Context(source_options)
        db_conn = await create_connection(ctx.connection_params)
        db_source_rows = recordset_to_list_flat(await db_conn.fetch(query))
        for row in db_source_rows:
            data_query = """SELECT "%s" FROM "%s"."%s" LIMIT 5""" % (
                row[2],
                row[0],
                row[1],
            )
            vals = recordset_to_list_flat(await db_conn.fetch(data_query))
            row.append(vals)
        await db_conn.close()

        ctx = Context(target_options)
        db_conn = await create_connection(ctx.connection_params)
        db_target_rows = recordset_to_list_flat(await db_conn.fetch(query))
        for row in db_target_rows:
            data_query = """SELECT "%s" FROM "%s"."%s" LIMIT 5""" % (
                row[2],
                row[0],
                row[1],
            )
            vals = recordset_to_list_flat(await db_conn.fetch(data_query))
            row.append(vals)
        await db_conn.close()

        target_tables = [x for x in db_target_rows if x not in db_source_rows]
        source_tables = [x for x in db_source_rows if x not in db_target_rows]
        # not_found_in_source = [x for x in db_target_rows if x not in db_source_rows]

        return source_tables, target_tables

    @property
    def test_expected_results_path(self) -> str:
        return os.path.join(os.getcwd(), 'tests', 'expected_results')

    @staticmethod
    def get_test_dict_path(dict_name: str, output: bool = False) -> str:
        type = 'output' if output else 'input'
        return os.path.join(os.getcwd(), 'tests', f'{type}_dict', dict_name)

    def get_test_expected_dict_path(self, dict_name: str) -> str:
        return os.path.join(self.test_expected_results_path, dict_name)

    @staticmethod
    def get_test_output_path(dir_name: str) -> str:
        return os.path.join(os.getcwd(), 'tests', 'output', dir_name)

    def save_and_compare_result(self, file_name, list_objects):
        saved_results_file_path = os.path.join(os.getcwd(), 'tests', 'saved_results')
        os.makedirs(saved_results_file_path, exist_ok=True)
        saved_results_file = os.path.join(saved_results_file_path, file_name)

        expected_results_file = os.path.join(self.test_expected_results_path, file_name + '.result')
        to_json(list_objects, formatted=True)

        with open(saved_results_file, "w") as out_file:
            out_file.write(to_json(list_objects, formatted=True))

        try:
            orig_data = open(expected_results_file, "r")
            orig_content = orig_data.read()
            orig_data.close()
            orig_content_obj = eval(orig_content)
        except:
            print(f"Needs to create {expected_results_file} with content: {to_json(list_objects, formatted=True)}")

        try:
            current_data = open(saved_results_file, "r")
            current_content = current_data.read()
            current_data.close()
            current_content_obj = eval(current_content)
        except:
            pass

        cmp = orig_content_obj == current_content_obj
        if not cmp:
            print(str(orig_content_obj))
            print("!=")
            print(str(current_content_obj))

        return cmp


class PGAnonArgumentsValidationUnitTest(unittest.IsolatedAsyncioTestCase, BasicUnitTest):

    def test_all_list_arguments_are_empty(self):
        options = build_run_options([
            f"--db-host={params.test_db_host}",
            f"--db-name={params.test_source_db}",
            f"--db-user={params.test_db_user}",
            f"--db-port={params.test_db_port}",
            f"--db-user-password={params.test_db_user_password}",
            f"--config={params.test_config}",
            f"--mode=create-dict",
        ])
        self.assertIsNone(options.meta_dict_files)
        self.assertIsNone(options.prepared_sens_dict_files)
        self.assertIsNone(options.prepared_no_sens_dict_files)

    def test_all_list_arguments_filled_with_simple_value(self):
        meta_dict_file_name = self.get_test_dict_path('test.py')
        prepared_sens_dict_file_name = self.get_test_dict_path('prepared_sens_dict_file.py')
        prepared_no_sens_dict_file_name = self.get_test_dict_path('prepared_no_sens_dict_file.py')

        options = build_run_options([
            f"--db-host={params.test_db_host}",
            f"--db-name={params.test_source_db}",
            f"--db-user={params.test_db_user}",
            f"--db-port={params.test_db_port}",
            f"--db-user-password={params.test_db_user_password}",
            f"--config={params.test_config}",
            f"--meta-dict-file={meta_dict_file_name}",
            f"--prepared-sens-dict-file={prepared_sens_dict_file_name}",
            f"--prepared-no-sens-dict-file={prepared_no_sens_dict_file_name}",
        ])
        self.assertEqual([meta_dict_file_name], options.meta_dict_files)
        self.assertEqual([prepared_sens_dict_file_name], options.prepared_sens_dict_files)
        self.assertEqual([prepared_no_sens_dict_file_name], options.prepared_no_sens_dict_files)

    def test_all_list_arguments_filled_with_list_values(self):
        meta_dict_file_names = [
            self.get_test_dict_path('test.py'),
            self.get_test_dict_path('meta_dict_file.py'),
        ]
        prepared_sens_dict_file_names = [
            self.get_test_dict_path('prepared_sens_dict_file.py'),
            self.get_test_dict_path('another_prepared_sens_dict_file.py'),
        ]
        prepared_no_sens_dict_file_names = [
            self.get_test_dict_path('prepared_no_sens_dict_file.py'),
            self.get_test_dict_path('another_prepared_no_sens_dict_file.py'),
        ]

        options = build_run_options([
            f"--db-host={params.test_db_host}",
            f"--db-name={params.test_source_db}",
            f"--db-user={params.test_db_user}",
            f"--db-port={params.test_db_port}",
            f"--db-user-password={params.test_db_user_password}",
            f"--config={params.test_config}",
            f"--meta-dict-file={','.join(meta_dict_file_names)}",
            f"--prepared-sens-dict-file={','.join(prepared_sens_dict_file_names)}",
            f"--prepared-no-sens-dict-file={','.join(prepared_no_sens_dict_file_names)}",
        ])
        self.assertEqual(meta_dict_file_names, options.meta_dict_files)
        self.assertEqual(prepared_sens_dict_file_names, options.prepared_sens_dict_files)
        self.assertEqual(prepared_no_sens_dict_file_names, options.prepared_no_sens_dict_files)


class PGAnonUnitTest(unittest.IsolatedAsyncioTestCase, BasicUnitTest):
    async def test_01_init(self):
        res = await self.init_env()
        self.assertEqual(res.result_code, ResultCode.DONE)

    async def test_02_dump(self):
        self.assertTrue("init_env" in passed_stages)

        prepared_sens_dict_file = self.get_test_dict_path("test.py")
        dict_file_name = get_file_name_from_path(prepared_sens_dict_file)
        output_dir = self.get_test_output_path(dict_file_name)

        options = build_run_options([
            f"--db-host={params.test_db_host}",
            f"--db-name={params.test_source_db}",
            f"--db-user={params.test_db_user}",
            f"--db-port={params.test_db_port}",
            f"--db-user-password={params.test_db_user_password}",
            f"--config={params.test_config}",
            "--mode=dump",
            f"--prepared-sens-dict-file={prepared_sens_dict_file}",
            f"--output-dir={output_dir}",
            f"--processes={params.test_processes}",
            f"--db-connections-per-process={params.db_connections_per_process}",
            "--clear-output-dir",
            "--debug",
        ])

        res = await PgAnonApp(options).run()
        self.assertEqual(res.result_code, ResultCode.DONE)
        passed_stages.append("test_02_dump")

    async def test_03_restore(self):
        self.assertTrue("test_02_dump" in passed_stages)

        input_dir = self.get_test_output_path("test")

        options = build_run_options([
            f"--db-host={params.test_db_host}",
            f"--db-name={params.test_target_db}",
            f"--db-user={params.test_db_user}",
            f"--db-port={params.test_db_port}",
            f"--db-user-password={params.test_db_user_password}",
            f"--config={params.test_config}",
            "--mode=restore",
            f"--db-connections-per-process={params.db_connections_per_process}",
            f"--input-dir={input_dir}",
            "--drop-custom-check-constr",
            "--debug",
        ])

        res = await PgAnonApp(options).run()
        self.assertEqual(res.result_code, ResultCode.DONE)
        passed_stages.append("test_03_restore")

    async def test_04_dump(self):
        self.assertTrue("init_env" in passed_stages)

        prepared_sens_dict_file = self.get_test_dict_path("test_exclude.py")
        dict_file_name = get_file_name_from_path(prepared_sens_dict_file)
        output_dir = self.get_test_output_path(dict_file_name)

        options = build_run_options([
            f"--db-host={params.test_db_host}",
            f"--db-name={params.test_source_db}",
            f"--db-user={params.test_db_user}",
            f"--db-port={params.test_db_port}",
            f"--db-user-password={params.test_db_user_password}",
            f"--config={params.test_config}",
            "--mode=dump",
            f"--prepared-sens-dict-file={prepared_sens_dict_file}",
            f"--output-dir={output_dir}",
            f"--processes={params.test_processes}",
            f"--db-connections-per-process={params.db_connections_per_process}",
            "--clear-output-dir",
            "--debug",
        ])

        res = await PgAnonApp(options).run()
        self.assertEqual(res.result_code, ResultCode.DONE)
        passed_stages.append("test_04_dump")

    async def test_05_restore(self):
        self.assertTrue("test_04_dump" in passed_stages)
        prepared_sens_dict_file = self.get_test_dict_path("test_exclude.py")
        
        input_dir = self.get_test_output_path("test_exclude")

        options = build_run_options([
            f"--db-host={params.test_db_host}",
            f"--db-name={params.test_target_db}_2",
            f"--db-user={params.test_db_user}",
            f"--db-port={params.test_db_port}",
            f"--db-user-password={params.test_db_user_password}",
            f"--config={params.test_config}",
            "--mode=restore",
            f"--db-connections-per-process={params.db_connections_per_process}",
            f"--input-dir={input_dir}",
            "--drop-custom-check-constr",
            "--debug",
        ])

        res = await PgAnonApp(options).run()
        self.assertEqual(res.result_code, ResultCode.DONE)

        options = build_run_options([
            f"--db-host={params.test_db_host}",
            f"--db-name={params.test_target_db}_2",
            f"--db-user={params.test_db_user}",
            f"--db-port={params.test_db_port}",
            f"--db-user-password={params.test_db_user_password}",
            f"--config={params.test_config}",
            f"--db-connections-per-process={params.db_connections_per_process}",
            f"--prepared-sens-dict-file={prepared_sens_dict_file}",
            "--debug",
        ])
        res = await PgAnonApp(options).validate_target_tables()
        self.assertEqual(res.result_code, ResultCode.DONE)
        passed_stages.append("test_05_restore")

    async def test_06_sync_struct(self):
        self.assertTrue("test_05_restore" in passed_stages)

        prepared_sens_dict_file = self.get_test_dict_path("test_sync_struct.py")
        dict_file_name = get_file_name_from_path(prepared_sens_dict_file)
        output_dir = self.get_test_output_path(dict_file_name)

        options = build_run_options([
            f"--db-host={params.test_db_host}",
            f"--db-name={params.test_source_db}",
            f"--db-user={params.test_db_user}",
            f"--db-port={params.test_db_port}",
            f"--db-user-password={params.test_db_user_password}",
            f"--config={params.test_config}",
            "--mode=sync-struct-dump",
            f"--processes={params.test_processes}",
            f"--db-connections-per-process={params.db_connections_per_process}",
            f"--prepared-sens-dict-file={prepared_sens_dict_file}",
            f"--output-dir={output_dir}",
            "--clear-output-dir",
            "--debug",
        ])

        res = await PgAnonApp(options).run()
        self.assertEqual(res.result_code, ResultCode.DONE)

        options = build_run_options([
            f"--db-host={params.test_db_host}",
            f"--db-name={params.test_target_db}_3",
            f"--db-user={params.test_db_user}",
            f"--db-port={params.test_db_port}",
            f"--db-user-password={params.test_db_user_password}",
            f"--config={params.test_config}",
            "--mode=sync-struct-restore",
            f"--db-connections-per-process={params.db_connections_per_process}",
            f"--input-dir={output_dir}",
            "--debug",
        ])

        res = await PgAnonApp(options).run()
        self.assertEqual(res.result_code, ResultCode.DONE)

        self.assertTrue(
            await self.check_list_tables(
                options,
                [
                    ["_SCHM.$complex#имя;@&* a'", "_TBL.$complex#имя;@&* a'"],
                    ["_SCHM.$complex#имя;@&* a'", "_TBL.$complex#имя;@&* a'2"],
                    ["_SCHM.$complex#имя;@&* a'", "_TBL.$complex#имя;@&* a'3"],
                    ['public', 'contracts'],
                    ['public', 'inn_info'],
                    ['public', 'key_value'],
                    ['public', 'tbl_100'],
                    ['public', 'tbl_constants'],
                    ['schm_customer', 'customer_company'],
                    ['schm_customer', 'customer_manager'],
                    ['schm_mask_exclude_1', 'other_tbl'],
                    ['schm_mask_exclude_1', 'some_tbl'],
                    ['schm_mask_ext_exclude_2', 'card_numbers'],
                    ['schm_mask_ext_exclude_2', 'other_ext_tbl_2'],
                    ['schm_mask_ext_exclude_2', 'some_ext_tbl'],
                    ['schm_mask_ext_include_2', 'other_ext_tbl'],
                    ['schm_mask_ext_include_2', 'some_ext_tbl'],
                    ['schm_mask_include_1', 'other_tbl'],
                    ['schm_mask_include_1', 'some_tbl'],
                    ["schm_mask_include_1", "tbl_123"],
                    ['schm_mask_include_1', 'tbl_123_456'],
                    ['schm_other_1', 'some_tbl'],
                    ["schm_other_2", "exclude_tbl"],
                    ["schm_other_2", "some_tbl"],
                    ['schm_other_2', 'tbl_test_anon_functions'],
                    ['schm_other_3', 'some_tbl'],
                    ['schm_other_4', 'partitioned_table'],
                    ['schm_other_4', 'partitioned_table_2025_01'],
                    ['schm_other_4', 'partitioned_table_2025_02'],
                    ['schm_other_4', 'partitioned_table_2025_03'],
                    ['schm_other_4', 'partitioned_table_default'],
                ],
            )
        )

        objs = [
            ["_SCHM.$complex#имя;@&* a'", "_TBL.$complex#имя;@&* a'", 0],
            ["_SCHM.$complex#имя;@&* a'", "_TBL.$complex#имя;@&* a'2", 0],
            ["_SCHM.$complex#имя;@&* a'", "_TBL.$complex#имя;@&* a'3", 0],
            ['public', 'contracts', 0],
            ['public', 'inn_info', 0],
            ['public', 'key_value', 0],
            ['public', 'tbl_100', 0],
            ['public', 'tbl_constants', 0],
            ['schm_customer', 'customer_company', 0],
            ['schm_customer', 'customer_manager', 0],
            ['schm_mask_exclude_1', 'other_tbl', 0],
            ['schm_mask_exclude_1', 'some_tbl', 0],
            ['schm_mask_ext_exclude_2', 'card_numbers', 0],
            ['schm_mask_ext_exclude_2', 'other_ext_tbl_2', 0],
            ['schm_mask_ext_exclude_2', 'some_ext_tbl', 0],
            ['schm_mask_ext_include_2', 'other_ext_tbl', 0],
            ['schm_mask_ext_include_2', 'some_ext_tbl', 0],
            ['schm_mask_include_1', 'other_tbl', 0],
            ['schm_mask_include_1', 'some_tbl', 0],
            ["schm_mask_include_1", "tbl_123", 0],
            ['schm_mask_include_1', 'tbl_123_456', 0],
            ['schm_other_1', 'some_tbl', 0],
            ["schm_other_2", "exclude_tbl", 0],
            ["schm_other_2", "some_tbl", 0],
            ['schm_other_2', 'tbl_test_anon_functions', 0],
            ['schm_other_3', 'some_tbl', 0],
            ['schm_other_4', 'partitioned_table', 0],
            ['schm_other_4', 'partitioned_table_2025_01', 0],
            ['schm_other_4', 'partitioned_table_2025_02', 0],
            ['schm_other_4', 'partitioned_table_2025_03', 0],
            ['schm_other_4', 'partitioned_table_default', 0],
        ]
        self.assertTrue(await self.check_rows_count(options, objs))

        self.assertEqual(res.result_code, ResultCode.DONE)
        passed_stages.append("test_06_sync_struct")

    async def test_07_sync_data(self):
        self.assertTrue("test_06_sync_struct" in passed_stages)

        prepared_sens_dict_file = self.get_test_dict_path("test_sync_data.py")
        dict_file_name = get_file_name_from_path(prepared_sens_dict_file)
        output_dir = self.get_test_output_path(dict_file_name)

        options = build_run_options([
                f"--db-host={params.test_db_host}",
                f"--db-name={params.test_source_db}",
                f"--db-user={params.test_db_user}",
                f"--db-port={params.test_db_port}",
                f"--db-user-password={params.test_db_user_password}",
                f"--config={params.test_config}",
                "--mode=sync-data-dump",
                f"--processes={params.test_processes}",
                f"--db-connections-per-process={params.db_connections_per_process}",
                f"--prepared-sens-dict-file={prepared_sens_dict_file}",
                f"--output-dir={output_dir}",
                "--clear-output-dir",
                "--debug",
            ]
        )

        res = await PgAnonApp(options).run()
        self.assertEqual(res.result_code, ResultCode.DONE)

        options = build_run_options([
            f"--db-host={params.test_db_host}",
            f"--db-name={params.test_target_db}_3",  # here target DB have 3 empty tables
            f"--db-user={params.test_db_user}",
            f"--db-port={params.test_db_port}",
            f"--db-user-password={params.test_db_user_password}",
            f"--config={params.test_config}",
            "--mode=sync-data-restore",
            f"--db-connections-per-process={params.db_connections_per_process}",
            f"--input-dir={output_dir}",
            "--debug",
        ])

        res = await PgAnonApp(options).run()
        self.assertEqual(res.result_code, ResultCode.DONE)

        self.assertTrue(
            await self.check_list_tables(
                options,
                [
                    ["_SCHM.$complex#имя;@&* a'", "_TBL.$complex#имя;@&* a'"],
                    ["_SCHM.$complex#имя;@&* a'", "_TBL.$complex#имя;@&* a'2"],
                    ["_SCHM.$complex#имя;@&* a'", "_TBL.$complex#имя;@&* a'3"],
                    ['public', 'contracts'],
                    ['public', 'inn_info'],
                    ['public', 'key_value'],
                    ['public', 'tbl_100'],
                    ['public', 'tbl_constants'],
                    ['schm_customer', 'customer_company'],
                    ['schm_customer', 'customer_manager'],
                    ['schm_mask_exclude_1', 'other_tbl'],
                    ['schm_mask_exclude_1', 'some_tbl'],
                    ['schm_mask_ext_exclude_2', 'card_numbers'],
                    ['schm_mask_ext_exclude_2', 'other_ext_tbl_2'],
                    ['schm_mask_ext_exclude_2', 'some_ext_tbl'],
                    ['schm_mask_ext_include_2', 'other_ext_tbl'],
                    ['schm_mask_ext_include_2', 'some_ext_tbl'],
                    ['schm_mask_include_1', 'other_tbl'],
                    ['schm_mask_include_1', 'some_tbl'],
                    ["schm_mask_include_1", "tbl_123"],
                    ['schm_mask_include_1', 'tbl_123_456'],
                    ['schm_other_1', 'some_tbl'],
                    ["schm_other_2", "exclude_tbl"],
                    ["schm_other_2", "some_tbl"],
                    ['schm_other_2', 'tbl_test_anon_functions'],
                    ['schm_other_3', 'some_tbl'],
                    ['schm_other_4', 'partitioned_table'],
                    ['schm_other_4', 'partitioned_table_2025_01'],
                    ['schm_other_4', 'partitioned_table_2025_02'],
                    ['schm_other_4', 'partitioned_table_2025_03'],
                    ['schm_other_4', 'partitioned_table_default'],
                ],
            )
        )

        objs = [
            ["schm_other_2", "exclude_tbl", rows_in_init_env * int(params.test_scale)],
            ["schm_other_2", "some_tbl", rows_in_init_env * int(params.test_scale)],
            ["schm_mask_include_1", "tbl_123", rows_in_init_env * int(params.test_scale)],
        ]
        self.assertTrue(await self.check_rows_count(options, objs))

        rows = [[3, "t***l_3"], [4, "t***l_4"]]
        self.assertTrue(
            await self.check_rows(options, "schm_mask_include_1", "tbl_123", None, rows)
        )

        self.assertEqual(res.result_code, ResultCode.DONE)
        passed_stages.append("test_07_sync_data")

    async def test_08_sync_data(self):
        # --mode=sync-data-dump ---> --mode=sync-data-restore [target DB is not empty]
        self.assertTrue("test_07_sync_data" in passed_stages)

        prepared_sens_dict_file = self.get_test_dict_path("test_sync_data_2.py")
        dict_file_name = get_file_name_from_path(prepared_sens_dict_file)
        output_dir = self.get_test_output_path(dict_file_name)

        options = build_run_options([
                f"--db-host={params.test_db_host}",
                f"--db-name={params.test_source_db}",
                f"--db-user={params.test_db_user}",
                f"--db-port={params.test_db_port}",
                f"--db-user-password={params.test_db_user_password}",
                f"--config={params.test_config}",
                "--mode=sync-data-dump",
                f"--processes={params.test_processes}",
                f"--db-connections-per-process={params.db_connections_per_process}",
                f"--prepared-sens-dict-file={prepared_sens_dict_file}",
                f"--output-dir={output_dir}",
                "--clear-output-dir",
                "--debug",
            ]
        )

        res = await PgAnonApp(options).run()
        self.assertEqual(res.result_code, ResultCode.DONE)

        options = build_run_options([
            f"--db-host={params.test_db_host}",
            f"--db-name={params.test_target_db}",  # here target DB is NOT empty
            f"--db-user={params.test_db_user}",
            f"--db-port={params.test_db_port}",
            f"--db-user-password={params.test_db_user_password}",
            f"--config={params.test_config}",
            "--mode=sync-data-restore",  # just sync data of specific tables from test_sync_data_2.py
            f"--db-connections-per-process={params.db_connections_per_process}",
            f"--input-dir={output_dir}",
            "--debug",
        ])

        ctx = Context(options)
        db_conn = await create_connection(ctx.connection_params)
        # pg_anon does not clear tables on its own
        await db_conn.execute("TRUNCATE TABLE schm_other_1.some_tbl")  # manual clean
        await db_conn.execute("TRUNCATE TABLE schm_other_2.some_tbl")
        await db_conn.close()

        res = await PgAnonApp(options).run()
        self.assertEqual(res.result_code, ResultCode.DONE)

        objs = [
            ["schm_other_1", "some_tbl", rows_in_init_env * int(params.test_scale)],
            ["schm_other_2", "some_tbl", rows_in_init_env * int(params.test_scale)],
        ]
        self.assertTrue(await self.check_rows_count(options, objs))

        self.assertEqual(res.result_code, ResultCode.DONE)
        passed_stages.append("test_08_sync_data")

    async def test_09_repeat_restore_in_existing_db(self):
        self.assertTrue("test_03_restore" in passed_stages)

        input_dir = self.get_test_output_path("test")

        options = build_run_options([
            f"--db-host={params.test_db_host}",
            f"--db-name={params.test_target_db}",
            f"--db-user={params.test_db_user}",
            f"--db-port={params.test_db_port}",
            f"--db-user-password={params.test_db_user_password}",
            f"--config={params.test_config}",
            "--mode=restore",
            f"--db-connections-per-process={params.db_connections_per_process}",
            f"--input-dir={input_dir}",
            "--drop-custom-check-constr",
            "--debug",
        ])

        res = await PgAnonApp(options).run()
        self.assertEqual(res.result_code, ResultCode.FAIL)
        passed_stages.append("test_09_repeat_restore_in_existing_db")

    async def test_10_repeat_restore_with_drop_db(self):
        self.assertTrue("test_03_restore" in passed_stages)

        input_dir = self.get_test_output_path("test")

        options = build_run_options([
            f"--db-host={params.test_db_host}",
            f"--db-name={params.test_target_db}",
            f"--db-user={params.test_db_user}",
            f"--db-port={params.test_db_port}",
            f"--db-user-password={params.test_db_user_password}",
            f"--config={params.test_config}",
            "--mode=restore",
            f"--db-connections-per-process={params.db_connections_per_process}",
            f"--input-dir={input_dir}",
            "--drop-custom-check-constr",
            "--drop-db",
            "--debug",
        ])

        res = await PgAnonApp(options).run()
        self.assertEqual(res.result_code, ResultCode.DONE)
        passed_stages.append("test_10_repeat_restore_with_drop_db")


class PGAnonRestoreCleanTest(unittest.IsolatedAsyncioTestCase, BasicUnitTest):
    test_source_db = f"{params.test_source_db}_2"
    test_target_db = f"{params.test_target_db}_9"

    async def init_db(self, db_name: str, scale: int = 1):
        connection_params = ConnectionParams(
            host=params.test_db_host,
            port=int(params.test_db_port),
            database=db_name,
            user=params.test_db_user,
            password=params.test_db_user_password,
        )
        db_conn = await create_connection(connection_params)
        await DBOperations.init_env(db_conn, "init_simple_env.sql", scale)
        await db_conn.close()

        options = build_run_options([
            f"--db-host={params.test_db_host}",
            f"--db-name={db_name}",
            f"--db-user={params.test_db_user}",
            f"--db-port={params.test_db_port}",
            f"--db-user-password={params.test_db_user_password}",
            f"--config={params.test_config}",
            "--mode=init",
            "--debug",
        ])

        res = await PgAnonApp(options).run()
        self.assertEqual(res.result_code, ResultCode.DONE)

    async def add_data_to_db(self, db_name: str, scale: int = 1):
        connection_params = ConnectionParams(
            host=params.test_db_host,
            port=int(params.test_db_port),
            database=db_name,
            user=params.test_db_user,
            password=params.test_db_user_password,
        )
        db_conn = await create_connection(connection_params)
        await DBOperations.init_env(db_conn, "init_additional_simple_env.sql", scale)
        await db_conn.close()

    async def make_dump(self, db_name: str):
        prepared_sens_dict_file = self.get_test_dict_path("test_empty_dictionary.py")
        dict_file_name = get_file_name_from_path(prepared_sens_dict_file)
        output_dir = self.get_test_output_path(dict_file_name)

        options = build_run_options([
            f"--db-host={params.test_db_host}",
            f"--db-name={db_name}",
            f"--db-user={params.test_db_user}",
            f"--db-port={params.test_db_port}",
            f"--db-user-password={params.test_db_user_password}",
            f"--config={params.test_config}",
            "--mode=dump",
            f"--prepared-sens-dict-file={prepared_sens_dict_file}",
            f"--output-dir={output_dir}",
            f"--processes={params.test_processes}",
            f"--db-connections-per-process={params.db_connections_per_process}",
            "--clear-output-dir",
            "--debug",
        ])

        res = await PgAnonApp(options).run()
        self.assertEqual(res.result_code, ResultCode.DONE)

    async def test_01_init(self):
        res = await self.init_env()
        self.assertEqual(res.result_code, ResultCode.DONE)

    async def test_02_dump_and_restore_with_clean_db(self):
        self.assertTrue("init_env" in passed_stages)

        restore_options = build_run_options([
            f"--db-host={params.test_db_host}",
            f"--db-name={self.test_target_db}",
            f"--db-user={params.test_db_user}",
            f"--db-port={params.test_db_port}",
            f"--db-user-password={params.test_db_user_password}",
            f"--config={params.test_config}",
            "--mode=restore",
            f"--db-connections-per-process={params.db_connections_per_process}",
            f"--input-dir={self.get_test_output_path('test_empty_dictionary')}",
            "--drop-custom-check-constr",
            "--clean-db",
            "--debug",
        ])

        await self.init_db(self.test_source_db)
        await self.make_dump(self.test_source_db)

        res = await PgAnonApp(restore_options).run()
        self.assertEqual(res.result_code, ResultCode.DONE)
        objs = [
            ["test_simple", "customer_company", rows_in_init_env],
            ["test_simple", "contracts", rows_in_init_env],
        ]
        self.assertTrue(await self.check_rows_count(restore_options, objs))

        rows_multiplier = 5
        await self.init_db(self.test_target_db, rows_multiplier)

        objs = [
            ["test_simple", "customer_company", rows_in_init_env * rows_multiplier],
            ["test_simple", "contracts", rows_in_init_env * rows_multiplier],
        ]
        self.assertTrue(await self.check_rows_count(restore_options, objs))

        res = await PgAnonApp(restore_options).run()
        self.assertEqual(res.result_code, ResultCode.DONE)
        objs = [
            ["test_simple", "customer_company", rows_in_init_env],
            ["test_simple", "contracts", rows_in_init_env],
        ]
        self.assertTrue(await self.check_rows_count(restore_options, objs))

        passed_stages.append("test_02_dump_and_restore_with_clean_db")

    async def test_03_dump_and_wrong_restore_with_clean_db(self):
        """
        Second restore must be failed, because in target DB will be added tables, what not include in dump
        """
        self.assertTrue("init_env" in passed_stages)

        restore_options = build_run_options([
            f"--db-host={params.test_db_host}",
            f"--db-name={self.test_target_db}",
            f"--db-user={params.test_db_user}",
            f"--db-port={params.test_db_port}",
            f"--db-user-password={params.test_db_user_password}",
            f"--config={params.test_config}",
            "--mode=restore",
            f"--db-connections-per-process={params.db_connections_per_process}",
            f"--input-dir={self.get_test_output_path('test_empty_dictionary')}",
            "--drop-custom-check-constr",
            "--clean-db",
            "--debug",
        ])

        await self.init_db(self.test_source_db)
        await self.make_dump(self.test_source_db)

        res = await PgAnonApp(restore_options).run()
        self.assertEqual(res.result_code, ResultCode.DONE)
        objs = [
            ["test_simple", "customer_company", rows_in_init_env],
            ["test_simple", "contracts", rows_in_init_env],
        ]
        self.assertTrue(await self.check_rows_count(restore_options, objs))

        rows_multiplier = 5
        await self.init_db(self.test_target_db, rows_multiplier)
        await self.add_data_to_db(self.test_target_db)

        objs = [
            ["test_simple", "customer_company", rows_in_init_env * rows_multiplier],
            ["test_simple", "contracts", rows_in_init_env * rows_multiplier],
            ["test_simple", "clients", rows_in_init_env],
            ["test_simple", "orders", rows_in_init_env],
        ]
        self.assertTrue(await self.check_rows_count(restore_options, objs))

        res = await PgAnonApp(restore_options).run()
        self.assertEqual(res.result_code, ResultCode.FAIL)

        passed_stages.append("test_03_dump_and_wrong_restore_with_clean_db")


class PGAnonValidateUnitTest(unittest.IsolatedAsyncioTestCase, BasicUnitTest):
    async def test_01_init(self):
        res = await self.init_env()
        self.assertEqual(res.result_code, ResultCode.DONE)

    async def test_02_sync_struct_for_validate(self):
        self.assertTrue("init_env" in passed_stages)

        prepared_sens_dict_file = self.get_test_dict_path("test_dbg_stages.py")
        output_dir = self.get_test_output_path("test_02_sync_struct_for_validate")

        options = build_run_options([
            f"--db-host={params.test_db_host}",
            f"--db-name={params.test_source_db}",
            f"--db-user={params.test_db_user}",
            f"--db-port={params.test_db_port}",
            f"--db-user-password={params.test_db_user_password}",
            f"--config={params.test_config}",
            "--mode=sync-struct-dump",
            f"--processes={params.test_processes}",
            f"--db-connections-per-process={params.db_connections_per_process}",
            f"--prepared-sens-dict-file={prepared_sens_dict_file}",
            "--clear-output-dir",
            "--debug",
            f"--output-dir={output_dir}",
            "--dbg-stage-3-validate-full"  # for not allowing post-data
        ])

        res_dump = await PgAnonApp(options).run()
        self.assertEqual(res_dump.result_code, ResultCode.DONE)

        options = build_run_options([
            f"--db-host={params.test_db_host}",
            f"--db-name={params.test_target_db}_7",
            f"--db-user={params.test_db_user}",
            f"--db-port={params.test_db_port}",
            f"--db-user-password={params.test_db_user_password}",
            f"--config={params.test_config}",
            "--mode=sync-struct-restore",
            f"--db-connections-per-process={params.db_connections_per_process}",
            f"--input-dir={output_dir}",
            "--debug",
        ])

        res_restore = await PgAnonApp(options).run()
        self.assertEqual(res_restore.result_code, ResultCode.DONE)
        passed_stages.append("test_02_sync_struct_for_validate")

    async def test_03_validate_dict(self):
        self.assertTrue("test_02_sync_struct_for_validate" in passed_stages)

        prepared_sens_dict_file = self.get_test_dict_path("test_dbg_stages.py")
        output_dir = self.get_test_output_path("test_03_validate_dict")

        options = build_run_options([
            f"--db-host={params.test_db_host}",
            f"--db-name={params.test_source_db}",
            f"--db-user={params.test_db_user}",
            f"--db-port={params.test_db_port}",
            f"--db-user-password={params.test_db_user_password}",
            f"--config={params.test_config}",
            "--mode=dump",
            f"--prepared-sens-dict-file={prepared_sens_dict_file}",
            f"--processes={params.test_processes}",
            f"--db-connections-per-process={params.db_connections_per_process}",
            "--clear-output-dir",
            "--debug",
            "--dbg-stage-1-validate-dict",
            f"--output-dir={output_dir}",
        ])

        res = await PgAnonApp(options).run()
        self.assertEqual(res.result_code, ResultCode.DONE)
        passed_stages.append("test_03_validate_dict")

    async def test_04_validate_data(self):
        self.assertTrue("test_03_validate_dict" in passed_stages)

        prepared_sens_dict_file = self.get_test_dict_path("test_dbg_stages.py")
        output_dir = self.get_test_output_path("test_04_validate_data")

        options = build_run_options([
            f"--db-host={params.test_db_host}",
            f"--db-name={params.test_source_db}",
            f"--db-user={params.test_db_user}",
            f"--db-port={params.test_db_port}",
            f"--db-user-password={params.test_db_user_password}",
            f"--config={params.test_config}",
            "--mode=dump",
            f"--prepared-sens-dict-file={prepared_sens_dict_file}",
            f"--processes={params.test_processes}",
            f"--db-connections-per-process={params.db_connections_per_process}",
            "--clear-output-dir",
            "--debug",
            "--dbg-stage-2-validate-data",
            f"--output-dir={output_dir}",
        ])

        res_dump = await PgAnonApp(options).run()
        self.assertEqual(res_dump.result_code, ResultCode.DONE)

        options = build_run_options([
            f"--db-host={params.test_db_host}",
            f"--db-name={params.test_target_db}_7",
            f"--db-user={params.test_db_user}",
            f"--db-port={params.test_db_port}",
            f"--db-user-password={params.test_db_user_password}",
            f"--config={params.test_config}",
            "--mode=sync-data-restore",
            f"--db-connections-per-process={params.db_connections_per_process}",
            "--debug",
            f"--input-dir={output_dir}",
        ])

        res_restore = await PgAnonApp(options).run()
        self.assertEqual(res_restore.result_code, ResultCode.DONE)

        passed_stages.append("test_04_validate_data")

    async def test_05_validate_full(self):
        self.assertTrue("test_04_validate_data" in passed_stages)
        
        prepared_sens_dict_file = self.get_test_dict_path("test_dbg_stages.py")
        output_dir = self.get_test_output_path("test_05_validate_full")

        options = build_run_options([
            f"--db-host={params.test_db_host}",
            f"--db-name={params.test_source_db}",
            f"--db-user={params.test_db_user}",
            f"--db-port={params.test_db_port}",
            f"--db-user-password={params.test_db_user_password}",
            f"--config={params.test_config}",
            "--mode=dump",
            f"--prepared-sens-dict-file={prepared_sens_dict_file}",
            f"--processes={params.test_processes}",
            f"--db-connections-per-process={params.db_connections_per_process}",
            "--clear-output-dir",
            "--debug",
            "--dbg-stage-3-validate-full",
            f"--output-dir={output_dir}",
        ])

        res_dump = await PgAnonApp(options).run()
        self.assertEqual(res_dump.result_code, ResultCode.DONE)

        options = build_run_options([
            f"--db-host={params.test_db_host}",
            f"--db-name={params.test_target_db}_8",
            f"--db-user={params.test_db_user}",
            f"--db-port={params.test_db_port}",
            f"--db-user-password={params.test_db_user_password}",
            f"--config={params.test_config}",
            "--mode=restore",
            f"--db-connections-per-process={params.db_connections_per_process}",
            "--debug",
            f"--input-dir={output_dir}",
        ])

        res_restore = await PgAnonApp(options).run()
        self.assertEqual(res_restore.result_code, ResultCode.DONE)

        passed_stages.append("test_05_validate_full")


class PGAnonPartialDumpRestoreUnitTest(unittest.IsolatedAsyncioTestCase, BasicUnitTest):
    async def test_01_init(self):
        res = await self.init_env()
        self.assertEqual(res.result_code, ResultCode.DONE)

    async def test_02_partial_dump_only_include(self):
        # self.assertTrue("init_env" in passed_stages) # TODO: [TTDB-1035] Uncomment it

        prepared_sens_dict_file = self.get_test_dict_path("test_empty_dictionary.py")
        partial_tables_dict_file = self.get_test_dict_path("test_partial_tables_dict.py")
        output_dir = self.get_test_output_path("test_02_partial_dump_only_include")

        options = build_run_options([
            f"--db-host={params.test_db_host}",
            f"--db-name={params.test_source_db}",
            f"--db-user={params.test_db_user}",
            f"--db-port={params.test_db_port}",
            f"--db-user-password={params.test_db_user_password}",
            f"--config={params.test_config}",
            "--mode=dump",
            f"--processes={params.test_processes}",
            f"--db-connections-per-process={params.db_connections_per_process}",
            f"--prepared-sens-dict-file={prepared_sens_dict_file}",
            f"--partial-tables-dict-files={partial_tables_dict_file}",
            "--clear-output-dir",
            "--debug",
            f"--output-dir={output_dir}",
        ])

        res_dump = await PgAnonApp(options).run()
        self.assertEqual(res_dump.result_code, ResultCode.DONE)

        passed_stages.append("test_02_partial_dump_only_include")

    async def test_03_partial_dump_only_exclude(self):
        self.assertTrue("test_02_partial_dump_only_include" in passed_stages)

        prepared_sens_dict_file = self.get_test_dict_path("test_empty_dictionary.py")
        partial_tables_exclude_dict_file = self.get_test_dict_path("test_partial_exclude_tables_dict.py")
        output_dir = self.get_test_output_path("test_03_partial_dump_only_exclude")

        options = build_run_options([
            f"--db-host={params.test_db_host}",
            f"--db-name={params.test_source_db}",
            f"--db-user={params.test_db_user}",
            f"--db-port={params.test_db_port}",
            f"--db-user-password={params.test_db_user_password}",
            f"--config={params.test_config}",
            "--mode=dump",
            f"--processes={params.test_processes}",
            f"--db-connections-per-process={params.db_connections_per_process}",
            f"--prepared-sens-dict-file={prepared_sens_dict_file}",
            f"--partial-tables-exclude-dict-files={partial_tables_exclude_dict_file}",
            "--clear-output-dir",
            "--debug",
            f"--output-dir={output_dir}",
        ])

        res_dump = await PgAnonApp(options).run()
        self.assertEqual(res_dump.result_code, ResultCode.DONE)

        passed_stages.append("test_03_partial_dump_only_exclude")

    async def test_04_partial_dump_include_and_exclude(self):
        self.assertTrue("test_03_partial_dump_only_exclude" in passed_stages)

        prepared_sens_dict_file = self.get_test_dict_path("test_empty_dictionary.py")
        partial_tables_dict_file = self.get_test_dict_path("test_partial_tables_dict.py")
        partial_tables_exclude_dict_file = self.get_test_dict_path("test_partial_exclude_tables_dict.py")
        output_dir = self.get_test_output_path("test_04_partial_dump_include_and_exclude")

        options = build_run_options([
            f"--db-host={params.test_db_host}",
            f"--db-name={params.test_source_db}",
            f"--db-user={params.test_db_user}",
            f"--db-port={params.test_db_port}",
            f"--db-user-password={params.test_db_user_password}",
            f"--config={params.test_config}",
            "--mode=dump",
            f"--processes={params.test_processes}",
            f"--db-connections-per-process={params.db_connections_per_process}",
            f"--prepared-sens-dict-file={prepared_sens_dict_file}",
            f"--partial-tables-dict-files={partial_tables_dict_file}",
            f"--partial-tables-exclude-dict-files={partial_tables_exclude_dict_file}",
            "--clear-output-dir",
            "--debug",
            f"--output-dir={output_dir}",
        ])

        res_dump = await PgAnonApp(options).run()
        self.assertEqual(res_dump.result_code, ResultCode.DONE)

        passed_stages.append("test_04_partial_dump_include_and_exclude")


class PGAnonDictGenUnitTest(unittest.IsolatedAsyncioTestCase, BasicUnitTest):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.target_sens_dict = self.get_test_dict_path("test_prepared_sens_dict_result.py", output=True)
        self.target_sens_dict_expected = self.get_test_expected_dict_path("test_prepared_sens_dict_result_expected.py")
        self.target_no_sens_dict = self.get_test_dict_path("test_prepared_no_sens_dict_result.py", output=True)
        self.target_no_sens_dict_expected = self.get_test_expected_dict_path("test_prepared_no_sens_dict_result_expected.py")

    def assert_sens_dicts(self, prepared_sens_dict: str, prepared_sens_dict_expected: str):
        """
        Comparing sens dicts
        :param prepared_sens_dict: output prepared sens dict
        :param prepared_sens_dict_expected: prepared sens dict for comparison
        :raise AssertError: if dicts are not identical
        """
        with (open(prepared_sens_dict, "r", encoding="utf-8") as file1,
              open(prepared_sens_dict_expected, "r", encoding="utf-8") as file2):
            d1 = json.load(file1)["dictionary"]
            d2 = json.load(file2)["dictionary"]

            def iterate_dict_level_2(data):
                for k, v in data.items():
                    yield {k: v}

            def iterate_dict_level_1(data):
                for item in data:
                    if "fields" in item:
                        yield from iterate_dict_level_2(item["fields"])
                    else:
                        yield from iterate_dict_level_2(item)

            flag_of_identity = True  # comparing elements of two dictionaries
            expected_result_list_of_iterate_dict = []
            result_list_of_iterate_dict = []

            print(f"============> Started comparison of {prepared_sens_dict} and {prepared_sens_dict_expected}")

            for line in iterate_dict_level_1(d2):
                expected_result_list_of_iterate_dict.append(line)

            for line in iterate_dict_level_1(d1):
                result_list_of_iterate_dict.append(line)
                if line not in expected_result_list_of_iterate_dict:
                    flag_of_identity = False
                    print(f"check_comparison: row {line} not found in {prepared_sens_dict}")

            if flag_of_identity:
                for line in iterate_dict_level_1(d2):
                    if line not in result_list_of_iterate_dict:
                        flag_of_identity = False
                        print(f"check_comparison: row {line} not found in {prepared_sens_dict_expected}")

            print(f"<============ Finished comparison of {prepared_sens_dict} and {prepared_sens_dict_expected}")

        self.assertTrue(flag_of_identity)

    def assert_no_sens_dicts(self, prepared_no_sens_dict: str, prepared_no_sens_dict_expected: str) -> bool:
        """
        Comparing no sens dicts
        :param prepared_no_sens_dict: output prepared no sens dict
        :param prepared_no_sens_dict_expected: prepared no sens dict for comparison
        :raise AssertError: if dicts are not identical
        """

        print(f"============> Started comparison of {prepared_no_sens_dict} and {prepared_no_sens_dict_expected}")
        # Checking no-sens dict
        with (open(prepared_no_sens_dict, "r", encoding="utf-8") as file1,
              open(prepared_no_sens_dict_expected, "r", encoding="utf-8") as file2):
            d1 = json.load(file1)
            d2 = json.load(file2)

            # Checking fields count first
            self.assertEqual(len(d1['no_sens_dictionary']), len(d2['no_sens_dictionary']))

            # Sorting fields for next comparison
            sorted_d1 = sorted(d1['no_sens_dictionary'], key=lambda x: (x['schema'], x['table']))
            sorted_d2 = sorted(d2['no_sens_dictionary'], key=lambda x: (x['schema'], x['table']))

            # Comparing fields between dicts
            for d1_field, d2_field in zip(sorted_d1, sorted_d2):
                self.assertEqual(d1_field['schema'], d2_field['schema'])
                self.assertEqual(d1_field['table'], d2_field['table'])
                self.assertEqual(set(d1_field['fields']), set(d2_field['fields']))

        print(f"<============ Finished comparison of {prepared_no_sens_dict} and {prepared_no_sens_dict_expected}")

    options = {}

    async def test_01_init(self):
        res = await self.init_env()
        self.assertEqual(res.result_code, ResultCode.DONE)

    async def test_02_create_dict(self):
        self.assertTrue("init_env" in passed_stages)
        
        meta_dict = self.get_test_dict_path('test_meta_dict.py')

        options = build_run_options([
            f"--db-host={params.test_db_host}",
            f"--db-name={params.test_source_db}",
            f"--db-user={params.test_db_user}",
            f"--db-port={params.test_db_port}",
            f"--db-user-password={params.test_db_user_password}",
            f"--config={params.test_config}",
            "--mode=create-dict",
            "--scan-mode=full",
            f"--meta-dict-file={meta_dict}",
            f"--output-sens-dict-file={self.target_sens_dict}",
            f"--output-no-sens-dict-file={self.target_no_sens_dict}",
            f"--db-connections-per-process={params.db_connections_per_process}",
            "--scan-partial-rows=10000",
            "--debug",
        ])

        res = await PgAnonApp(options).run()
        self.assertTrue(os.path.exists(self.target_sens_dict))
        self.assertTrue(os.path.exists(self.target_no_sens_dict))

        self.assert_sens_dicts(self.target_sens_dict, self.target_sens_dict_expected)
        self.assert_no_sens_dicts(self.target_no_sens_dict, self.target_no_sens_dict_expected)

        self.assertEqual(res.result_code, ResultCode.DONE)
        passed_stages.append("test_02_create_dict")

    async def test_03_dump(self):
        self.assertTrue("init_env" in passed_stages)

        dict_file_name = get_file_name_from_path(self.target_sens_dict)
        output_dir = self.get_test_output_path(dict_file_name)

        options = build_run_options([
            f"--db-host={params.test_db_host}",
            f"--db-name={params.test_source_db}",
            f"--db-user={params.test_db_user}",
            f"--db-port={params.test_db_port}",
            f"--db-user-password={params.test_db_user_password}",
            f"--config={params.test_config}",
            "--mode=dump",
            f"--prepared-sens-dict-file={self.target_sens_dict}",
            f"--output-dir={output_dir}",
            f"--processes={params.test_processes}",
            f"--db-connections-per-process={params.db_connections_per_process}",
            "--clear-output-dir",
            "--debug",
        ])

        self.options["dump"] = options
        res = await PgAnonApp(options).run()
        self.assertEqual(res.result_code, ResultCode.DONE)
        passed_stages.append("test_03_dump")

    async def test_04_restore(self):
        self.assertTrue("test_03_dump" in passed_stages)

        dict_file_name = get_file_name_from_path(self.target_sens_dict)
        input_dir = self.get_test_output_path(dict_file_name)

        options = build_run_options([
            f"--db-host={params.test_db_host}",
            f"--db-name={params.test_target_db}_4",
            f"--db-user={params.test_db_user}",
            f"--db-port={params.test_db_port}",
            f"--db-user-password={params.test_db_user_password}",
            f"--config={params.test_config}",
            f"--db-connections-per-process={params.db_connections_per_process}",
            "--mode=restore",
            f"--input-dir={input_dir}",
            "--drop-custom-check-constr",
            "--debug",
        ])
        self.options["restore"] = options
        res = await PgAnonApp(options).run()
        self.assertEqual(res.result_code, ResultCode.DONE)

        rows = [
            [
                1,
                "ccd778e5850ddf15d7e9a7ad11a8bbd8",
                "invalid_val_1",
                "*",
                round(Decimal(0.1), 2),
                "8cbd2171ab4a14fc243421cde93a71c2",
                "f0b314b0620d1ad1a8af2f56cbdd22ac",
            ],
            [
                2,
                "555da16355e56e162c12c95403419eea",
                "invalid_val_2",
                "*",
                round(Decimal(0.2), 2),
                "8cbd2171ab4a14fc243421cde93a71c2",
                "5385212a24152afd7599bdb3577c7f47",
            ],
        ]
        self.assertTrue(
            await self.check_rows(
                options, "schm_mask_ext_exclude_2", "card_numbers", None, rows
            )
        )

        objs = [
            [
                "schm_mask_ext_exclude_2",
                "card_numbers",
                rows_in_init_env * int(params.test_scale) * 3,
            ]  # see init_env.sql
        ]
        self.assertTrue(await self.check_rows_count(options, objs))

        not_found_in_target = await self.check_list_tables_and_fields(
            self.options["dump"], self.options["restore"]
        )
        self.assertEqual(len(not_found_in_target), 3)
        self.assertEqual(not_found_in_target, [
                ["columnar_internal", "tbl_200", "id"],
                ["columnar_internal", "tbl_200", "val"],
                ["columnar_internal", "tbl_200", "val_skip"],
            ]
        )

        self.assertEqual(res.result_code, ResultCode.DONE)
        passed_stages.append("test_04_restore")

    async def test_05_repeat_create_dict_with_no_sens_dict(self):
        self.assertTrue("test_02_create_dict" in passed_stages)

        meta_dict_file = self.get_test_dict_path("test_meta_dict.py")
        target_no_sens_dict_repeat = self.get_test_dict_path("test_prepared_no_sens_dict_result_repeat.py", output=True)

        options = build_run_options([
            f"--db-host={params.test_db_host}",
            f"--db-name={params.test_source_db}",
            f"--db-user={params.test_db_user}",
            f"--db-port={params.test_db_port}",
            f"--db-user-password={params.test_db_user_password}",
            f"--config={params.test_config}",
            "--mode=create-dict",
            "--scan-mode=full",
            f"--meta-dict-file={meta_dict_file}",
            f"--output-sens-dict-file={self.target_sens_dict}",
            f"--output-no-sens-dict-file={target_no_sens_dict_repeat}",
            f"--prepared-no-sens-dict-file={self.target_no_sens_dict}",
            f"--db-connections-per-process={params.db_connections_per_process}",
            "--scan-partial-rows=10000",
            "--debug",
        ])

        res = await PgAnonApp(options).run()
        self.assertTrue(os.path.exists(self.target_sens_dict))
        self.assertTrue(os.path.exists(target_no_sens_dict_repeat))

        self.assert_sens_dicts(self.target_sens_dict, self.target_sens_dict_expected)
        self.assert_no_sens_dicts(target_no_sens_dict_repeat, self.target_no_sens_dict_expected)

        self.assertEqual(res.result_code, ResultCode.DONE)
        passed_stages.append("test_05_repeat_create_dict_with_no_sens_dict")

    async def test_06_repeat_create_dict_with_no_sens_dict_and_sens_dict(self):
        self.assertTrue("test_02_create_dict" in passed_stages)

        meta_dict_file = self.get_test_dict_path("test_meta_dict.py")

        target_no_sens_dict_repeat = self.get_test_dict_path("test_prepared_no_sens_dict_result_repeat.py", output=True)

        options = build_run_options([
            f"--db-host={params.test_db_host}",
            f"--db-name={params.test_source_db}",
            f"--db-user={params.test_db_user}",
            f"--db-port={params.test_db_port}",
            f"--db-user-password={params.test_db_user_password}",
            f"--config={params.test_config}",
            "--mode=create-dict",
            "--scan-mode=full",
            f"--meta-dict-file={meta_dict_file}",
            f"--output-sens-dict-file={self.target_sens_dict}",
            f"--output-no-sens-dict-file={target_no_sens_dict_repeat}",
            f"--prepared-sens-dict-file={self.target_sens_dict}",
            f"--prepared-no-sens-dict-file={self.target_no_sens_dict}",
            f"--db-connections-per-process={params.db_connections_per_process}",
            "--scan-partial-rows=10000",
            "--debug",
        ])

        res = await PgAnonApp(options).run()
        self.assertTrue(os.path.exists(self.target_sens_dict))
        self.assertTrue(os.path.exists(target_no_sens_dict_repeat))

        self.assert_sens_dicts(self.target_sens_dict, self.target_sens_dict_expected)
        self.assert_no_sens_dicts(target_no_sens_dict_repeat, self.target_no_sens_dict_expected)

        self.assertEqual(res.result_code, ResultCode.DONE)
        passed_stages.append("test_06_repeat_create_dict_with_no_sens_dict_and_sens_dict")

    async def test_07_create_dict_using_include_rules(self):
        self.assertTrue("init_env" in passed_stages)

        meta_dicts = [
            self.get_test_dict_path('test_meta_dict.py'),
            self.get_test_dict_path('meta_include_rules.py'),
        ]
        prepared_sens_dict = self.get_test_dict_path("test_prepared_sens_dict_result_by_include_rule.py", output=True)
        prepared_sens_dict_expected = self.get_test_expected_dict_path("test_prepared_sens_dict_result_by_include_rule_expected.py")

        options = build_run_options([
            f"--db-host={params.test_db_host}",
            f"--db-name={params.test_source_db}",
            f"--db-user={params.test_db_user}",
            f"--db-port={params.test_db_port}",
            f"--db-user-password={params.test_db_user_password}",
            f"--config={params.test_config}",
            "--mode=create-dict",
            "--scan-mode=full",
            f"--meta-dict-file={','.join(meta_dicts)}",
            f"--output-sens-dict-file={prepared_sens_dict}",
            f"--db-connections-per-process={params.db_connections_per_process}",
            "--scan-partial-rows=10000",
            "--debug",
        ])

        res = await PgAnonApp(options).run()
        self.assertTrue(os.path.exists(prepared_sens_dict))
        self.assertTrue(os.path.exists(self.target_no_sens_dict))

        self.assert_sens_dicts(prepared_sens_dict, prepared_sens_dict_expected)

        self.assertEqual(res.result_code, ResultCode.DONE)
        passed_stages.append("test_07_create_dict_using_include_rules")
    
    async def test_08_create_dict_using_include_and_skip_rules_with_masks(self):
        self.assertTrue("init_env" in passed_stages)

        meta_dicts = [
            self.get_test_dict_path('test_meta_dict.py'),
            self.get_test_dict_path('meta_include_and_skip_rules.py'),
        ]
        prepared_sens_dict = self.get_test_dict_path("test_prepared_sens_dict_result_by_include_and_skip_rules.py", output=True)
        prepared_sens_dict_expected = self.get_test_expected_dict_path("test_prepared_sens_dict_result_by_include_and_skip_rules_expected.py")

        options = build_run_options([
            f"--db-host={params.test_db_host}",
            f"--db-name={params.test_source_db}",
            f"--db-user={params.test_db_user}",
            f"--db-port={params.test_db_port}",
            f"--db-user-password={params.test_db_user_password}",
            f"--config={params.test_config}",
            "--mode=create-dict",
            "--scan-mode=full",
            f"--meta-dict-file={','.join(meta_dicts)}",
            f"--output-sens-dict-file={prepared_sens_dict}",
            f"--db-connections-per-process={params.db_connections_per_process}",
            "--scan-partial-rows=10000",
            "--debug",
        ])

        res = await PgAnonApp(options).run()
        self.assertTrue(os.path.exists(prepared_sens_dict))
        self.assertTrue(os.path.exists(self.target_no_sens_dict))

        self.assert_sens_dicts(prepared_sens_dict, prepared_sens_dict_expected)

        self.assertEqual(res.result_code, ResultCode.DONE)
        passed_stages.append("test_08_create_dict_using_include_and_skip_rules_with_masks")

    async def test_09_create_dict_using_partial_constants(self):
        self.assertTrue("init_env" in passed_stages)

        meta_dicts = [
            self.get_test_dict_path('test_meta_dict.py'),
            self.get_test_dict_path('meta_partial_constants.py'),
        ]
        prepared_sens_dict = self.get_test_dict_path("test_prepared_sens_dict_result_by_partial_constants.py", output=True)
        prepared_sens_dict_expected = self.get_test_expected_dict_path("test_prepared_sens_dict_result_by_partial_constants_expected.py")

        options = build_run_options([
            f"--db-host={params.test_db_host}",
            f"--db-name={params.test_source_db}",
            f"--db-user={params.test_db_user}",
            f"--db-port={params.test_db_port}",
            f"--db-user-password={params.test_db_user_password}",
            f"--config={params.test_config}",
            "--mode=create-dict",
            "--scan-mode=full",
            f"--meta-dict-file={','.join(meta_dicts)}",
            f"--output-sens-dict-file={prepared_sens_dict}",
            f"--db-connections-per-process={params.db_connections_per_process}",
            "--scan-partial-rows=10000",
            "--debug",
        ])

        res = await PgAnonApp(options).run()
        self.assertTrue(os.path.exists(prepared_sens_dict))
        self.assertTrue(os.path.exists(self.target_no_sens_dict))

        self.assert_sens_dicts(prepared_sens_dict, prepared_sens_dict_expected)

        self.assertEqual(res.result_code, ResultCode.DONE)
        passed_stages.append("test_09_create_dict_using_partial_constants")

    async def test_10_create_dict_using_data_sql_condition(self):
        self.assertTrue("init_env" in passed_stages)

        meta_dicts = [
            self.get_test_dict_path('test_meta_dict.py'),
            self.get_test_dict_path('meta_data_sql_condition.py'),
        ]
        prepared_sens_dict = self.get_test_dict_path("test_prepared_sens_dict_result_by_data_sql_condition.py", output=True)
        prepared_sens_dict_expected = self.get_test_expected_dict_path("test_prepared_sens_dict_result_by_data_sql_condition_expected.py")

        options = build_run_options([
            f"--db-host={params.test_db_host}",
            f"--db-name={params.test_source_db}",
            f"--db-user={params.test_db_user}",
            f"--db-port={params.test_db_port}",
            f"--db-user-password={params.test_db_user_password}",
            f"--config={params.test_config}",
            "--mode=create-dict",
            "--scan-mode=full",
            f"--meta-dict-file={','.join(meta_dicts)}",
            f"--output-sens-dict-file={prepared_sens_dict}",
            f"--db-connections-per-process={params.db_connections_per_process}",
            "--scan-partial-rows=10000",
            "--debug",
        ])

        res = await PgAnonApp(options).run()
        self.assertTrue(os.path.exists(prepared_sens_dict))
        self.assertTrue(os.path.exists(self.target_no_sens_dict))

        self.assert_sens_dicts(prepared_sens_dict, prepared_sens_dict_expected)

        self.assertEqual(res.result_code, ResultCode.DONE)
        passed_stages.append("test_10_create_dict_using_data_sql_condition")

    async def test_11_create_dict_using_data_func(self):
        self.assertTrue("init_env" in passed_stages)

        meta_dicts = [
            self.get_test_dict_path('test_meta_dict.py'),
            self.get_test_dict_path('meta_data_func.py'),
        ]
        prepared_sens_dict = self.get_test_dict_path("test_prepared_sens_dict_result_by_data_func.py", output=True)
        prepared_sens_dict_expected = self.get_test_expected_dict_path("test_prepared_sens_dict_result_by_data_func_expected.py")

        options = build_run_options([
            f"--db-host={params.test_db_host}",
            f"--db-name={params.test_source_db}",
            f"--db-user={params.test_db_user}",
            f"--db-port={params.test_db_port}",
            f"--db-user-password={params.test_db_user_password}",
            f"--config={params.test_config}",
            "--mode=create-dict",
            "--scan-mode=full",
            f"--meta-dict-file={','.join(meta_dicts)}",
            f"--output-sens-dict-file={prepared_sens_dict}",
            f"--db-connections-per-process={params.db_connections_per_process}",
            "--scan-partial-rows=10000",
            "--debug",
        ])

        res = await PgAnonApp(options).run()
        self.assertTrue(os.path.exists(prepared_sens_dict))
        self.assertTrue(os.path.exists(self.target_no_sens_dict))

        self.assert_sens_dicts(prepared_sens_dict, prepared_sens_dict_expected)

        self.assertEqual(res.result_code, ResultCode.DONE)
        passed_stages.append("test_11_create_dict_using_data_func")

    async def test_12_create_dict_with_type_aliases(self):
        self.assertTrue("init_env" in passed_stages)

        meta_dicts = [
            self.get_test_dict_path('test_meta_dict_type_aliases.py'),
        ]
        prepared_sens_dict = self.get_test_dict_path("test_prepared_sens_dict_result_type_aliases.py", output=True)
        prepared_sens_dict_expected = self.get_test_expected_dict_path("test_prepared_sens_dict_result_type_aliases_expected.py")

        options = build_run_options([
            f"--db-host={params.test_db_host}",
            f"--db-name={params.test_source_db}",
            f"--db-user={params.test_db_user}",
            f"--db-port={params.test_db_port}",
            f"--db-user-password={params.test_db_user_password}",
            f"--config={params.test_config}",
            "--mode=create-dict",
            "--scan-mode=full",
            f"--meta-dict-file={','.join(meta_dicts)}",
            f"--output-sens-dict-file={prepared_sens_dict}",
            f"--db-connections-per-process={params.db_connections_per_process}",
            "--scan-partial-rows=10000",
            "--debug",
        ])

        res = await PgAnonApp(options).run()
        self.assertTrue(os.path.exists(prepared_sens_dict))
        self.assertTrue(os.path.exists(self.target_no_sens_dict))

        self.assert_sens_dicts(prepared_sens_dict, prepared_sens_dict_expected)

        self.assertEqual(res.result_code, ResultCode.DONE)
        passed_stages.append("test_12_create_dict_with_type_aliases")

    async def test_13_create_dict_with_default_anonymization_function(self):
        self.assertTrue("init_env" in passed_stages)

        meta_dicts = [
            self.get_test_dict_path('test_meta_dict_default_func.py'),
        ]
        prepared_sens_dict = self.get_test_dict_path("test_prepared_sens_dict_result_default_func.py", output=True)
        prepared_sens_dict_expected = self.get_test_expected_dict_path("test_prepared_sens_dict_result_default_func_expected.py")

        options = build_run_options([
            f"--db-host={params.test_db_host}",
            f"--db-name={params.test_source_db}",
            f"--db-user={params.test_db_user}",
            f"--db-port={params.test_db_port}",
            f"--db-user-password={params.test_db_user_password}",
            f"--config={params.test_config}",
            "--mode=create-dict",
            "--scan-mode=full",
            f"--meta-dict-file={','.join(meta_dicts)}",
            f"--output-sens-dict-file={prepared_sens_dict}",
            f"--db-connections-per-process={params.db_connections_per_process}",
            "--scan-partial-rows=10000",
            "--debug",
        ])

        res = await PgAnonApp(options).run()
        self.assertTrue(os.path.exists(prepared_sens_dict))
        self.assertTrue(os.path.exists(self.target_no_sens_dict))

        self.assert_sens_dicts(prepared_sens_dict, prepared_sens_dict_expected)

        self.assertEqual(res.result_code, ResultCode.DONE)
        passed_stages.append("test_13_create_dict_with_default_anonymization_function")

    async def test_14_create_dict_using_words_and_phrases_constants(self):
        self.assertTrue("init_env" in passed_stages)

        meta_dicts = [
            self.get_test_dict_path('test_meta_dict.py'),
            self.get_test_dict_path('meta_words_and_phrases_constants.py'),
        ]
        prepared_sens_dict = self.get_test_dict_path("test_prepared_sens_dict_result_by_words_and_phrases_constants.py", output=True)
        prepared_sens_dict_expected = self.get_test_expected_dict_path("test_prepared_sens_dict_result_by_words_and_phrases_constants_expected.py")

        options = build_run_options([
            f"--db-host={params.test_db_host}",
            f"--db-name={params.test_source_db}",
            f"--db-user={params.test_db_user}",
            f"--db-port={params.test_db_port}",
            f"--db-user-password={params.test_db_user_password}",
            f"--config={params.test_config}",
            "--mode=create-dict",
            "--scan-mode=full",
            f"--meta-dict-file={','.join(meta_dicts)}",
            f"--output-sens-dict-file={prepared_sens_dict}",
            f"--db-connections-per-process={params.db_connections_per_process}",
            "--scan-partial-rows=10000",
            "--debug",
        ])

        res = await PgAnonApp(options).run()
        self.assertTrue(os.path.exists(prepared_sens_dict))
        self.assertTrue(os.path.exists(self.target_no_sens_dict))

        self.assert_sens_dicts(prepared_sens_dict, prepared_sens_dict_expected)

        self.assertEqual(res.result_code, ResultCode.DONE)
        passed_stages.append("test_14_create_dict_using_words_and_phrases_constants")


class TmpResults:
    res_test_02 = None
    res_test_03 = None


tmp_results = TmpResults()


class PGAnonDictGenStressUnitTest(unittest.IsolatedAsyncioTestCase, BasicUnitTest):
    target_dict = "test_create_dict_result.py"
    options = {}

    async def test_01_stress_init(self):
        res = await self.init_stress_env()
        self.assertEqual(res.result_code, ResultCode.DONE)

    async def test_02_create_dict(self):
        self.assertTrue("init_stress_env" in passed_stages)

        meta_dict_file = self.get_test_dict_path('test_meta_dict.py')
        output_sens_dict_file = self.get_test_dict_path(f'stress_{self.target_dict}', output=True)
        options = build_run_options([
            f"--db-host={params.test_db_host}",
            f"--db-name={params.test_source_db}_stress",
            f"--db-user={params.test_db_user}",
            f"--db-port={params.test_db_port}",
            f"--db-user-password={params.test_db_user_password}",
            f"--config={params.test_config}",
            "--mode=create-dict",
            "--scan-mode=partial",
            f"--meta-dict-file={meta_dict_file}",
            f"--output-sens-dict-file={output_sens_dict_file}",
            "--db-connections-per-process=4",
            "--processes=2",
            "--scan-partial-rows=100",
        ])

        res = await PgAnonApp(options).run()
        self.assertEqual(res.result_code, ResultCode.DONE)
        tmp_results.res_test_02 = res.elapsed
        passed_stages.append("test_02_create_dict")

    async def test_03_create_dict(self):
        self.assertTrue("init_stress_env" in passed_stages)

        meta_dict_file = self.get_test_dict_path("test_meta_dict.py")
        output_sens_dict_file = self.get_test_dict_path(f'stress_{self.target_dict}', output=True)

        options = build_run_options([
            f"--db-host={params.test_db_host}",
            f"--db-name={params.test_source_db}_stress",
            f"--db-user={params.test_db_user}",
            f"--db-port={params.test_db_port}",
            f"--db-user-password={params.test_db_user_password}",
            f"--config={params.test_config}",
            "--mode=create-dict",
            "--scan-mode=full",
            f"--meta-dict-file={meta_dict_file}",
            f"--output-sens-dict-file={output_sens_dict_file}",
            "--db-connections-per-process=4",
            "--processes=2",
        ])

        res = await PgAnonApp(options).run()
        self.assertEqual(res.result_code, ResultCode.DONE)
        tmp_results.res_test_03 = res.elapsed
        passed_stages.append("test_03_create_dict")

    async def test_04_create_dict(self):
        self.assertTrue("test_02_create_dict" in passed_stages and "test_03_create_dict" in passed_stages)

        print(
            f"Comparing values: %s < (%s / 5)"
            % (tmp_results.res_test_02, tmp_results.res_test_03)
        )
        # Warning: this test will be failed if you use debugger
        # We are testing performance of test_02_create_dict vs test_03_create_dict
        self.assertTrue(
            float(tmp_results.res_test_02) < float(tmp_results.res_test_03) / 5
        )


class PGAnonMaskUnitTest(unittest.IsolatedAsyncioTestCase, BasicUnitTest):
    options = {}

    async def test_01_init(self):
        res = await self.init_env()
        self.assertEqual(res.result_code, ResultCode.DONE)

    async def test_02_mask_dump(self):
        self.assertTrue("init_env" in passed_stages)

        prepared_sens_dict_file = self.get_test_dict_path('mask_test.py')
        dict_file_name = get_file_name_from_path(prepared_sens_dict_file)
        output_dir = self.get_test_output_path(dict_file_name)

        options = build_run_options([
            f"--db-host={params.test_db_host}",
            f"--db-name={params.test_source_db}",
            f"--db-user={params.test_db_user}",
            f"--db-port={params.test_db_port}",
            f"--db-user-password={params.test_db_user_password}",
            f"--config={params.test_config}",
            "--mode=dump",
            f"--prepared-sens-dict-file={prepared_sens_dict_file}",
            f"--output-dir={output_dir}",
            f"--processes={params.test_processes}",
            f"--db-connections-per-process={params.db_connections_per_process}",
            "--clear-output-dir",
            "--debug",
        ])

        self.options["dump"] = options
        res = await PgAnonApp(options).run()
        self.assertEqual(res.result_code, ResultCode.DONE)
        passed_stages.append("test_02_mask_dump")

    async def test_03_mask_restore(self):
        self.assertTrue("test_02_mask_dump" in passed_stages)
        input_dir = self.get_test_output_path("mask_test")

        options = build_run_options([
            f"--db-host={params.test_db_host}",
            f"--db-name={params.test_target_db}_5",
            f"--db-user={params.test_db_user}",
            f"--db-port={params.test_db_port}",
            f"--db-user-password={params.test_db_user_password}",
            f"--config={params.test_config}",
            "--mode=restore",
            f"--db-connections-per-process={params.db_connections_per_process}",
            f"--input-dir={input_dir}",
            "--drop-custom-check-constr",
            "--debug",
        ])

        self.options["restore"] = options
        res = await PgAnonApp(options).run()
        self.assertEqual(res.result_code, ResultCode.DONE)

        rows = [[1, round(Decimal(101010), 2)], [2, round(Decimal(101010), 2)]]
        self.assertTrue(
            await self.check_rows(options, "public", "contracts", ["id", "amount"], rows)
        )

        rows = [[1, round(Decimal(202020), 2)], [2, round(Decimal(202020), 2)]]
        self.assertTrue(
            await self.check_rows(options, "public", "tbl_100", ["id", "amount"], rows)
        )

        source_tables, target_tables = await self.get_list_tables_with_diff_data(
            self.options["dump"], self.options["restore"]
        )
        self.assertTrue(
            self.save_and_compare_result(
                "PGAnonMaskUnitTest_source_tables", source_tables
            )
        )
        self.assertTrue(
            self.save_and_compare_result(
                "PGAnonMaskUnitTest_target_tables", target_tables
            )
        )


class PGAnonViewDataUnitTest(unittest.IsolatedAsyncioTestCase, BasicUnitTest):
    async def test_01_init(self):
        res = await self.init_env()
        self.assertEqual(res.result_code, ResultCode.DONE)

    async def test_02_view_data_print(self):
        self.assertTrue("init_env" in passed_stages)

        prepared_sens_dict_file_name = self.get_test_expected_dict_path('test_prepared_sens_dict_result_expected.py')

        options = build_run_options([
            f"--db-host={params.test_db_host}",
            f"--db-name={params.test_source_db}",
            f"--db-user={params.test_db_user}",
            f"--db-port={params.test_db_port}",
            f"--db-user-password={params.test_db_user_password}",
            f"--config={params.test_config}",
            "--mode=view-data",
            f"--prepared-sens-dict-file={prepared_sens_dict_file_name}",
            "--schema-name=public",
            "--table-name=contracts",
            "--limit=10",
            "--offset=0",
            "--debug",
        ])
        res = await PgAnonApp(options).run()
        self.assertEqual(res.result_code, ResultCode.DONE)

        passed_stages.append("test_02_view_data_print")

    async def test_03_view_data_json(self):
        self.assertTrue("init_env" in passed_stages)

        prepared_sens_dict_file_name = self.get_test_expected_dict_path('test_prepared_sens_dict_result_expected.py')

        options = build_run_options([
            f"--db-host={params.test_db_host}",
            f"--db-name={params.test_source_db}",
            f"--db-user={params.test_db_user}",
            f"--db-port={params.test_db_port}",
            f"--db-user-password={params.test_db_user_password}",
            f"--config={params.test_config}",
            "--json",
            "--mode=view-data",
            f"--prepared-sens-dict-file={prepared_sens_dict_file_name}",
            "--schema-name=public",
            "--table-name=contracts",
            "--limit=10",
            "--offset=0",
            "--debug",
        ])
        context = PgAnonApp(options).context  # Setup for context reusing only

        executor = ViewDataMode(context)
        await executor.run()

        row_len = set(len(row) for row in list(json.loads(executor.json).values()))
        self.assertEqual(len(row_len), 1)  # all fields have equal length of rows

        passed_stages.append("test_03_view_data_json")

    async def test_04_view_data_null(self):
        self.assertTrue("init_env" in passed_stages)

        prepared_sens_dict_file_name = self.get_test_expected_dict_path('test_prepared_sens_dict_result_expected.py')

        options = [
            f"--db-host={params.test_db_host}",
            f"--db-name={params.test_source_db}",
            f"--db-user={params.test_db_user}",
            f"--db-port={params.test_db_port}",
            f"--db-user-password={params.test_db_user_password}",
            f"--config={params.test_config}",
            "--mode=view-data",
            f"--prepared-sens-dict-file={prepared_sens_dict_file_name}",
            "--schema-name=schm_mask_ext_exclude_2",
            "--table-name=card_numbers",
            "--limit=10",
            "--offset=30235",
            "--debug",
        ]
        options_print = build_run_options(options)
        res_print = await PgAnonApp(options_print).run()
        self.assertEqual(res_print.result_code, ResultCode.DONE)

        options.append("--json")
        options_json = build_run_options(options)
        res_json = await PgAnonApp(options_json).run()
        self.assertEqual(res_json.result_code, ResultCode.DONE)

        passed_stages.append("test_04_view_data_null")

    async def test_05_view_data_without_matched_rule(self):
        self.assertTrue("init_env" in passed_stages)

        prepared_sens_dict_file_name = self.get_test_expected_dict_path('test_prepared_sens_dict_result_with_no_existing_schema.py')
        schema_name: str = 'schm_mask_ext_exclude_2'
        table_name: str = 'card_numbers'

        options = build_run_options([
            f"--db-host={params.test_db_host}",
            f"--db-name={params.test_source_db}",
            f"--db-user={params.test_db_user}",
            f"--db-port={params.test_db_port}",
            f"--db-user-password={params.test_db_user_password}",
            f"--config={params.test_config}",
            "--mode=view-data",
            f"--prepared-sens-dict-file={prepared_sens_dict_file_name}",
            f"--schema-name={schema_name}",
            f"--table-name={table_name}",
            "--limit=10",
            "--offset=0",
            "--debug",
        ])
        context = PgAnonApp(options).context  # Setup for context reusing only

        executor = ViewDataMode(context)
        await executor.run()

        self.assertTrue(len(executor.table.rows) > 0)

        passed_stages.append("test_05_view_data_without_matched_rule")


class PGAnonViewFieldsUnitTest(unittest.IsolatedAsyncioTestCase, BasicUnitTest):

    @staticmethod
    def _count_fields_by_type(executor: ViewFieldsMode) -> Dict[str, int]:
        counters = {
            'in_dict': 0,
            'not_in_dict': 0,
        }

        for field in executor.fields:
            if field.rule != '---':
                # if field has hash-function, we expect, what this field in dictionary, but we need check it
                dict_rule = get_dict_rule_for_table(
                    dictionary_rules=executor.context.prepared_dictionary_obj['dictionary'],
                    schema=field.nspname,
                    table=field.relname
                )

                if dict_rule and (field.column_name in dict_rule.get('fields', {}) or dict_rule.get('raw_sql')):
                    counters['in_dict'] += 1
            else:
                # if field hasn't hash-function, we sure, what this field not in dictionary
                counters['not_in_dict'] += 1

        return counters

    async def test_01_init(self):
        res = await self.init_env()
        self.assertEqual(res.result_code, ResultCode.DONE)

    async def test_02_view_fields_full(self):
        self.assertTrue("init_env" in passed_stages)

        prepared_sens_dict_file_name = self.get_test_dict_path('test.py')

        options = build_run_options([
            f"--db-host={params.test_db_host}",
            f"--db-name={params.test_source_db}",
            f"--db-user={params.test_db_user}",
            f"--db-port={params.test_db_port}",
            f"--db-user-password={params.test_db_user_password}",
            f"--config={params.test_config}",
            "--mode=view-fields",
            f"--prepared-sens-dict-file={prepared_sens_dict_file_name}",
        ])

        context = PgAnonApp(options).context  # Setup for context reusing only

        executor = ViewFieldsMode(context)
        await executor.run()

        all_rows_count = await get_scan_fields_count(context.connection_params)
        self.assertEqual(len(executor.table.rows), all_rows_count)

        fields_counters = self._count_fields_by_type(executor)
        self.assertEqual(all_rows_count, fields_counters['in_dict'] + fields_counters['not_in_dict'])

        passed_stages.append("test_02_view_fields_full")

    async def test_03_view_fields_full_by_schema(self):
        self.assertTrue("init_env" in passed_stages)

        prepared_sens_dict_file_name = self.get_test_dict_path('test.py')

        schema_name: str = 'public'
        options = build_run_options([
            f"--db-host={params.test_db_host}",
            f"--db-name={params.test_source_db}",
            f"--db-user={params.test_db_user}",
            f"--db-port={params.test_db_port}",
            f"--db-user-password={params.test_db_user_password}",
            f"--config={params.test_config}",
            "--mode=view-fields",
            f"--prepared-sens-dict-file={prepared_sens_dict_file_name}",
            f"--schema-name={schema_name}",
        ])

        context = PgAnonApp(options).context  # Setup for context reusing only

        executor = ViewFieldsMode(context)
        await executor.run()

        for field in executor.fields:
            self.assertEqual(field.nspname, schema_name)

        passed_stages.append("test_03_view_fields_full_by_schema")

    async def test_04_view_fields_full_by_schema_mask(self):
        self.assertTrue("init_env" in passed_stages)

        prepared_sens_dict_file_name = self.get_test_dict_path('test.py')

        schema_mask: str = '^pub.*'
        options = build_run_options([
            f"--db-host={params.test_db_host}",
            f"--db-name={params.test_source_db}",
            f"--db-user={params.test_db_user}",
            f"--db-port={params.test_db_port}",
            f"--db-user-password={params.test_db_user_password}",
            f"--config={params.test_config}",
            "--mode=view-fields",
            f"--prepared-sens-dict-file={prepared_sens_dict_file_name}",
            f"--schema-mask={schema_mask}",
        ])

        context = PgAnonApp(options).context  # Setup for context reusing only

        executor = ViewFieldsMode(context)
        await executor.run()

        for field in executor.fields:
            match = re.search(schema_mask, field.nspname)
            self.assertIsNotNone(match)

        passed_stages.append("test_04_view_fields_full_by_schema_mask")

    async def test_05_view_fields_full_by_table(self):
        self.assertTrue("init_env" in passed_stages)

        prepared_sens_dict_file_name = self.get_test_dict_path('test.py')

        table_name: str = 'inn_info'
        options = build_run_options([
            f"--db-host={params.test_db_host}",
            f"--db-name={params.test_source_db}",
            f"--db-user={params.test_db_user}",
            f"--db-port={params.test_db_port}",
            f"--db-user-password={params.test_db_user_password}",
            f"--config={params.test_config}",
            "--mode=view-fields",
            f"--prepared-sens-dict-file={prepared_sens_dict_file_name}",
            f"--table-name={table_name}",
        ])

        context = PgAnonApp(options).context  # Setup for context reusing only

        executor = ViewFieldsMode(context)
        await executor.run()

        for field in executor.fields:
            self.assertEqual(field.relname, table_name)

        passed_stages.append("test_05_view_fields_full_by_table")

    async def test_06_view_fields_full_by_table_mask(self):
        self.assertTrue("init_env" in passed_stages)

        prepared_sens_dict_file_name = self.get_test_dict_path('test.py')

        table_mask: str = '.*\\d$'
        options = build_run_options([
            f"--db-host={params.test_db_host}",
            f"--db-name={params.test_source_db}",
            f"--db-user={params.test_db_user}",
            f"--db-port={params.test_db_port}",
            f"--db-user-password={params.test_db_user_password}",
            f"--config={params.test_config}",
            "--mode=view-fields",
            f"--prepared-sens-dict-file={prepared_sens_dict_file_name}",
            f"--table-mask={table_mask}",
        ])

        context = PgAnonApp(options).context  # Setup for context reusing only

        executor = ViewFieldsMode(context)
        await executor.run()

        for field in executor.fields:
            match = re.search(table_mask, field.relname)
            self.assertIsNotNone(match)

        passed_stages.append("test_06_view_fields_full_by_table_mask")

    async def test_07_view_fields_full_with_cut_output_and_notice(self):
        self.assertTrue("init_env" in passed_stages)

        prepared_sens_dict_file_name = self.get_test_dict_path('test.py')
        fields_scan_length = 5

        options = build_run_options([
            f"--db-host={params.test_db_host}",
            f"--db-name={params.test_source_db}",
            f"--db-user={params.test_db_user}",
            f"--db-port={params.test_db_port}",
            f"--db-user-password={params.test_db_user_password}",
            f"--config={params.test_config}",
            "--mode=view-fields",
            f"--prepared-sens-dict-file={prepared_sens_dict_file_name}",
            f"--fields-count={fields_scan_length}",
        ])

        context = PgAnonApp(options).context  # Setup for context reusing only

        executor = ViewFieldsMode(context)
        await executor.run()

        all_rows_count = await get_scan_fields_count(context.connection_params)
        self.assertNotEqual(len(executor.table.rows), all_rows_count)
        self.assertEqual(len(executor.table.rows), fields_scan_length)
        self.assertEqual(len(executor.fields), fields_scan_length)
        self.assertTrue(executor.fields_cut_by_limits)

        passed_stages.append("test_07_view_fields_full_with_cut_output")

    async def test_08_view_fields_with_only_sensitive_fields(self):
        self.assertTrue("init_env" in passed_stages)

        prepared_sens_dict_file_name = self.get_test_dict_path('test.py')

        # Only sensitive executor run
        options_only_sensitive = build_run_options([
            f"--db-host={params.test_db_host}",
            f"--db-name={params.test_source_db}",
            f"--db-user={params.test_db_user}",
            f"--db-port={params.test_db_port}",
            f"--db-user-password={params.test_db_user_password}",
            f"--config={params.test_config}",
            "--mode=view-fields",
            f"--prepared-sens-dict-file={prepared_sens_dict_file_name}",
            "--view-only-sensitive-fields",
        ])
        context_only_sensitive = PgAnonApp(options_only_sensitive).context  # Setup for context reusing only
        executor_only_sensitive = ViewFieldsMode(context_only_sensitive)
        await executor_only_sensitive.run()

        # Full executor run
        options_full = build_run_options([
            f"--db-host={params.test_db_host}",
            f"--db-name={params.test_source_db}",
            f"--db-user={params.test_db_user}",
            f"--db-port={params.test_db_port}",
            f"--db-user-password={params.test_db_user_password}",
            f"--config={params.test_config}",
            "--mode=view-fields",
            f"--prepared-sens-dict-file={prepared_sens_dict_file_name}",
        ])
        context_full = PgAnonApp(options_full).context  # Setup for context reusing only
        executor_full = ViewFieldsMode(context_full)
        await executor_full.run()

        all_rows_count = await get_scan_fields_count(context_full.connection_params)
        self.assertNotEqual(len(executor_full.fields), len(executor_only_sensitive.fields))
        self.assertEqual(len(executor_full.table.rows), all_rows_count)
        self.assertNotEqual(len(executor_only_sensitive.table.rows), all_rows_count)

        sensitive_fields_in_full_executor: Set[str] = {
            str(field) for field in executor_full.fields if field.rule != '---'
        }

        executor_only_sensitive_fields_set: Set[str] = {
            str(field) for field in executor_only_sensitive.fields
        }

        self.assertEqual(sensitive_fields_in_full_executor, executor_only_sensitive_fields_set)

        passed_stages.append("test_08_view_fields_with_only_sensitive_fields")

    async def test_09_view_filter_json_output(self):
        self.assertTrue("init_env" in passed_stages)

        prepared_sens_dict_file_name = self.get_test_dict_path('test.py')

        options = build_run_options([
            f"--db-host={params.test_db_host}",
            f"--db-name={params.test_source_db}",
            f"--db-user={params.test_db_user}",
            f"--db-port={params.test_db_port}",
            f"--db-user-password={params.test_db_user_password}",
            f"--config={params.test_config}",
            "--mode=view-fields",
            f"--prepared-sens-dict-file={prepared_sens_dict_file_name}",
            f"--json",
        ])

        context = PgAnonApp(options).context  # Setup for context reusing only

        executor = ViewFieldsMode(context)
        await executor.run()

        self.assertIsNone(executor.table)
        self.assertIsNotNone(executor.json)

        all_rows_count = await get_scan_fields_count(context.connection_params)
        json_data_len = len(json.loads(executor.json))
        self.assertEqual(json_data_len, all_rows_count)
        self.assertEqual(json_data_len, len(executor.fields))

        passed_stages.append("test_11_view_filter_json_output")

    async def test_10_view_fields_exception_on_zero_fields(self):
        self.assertTrue("init_env" in passed_stages)

        prepared_sens_dict_file_name = self.get_test_dict_path('test.py')

        options = build_run_options([
            f"--db-host={params.test_db_host}",
            f"--db-name={params.test_source_db}",
            f"--db-user={params.test_db_user}",
            f"--db-port={params.test_db_port}",
            f"--db-user-password={params.test_db_user_password}",
            f"--config={params.test_config}",
            "--mode=view-fields",
            f"--prepared-sens-dict-file={prepared_sens_dict_file_name}",
            "--fields-count=0",
        ])
        executor_failed = False

        context = PgAnonApp(options).context  # Setup for context reusing only

        executor = ViewFieldsMode(context)
        try:
            await executor.run()
        except ValueError:
            executor_failed = True

        self.assertTrue(executor_failed)
        self.assertIsNone(executor.fields)
        self.assertIsNone(executor.table)

        passed_stages.append("test_09_view_fields_json")

    async def test_10_view_fields_exception_on_filter_to_zero_fields(self):
        self.assertTrue("init_env" in passed_stages)

        prepared_sens_dict_file_name = self.get_test_dict_path('test.py')

        schema_name: str = 'not_exists_schema_name'
        options = build_run_options([
            f"--db-host={params.test_db_host}",
            f"--db-name={params.test_source_db}",
            f"--db-user={params.test_db_user}",
            f"--db-port={params.test_db_port}",
            f"--db-user-password={params.test_db_user_password}",
            f"--config={params.test_config}",
            "--mode=view-fields",
            f"--prepared-sens-dict-file={prepared_sens_dict_file_name}",
            f"--schema-name={schema_name}",
        ])
        executor_failed = False

        context = PgAnonApp(options).context  # Setup for context reusing only

        executor = ViewFieldsMode(context)
        try:
            await executor.run()
        except ValueError:
            executor_failed = True

        self.assertTrue(executor_failed)
        self.assertEqual(len(executor.fields), 0)
        self.assertIsNone(executor.table)
        self.assertFalse(executor.fields_cut_by_limits)

        passed_stages.append("test_10_view_filter_to_zero_fields")

    async def test_12_view_fields_exception_on_empty_prepared_dictionary(self):
        self.assertTrue("init_env" in passed_stages)

        prepared_sens_dict_file_name = self.get_test_dict_path('test_empty_dictionary.py')

        options = build_run_options([
            f"--db-host={params.test_db_host}",
            f"--db-name={params.test_source_db}",
            f"--db-user={params.test_db_user}",
            f"--db-port={params.test_db_port}",
            f"--db-user-password={params.test_db_user_password}",
            f"--config={params.test_config}",
            f"--prepared-sens-dict-file={prepared_sens_dict_file_name}",
            "--mode=view-fields",
        ])
        executor_failed = False

        context = PgAnonApp(options).context  # Setup for context reusing only

        executor = ViewFieldsMode(context)
        try:
            await executor.run()
        except ValueError:
            executor_failed = True

        self.assertTrue(executor_failed)

        passed_stages.append("test_12_view_with_empty_prepared_dictionary")

    async def test_13_view_fields_exception_on_no_prepared_dictionary(self):
        self.assertTrue("init_env" in passed_stages)

        """

        run_options = RunOptions([
            f"--db-host={params.test_db_host}",
            f"--db-name={params.test_source_db}",
            f"--db-user={params.test_db_user}",
            f"--db-port={params.test_db_port}",
            f"--db-user-password={params.test_db_user_password}",
            f"--config={params.test_config}",
            "--mode=view-fields",
        ])
        executor_failed = False

        context = Context(run_options)  # Setup for context reusing only

        
        """

        options = build_run_options([
            f"--db-host={params.test_db_host}",
            f"--db-name={params.test_source_db}",
            f"--db-user={params.test_db_user}",
            f"--db-port={params.test_db_port}",
            f"--db-user-password={params.test_db_user_password}",
            f"--config={params.test_config}",
            "--mode=view-fields",
        ])
        executor_failed = False

        context = PgAnonApp(options).context  # Setup for context reusing only

        executor = ViewFieldsMode(context)
        try:
            await executor.run()
        except ValueError:
            executor_failed = True

        self.assertTrue(executor_failed)

        passed_stages.append("test_13_view_without_prepared_dictionary")


if __name__ == "__main__":
    unittest.main(exit=False)
    # loader = unittest.TestLoader()
    #
    # tests = loader.loadTestsFromTestCase(PGAnonDictGenUnitTest)
    # test_suite = unittest.TestSuite(tests)
    #
    # test_suite = loader.discover(start_dir='.', pattern='test*.py')
    #
    # runner = unittest.TextTestRunner(failfast=True, verbosity=2)
    # runner.run(test_suite)
