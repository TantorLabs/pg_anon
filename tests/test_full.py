import copy
import json
import os
import sys
import unittest
from decimal import Decimal

import asyncpg

from pg_anon.common import (
    PgAnonResult,
    ResultCode,
    exception_helper,
    recordset_to_list_flat,
    to_json,
)
from pg_anon.context import Context

from pg_anon import MainRoutine

input_args = None
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
    test_threads = 4

    def __init__(self):
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
        if os.environ.get("TEST_THREADS") is not None:
            self.test_threads = os.environ["TEST_THREADS"]


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
        with open(os.path.join(current_dir, env_sql_file), "r", encoding="utf-8") as f:
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

        parser = Context.get_arg_parser()
        args = parser.parse_args(
            [
                "--db-host=%s" % params.test_db_host,
                "--db-name=postgres",
                "--db-user=%s" % params.test_db_user,
                "--db-port=%s" % params.test_db_port,
                "--db-user-password=%s" % params.test_db_user_password,
                "--mode=init",
                "--debug",
            ]
        )

        ctx = Context(args)

        db_conn = await asyncpg.connect(**ctx.conn_params)
        await DBOperations.init_db(db_conn, params.test_source_db)
        await DBOperations.init_db(db_conn, params.test_target_db)
        await DBOperations.init_db(db_conn, params.test_target_db + "_2")
        await DBOperations.init_db(db_conn, params.test_target_db + "_3")
        await DBOperations.init_db(db_conn, params.test_target_db + "_4")
        await DBOperations.init_db(db_conn, params.test_target_db + "_5")
        await DBOperations.init_db(db_conn, params.test_target_db + "_6")
        await DBOperations.init_db(db_conn, params.test_target_db + "_7")  # for PGAnonValidateUnitTest 04
        await DBOperations.init_db(db_conn, params.test_target_db + "_8")  # for PGAnonValidateUnitTest 05
        await db_conn.close()

        sourse_db_params = ctx.conn_params.copy()
        sourse_db_params["database"] = params.test_source_db

        print("============> Started init_env")
        db_conn = await asyncpg.connect(**sourse_db_params)
        await DBOperations.init_env(db_conn, "init_env.sql", params.test_scale)
        await db_conn.close()
        print("<============ Finished init_env")

        args = parser.parse_args(
            [
                "--db-host=%s" % params.test_db_host,
                "--db-name=%s" % params.test_source_db,
                "--db-user=%s" % params.test_db_user,
                "--db-port=%s" % params.test_db_port,
                "--db-user-password=%s" % params.test_db_user_password,
                "--mode=init",
                "--verbose=debug",
                "--debug",
            ]
        )

        res = await MainRoutine(args).run()
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

        parser = Context.get_arg_parser()
        args = parser.parse_args(
            [
                "--db-host=%s" % params.test_db_host,
                "--db-name=postgres",
                "--db-user=%s" % params.test_db_user,
                "--db-port=%s" % params.test_db_port,
                "--db-user-password=%s" % params.test_db_user_password,
                "--mode=init",
                "--debug",
            ]
        )

        ctx = Context(args)

        db_conn = await asyncpg.connect(**ctx.conn_params)
        await DBOperations.init_db_once(db_conn, params.test_source_db + "_stress")

        sourse_db_params = ctx.conn_params.copy()
        sourse_db_params["database"] = params.test_source_db + "_stress"

        args = parser.parse_args(
            [
                "--db-host=%s" % params.test_db_host,
                "--db-name=%s" % params.test_source_db + "_stress",
                "--db-user=%s" % params.test_db_user,
                "--db-port=%s" % params.test_db_port,
                "--db-user-password=%s" % params.test_db_user_password,
                "--mode=init",
                "--verbose=debug",
                "--debug",
            ]
        )

        res = await MainRoutine(args).run()

        schema_exists = await db_conn.fetch(
            """select nspname from pg_namespace where nspname = 'stress'"""
        )
        await db_conn.close()
        if len(schema_exists) == 0:
            print("============> Started init_stress_env")
            db_conn = await asyncpg.connect(**sourse_db_params)
            await DBOperations.init_env(
                db_conn, "init_stress_env.sql", params.test_scale
            )
            await db_conn.close()
            print("<============ Finished init_stress_env")
        else:
            print("============> Schema 'stress' already exists")

        if res.result_code != ResultCode.DONE:
            raise ValueError("Init stress env failed")

        passed_stages.append("init_stress_env")
        return res

    async def check_rows(self, args, schema, table, fields, rows):
        ctx = Context(args)
        db_conn = await asyncpg.connect(**ctx.conn_params)
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

    async def check_list_tables_and_fields(self, source_args, target_args):
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

        ctx = Context(source_args)
        db_conn = await asyncpg.connect(**ctx.conn_params)
        db_source_rows = recordset_to_list_flat(await db_conn.fetch(query))
        await db_conn.close()

        ctx = Context(target_args)
        db_conn = await asyncpg.connect(**ctx.conn_params)
        db_target_rows = recordset_to_list_flat(await db_conn.fetch(query))
        await db_conn.close()

        not_found_in_target = [x for x in db_source_rows if x not in db_target_rows]
        # not_found_in_source = [x for x in db_target_rows if x not in db_source_rows]

        return not_found_in_target

    async def check_rows_count(self, args, objs) -> bool:
        failed_objs = []
        ctx = Context(args)
        db_conn = await asyncpg.connect(**ctx.conn_params)

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

    async def check_list_tables(self, args, expected_tables_list) -> bool:
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

        ctx = Context(args)
        db_conn = await asyncpg.connect(**ctx.conn_params)
        db_rows = recordset_to_list_flat(await db_conn.fetch(query))
        await db_conn.close()

        not_found_tables = [x for x in expected_tables_list if x not in db_rows]
        for v in not_found_tables:
            print("check_list_tables: Table %s not found!" % str(v))
        for v in [x for x in db_rows if x not in expected_tables_list]:
            print("check_list_tables: Found unexpected table %s!" % str(v))

        return len(not_found_tables) == 0 and len(db_rows) == len(expected_tables_list)

    async def get_list_tables_with_diff_data(self, source_args, target_args):
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

        ctx = Context(source_args)
        db_conn = await asyncpg.connect(**ctx.conn_params)
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

        ctx = Context(target_args)
        db_conn = await asyncpg.connect(**ctx.conn_params)
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

    def save_and_compare_result(self, file_name, list_objects):
        current_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)))
        to_json(list_objects, formatted=True)
        with open(os.path.join(current_dir, file_name), "w") as out_file:
            out_file.write(to_json(list_objects, formatted=True))

        try:
            orig_data = open(os.path.join(current_dir, file_name + ".result"), "r")
            orig_content = orig_data.read()
            orig_data.close()
            orig_content_obj = eval(orig_content)
        except:
            print(
                "Needs to create %s with content: %s"
                % (
                    os.path.join(current_dir, file_name + ".result"),
                    to_json(list_objects, formatted=True),
                )
            )

        try:
            current_data = open(os.path.join(current_dir, file_name), "r")
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
        parser = Context.get_arg_parser()
        args = parser.parse_args(
            [
                f"--db-host={params.test_db_host}",
                f"--db-name={params.test_source_db}",
                f"--db-user={params.test_db_user}",
                f"--db-port={params.test_db_port}",
                f"--db-user-password={params.test_db_user_password}",
                f"--mode=create-dict",
            ]
        )
        self.assertIsNone(args.meta_dict_files)
        self.assertIsNone(args.prepared_sens_dict_files)
        self.assertIsNone(args.prepared_no_sens_dict_files)

    def test_all_list_arguments_filled_with_simple_value(self):
        meta_dict_file_name = 'test.py'
        prepared_sens_dict_file_name = 'prepared_sens_dict_file.py'
        prepared_no_sens_dict_file_name = 'prepared_no_sens_dict_file.py'

        parser = Context.get_arg_parser()
        args = parser.parse_args(
            [
                f"--db-host={params.test_db_host}",
                f"--db-name={params.test_source_db}",
                f"--db-user={params.test_db_user}",
                f"--db-port={params.test_db_port}",
                f"--db-user-password={params.test_db_user_password}",
                f"--meta-dict-file={meta_dict_file_name}",
                f"--prepared-sens-dict-file={prepared_sens_dict_file_name}",
                f"--prepared-no-sens-dict-file={prepared_no_sens_dict_file_name}",
            ]
        )
        self.assertEqual([meta_dict_file_name], args.meta_dict_files)
        self.assertEqual([prepared_sens_dict_file_name], args.prepared_sens_dict_files)
        self.assertEqual([prepared_no_sens_dict_file_name], args.prepared_no_sens_dict_files)

    def test_all_list_arguments_filled_with_list_values(self):
        meta_dict_file_names = ['test.py', 'meta_dict_file.py']
        prepared_sens_dict_file_names = ['prepared_sens_dict_file.py', 'another_prepared_sens_dict_file.py']
        prepared_no_sens_dict_file_names = ['prepared_no_sens_dict_file.py', 'another_prepared_no_sens_dict_file.py']

        parser = Context.get_arg_parser()
        args = parser.parse_args(
            [
                f"--db-host={params.test_db_host}",
                f"--db-name={params.test_source_db}",
                f"--db-user={params.test_db_user}",
                f"--db-port={params.test_db_port}",
                f"--db-user-password={params.test_db_user_password}",
                f"--meta-dict-file={','.join(meta_dict_file_names)}",
                f"--prepared-sens-dict-file={','.join(prepared_sens_dict_file_names)}",
                f"--prepared-no-sens-dict-file={','.join(prepared_no_sens_dict_file_names)}",
            ]
        )
        self.assertEqual(meta_dict_file_names, args.meta_dict_files)
        self.assertEqual(prepared_sens_dict_file_names, args.prepared_sens_dict_files)
        self.assertEqual(prepared_no_sens_dict_file_names, args.prepared_no_sens_dict_files)


class PGAnonUnitTest(unittest.IsolatedAsyncioTestCase, BasicUnitTest):
    async def test_01_init(self):
        res = await self.init_env()
        self.assertEqual(res.result_code, ResultCode.DONE)

    async def test_02_dump(self):
        self.assertTrue("init_env" in passed_stages)

        parser = Context.get_arg_parser()
        args = parser.parse_args(
            [
                "--db-host=%s" % params.test_db_host,
                "--db-name=%s" % params.test_source_db,
                "--db-user=%s" % params.test_db_user,
                "--db-port=%s" % params.test_db_port,
                "--db-user-password=%s" % params.test_db_user_password,
                "--mode=dump",
                "--prepared-sens-dict-file=test.py",
                "--threads=%s" % params.test_threads,
                "--clear-output-dir",
                "--verbose=debug",
                "--debug",
            ]
        )

        res = await MainRoutine(args).run()
        self.assertEqual(res.result_code, ResultCode.DONE)
        passed_stages.append("test_02_dump")

    async def test_03_restore(self):
        self.assertTrue("test_02_dump" in passed_stages)

        parser = Context.get_arg_parser()
        args = parser.parse_args(
            [
                "--db-host=%s" % params.test_db_host,
                "--db-name=%s" % params.test_target_db,
                "--db-user=%s" % params.test_db_user,
                "--db-port=%s" % params.test_db_port,
                "--db-user-password=%s" % params.test_db_user_password,
                "--threads=%s" % params.test_threads,
                "--mode=restore",
                "--input-dir=test",
                "--drop-custom-check-constr",
                "--verbose=debug",
                "--debug",
            ]
        )

        res = await MainRoutine(args).run()
        self.assertEqual(res.result_code, ResultCode.DONE)

    async def test_04_dump(self):
        self.assertTrue("init_env" in passed_stages)

        parser = Context.get_arg_parser()
        args = parser.parse_args(
            [
                "--db-host=%s" % params.test_db_host,
                "--db-name=%s" % params.test_source_db,
                "--db-user=%s" % params.test_db_user,
                "--db-port=%s" % params.test_db_port,
                "--db-user-password=%s" % params.test_db_user_password,
                "--mode=dump",
                "--prepared-sens-dict-file=test_exclude.py",
                "--threads=%s" % params.test_threads,
                "--clear-output-dir",
                "--verbose=debug",
                "--debug",
            ]
        )

        res = await MainRoutine(args).run()
        self.assertEqual(res.result_code, ResultCode.DONE)
        passed_stages.append("test_04_dump")

    async def test_05_restore(self):
        self.assertTrue("test_04_dump" in passed_stages)

        parser = Context.get_arg_parser()
        args = parser.parse_args(
            [
                "--db-host=%s" % params.test_db_host,
                "--db-name=%s" % params.test_target_db + "_2",
                "--db-user=%s" % params.test_db_user,
                "--db-port=%s" % params.test_db_port,
                "--db-user-password=%s" % params.test_db_user_password,
                "--threads=%s" % params.test_threads,
                "--mode=restore",
                "--input-dir=test_exclude",
                "--drop-custom-check-constr",
                "--verbose=debug",
                "--debug",
            ]
        )

        res = await MainRoutine(args).run()
        self.assertEqual(res.result_code, ResultCode.DONE)

        args = parser.parse_args(
            [
                "--db-host=%s" % params.test_db_host,
                "--db-name=%s" % params.test_target_db + "_2",
                "--db-user=%s" % params.test_db_user,
                "--db-port=%s" % params.test_db_port,
                "--db-user-password=%s" % params.test_db_user_password,
                "--threads=%s" % params.test_threads,
                "--prepared-sens-dict-file=test_exclude.py",
                "--verbose=debug",
                "--debug",
            ]
        )
        res = await MainRoutine(args).validate_target_tables()
        self.assertEqual(res.result_code, ResultCode.DONE)
        passed_stages.append("test_05_restore")

    async def test_06_sync_struct(self):
        self.assertTrue("test_05_restore" in passed_stages)

        parser = Context.get_arg_parser()
        args = parser.parse_args(
            [
                "--db-host=%s" % params.test_db_host,
                "--db-name=%s" % params.test_source_db,
                "--db-user=%s" % params.test_db_user,
                "--db-port=%s" % params.test_db_port,
                "--db-user-password=%s" % params.test_db_user_password,
                "--threads=%s" % params.test_threads,
                "--mode=sync-struct-dump",
                "--prepared-sens-dict-file=test_sync_struct.py",
                "--verbose=debug",
                "--clear-output-dir",
                "--debug",
            ]
        )

        res = await MainRoutine(args).run()
        self.assertEqual(res.result_code, ResultCode.DONE)

        args = parser.parse_args(
            [
                "--db-host=%s" % params.test_db_host,
                "--db-name=%s" % params.test_target_db
                + "_3",  # here will be created 3 empty tables
                "--db-user=%s" % params.test_db_user,
                "--db-port=%s" % params.test_db_port,
                "--db-user-password=%s" % params.test_db_user_password,
                "--threads=%s" % params.test_threads,
                "--mode=sync-struct-restore",
                "--input-dir=test_sync_struct",
                "--verbose=debug",
                "--debug",
            ]
        )

        res = await MainRoutine(args).run()
        self.assertEqual(res.result_code, ResultCode.DONE)

        self.assertTrue(
            await self.check_list_tables(
                args,
                [  # TODO: get list tables from specific dict
                    ["schm_other_2", "exclude_tbl"],
                    ["schm_other_2", "some_tbl"],
                    ["schm_mask_include_1", "tbl_123"],
                ],
            )
        )

        objs = [
            ["schm_other_2", "exclude_tbl", 0],
            ["schm_other_2", "some_tbl", 0],
            ["schm_mask_include_1", "tbl_123", 0],
        ]
        self.assertTrue(await self.check_rows_count(args, objs))

        self.assertEqual(res.result_code, ResultCode.DONE)
        passed_stages.append("test_06_sync_struct")

    async def test_07_sync_data(self):
        # --mode=sync-data-dump ---> --mode=sync-data-restore [3 empty tables already exists]
        self.assertTrue("test_06_sync_struct" in passed_stages)

        parser = Context.get_arg_parser()
        args = parser.parse_args(
            [
                "--db-host=%s" % params.test_db_host,
                "--db-name=%s" % params.test_source_db,
                "--db-user=%s" % params.test_db_user,
                "--db-port=%s" % params.test_db_port,
                "--db-user-password=%s" % params.test_db_user_password,
                "--threads=%s" % params.test_threads,
                "--mode=sync-data-dump",
                "--prepared-sens-dict-file=test_sync_data.py",  # data will be saved to "output/test_sync_data"
                "--verbose=debug",
                "--clear-output-dir",
                "--debug",
            ]
        )

        res = await MainRoutine(args).run()
        self.assertEqual(res.result_code, ResultCode.DONE)

        args = parser.parse_args(
            [
                "--db-host=%s" % params.test_db_host,
                "--db-name=%s" % params.test_target_db
                + "_3",  # here target DB have 3 empty tables
                "--db-user=%s" % params.test_db_user,
                "--db-port=%s" % params.test_db_port,
                "--db-user-password=%s" % params.test_db_user_password,
                "--threads=%s" % params.test_threads,
                "--mode=sync-data-restore",
                "--input-dir=test_sync_data",
                "--verbose=debug",
                "--debug",
            ]
        )

        res = await MainRoutine(args).run()
        self.assertEqual(res.result_code, ResultCode.DONE)

        self.assertTrue(
            await self.check_list_tables(
                args,
                [  # TODO: get list tables from specific dict
                    ["schm_other_2", "exclude_tbl"],
                    ["schm_other_2", "some_tbl"],
                    ["schm_mask_include_1", "tbl_123"],
                ],
            )
        )

        objs = [
            ["schm_other_2", "exclude_tbl", rows_in_init_env * int(params.test_scale)],
            ["schm_other_2", "some_tbl", rows_in_init_env * int(params.test_scale)],
            [
                "schm_mask_include_1",
                "tbl_123",
                rows_in_init_env * int(params.test_scale),
            ],
        ]
        self.assertTrue(await self.check_rows_count(args, objs))

        rows = [[3, "t***l_3"], [4, "t***l_4"]]
        self.assertTrue(
            await self.check_rows(args, "schm_mask_include_1", "tbl_123", None, rows)
        )

        self.assertEqual(res.result_code, ResultCode.DONE)
        passed_stages.append("test_07_sync_data")

    async def test_08_sync_data(self):
        # --mode=sync-data-dump ---> --mode=sync-data-restore [target DB is not empty]
        self.assertTrue("test_07_sync_data" in passed_stages)

        parser = Context.get_arg_parser()
        args = parser.parse_args(
            [
                "--db-host=%s" % params.test_db_host,
                "--db-name=%s" % params.test_source_db,
                "--db-user=%s" % params.test_db_user,
                "--db-port=%s" % params.test_db_port,
                "--db-user-password=%s" % params.test_db_user_password,
                "--threads=%s" % params.test_threads,
                "--mode=sync-data-dump",
                "--prepared-sens-dict-file=test_sync_data_2.py",
                "--verbose=debug",
                "--clear-output-dir",
                "--debug",
            ]
        )

        res = await MainRoutine(args).run()
        self.assertEqual(res.result_code, ResultCode.DONE)

        args = parser.parse_args(
            [
                "--db-host=%s" % params.test_db_host,
                "--db-name=%s" % params.test_target_db,  # here target DB is NOT empty
                "--db-user=%s" % params.test_db_user,
                "--db-port=%s" % params.test_db_port,
                "--db-user-password=%s" % params.test_db_user_password,
                "--threads=%s" % params.test_threads,
                "--mode=sync-data-restore",  # just sync data of specific tables from test_sync_data_2.py
                "--input-dir=test_sync_data_2",
                "--verbose=debug",
                "--debug",
            ]
        )

        ctx = Context(args)
        db_conn = await asyncpg.connect(**ctx.conn_params)
        # pg_anon does not clear tables on its own
        await db_conn.execute("TRUNCATE TABLE schm_other_1.some_tbl")  # manual clean
        await db_conn.execute("TRUNCATE TABLE schm_other_2.some_tbl")
        await db_conn.close()

        res = await MainRoutine(args).run()
        self.assertEqual(res.result_code, ResultCode.DONE)

        objs = [
            ["schm_other_1", "some_tbl", rows_in_init_env * int(params.test_scale)],
            ["schm_other_2", "some_tbl", rows_in_init_env * int(params.test_scale)],
        ]
        self.assertTrue(await self.check_rows_count(args, objs))

        self.assertEqual(res.result_code, ResultCode.DONE)
        passed_stages.append("test_08_sync_data")


class PGAnonValidateUnitTest(unittest.IsolatedAsyncioTestCase, BasicUnitTest):
    async def test_01_init(self):
        res = await self.init_env()
        self.assertEqual(res.result_code, ResultCode.DONE)

    async def test_02_sync_struct_for_validate(self):
        self.assertTrue("init_env" in passed_stages)

        parser = Context.get_arg_parser()
        args = parser.parse_args(
            [
                "--db-host=%s" % params.test_db_host,
                "--db-name=%s" % params.test_source_db,
                "--db-user=%s" % params.test_db_user,
                "--db-port=%s" % params.test_db_port,
                "--db-user-password=%s" % params.test_db_user_password,
                "--threads=%s" % params.test_threads,
                "--mode=sync-struct-dump",
                "--prepared-sens-dict-file=test_dbg_stages.py",
                "--verbose=debug",
                "--clear-output-dir",
                "--debug",
                "--output-dir=test_02_sync_struct_for_validate",
                "--dbg-stage-3-validate-full"  # for not allowing post-data
            ]
        )

        res_dump = await MainRoutine(args).run()
        self.assertEqual(res_dump.result_code, ResultCode.DONE)

        args = parser.parse_args(
            [
                "--db-host=%s" % params.test_db_host,
                "--db-name=%s" % params.test_target_db + "_7",
                "--db-user=%s" % params.test_db_user,
                "--db-port=%s" % params.test_db_port,
                "--db-user-password=%s" % params.test_db_user_password,
                "--threads=%s" % params.test_threads,
                "--mode=sync-struct-restore",
                "--input-dir=test_02_sync_struct_for_validate",
                "--verbose=debug",
                "--debug",
            ]
        )

        res_restore = await MainRoutine(args).run()
        self.assertEqual(res_restore.result_code, ResultCode.DONE)
        passed_stages.append("test_02_sync_struct_for_validate")

    async def test_03_validate_dict(self):
        self.assertTrue("test_02_sync_struct_for_validate" in passed_stages)

        parser = Context.get_arg_parser()
        args = parser.parse_args(
            [
                "--db-host=%s" % params.test_db_host,
                "--db-name=%s" % params.test_source_db,
                "--db-user=%s" % params.test_db_user,
                "--db-port=%s" % params.test_db_port,
                "--db-user-password=%s" % params.test_db_user_password,
                "--mode=dump",
                "--prepared-sens-dict-file=test_dbg_stages.py",
                "--threads=%s" % params.test_threads,
                "--clear-output-dir",
                "--verbose=debug",
                "--debug",
                "--dbg-stage-1-validate-dict",
                "--output-dir=test_03_validate_dict",
            ]
        )

        res = await MainRoutine(args).run()
        self.assertEqual(res.result_code, ResultCode.DONE)
        passed_stages.append("test_03_validate_dict")

    async def test_04_validate_data(self):
        self.assertTrue("test_03_validate_dict" in passed_stages)

        parser = Context.get_arg_parser()
        args = parser.parse_args(
            [
                "--db-host=%s" % params.test_db_host,
                "--db-name=%s" % params.test_source_db,
                "--db-user=%s" % params.test_db_user,
                "--db-port=%s" % params.test_db_port,
                "--db-user-password=%s" % params.test_db_user_password,
                "--mode=dump",
                "--prepared-sens-dict-file=test_dbg_stages.py",
                "--threads=%s" % params.test_threads,
                "--clear-output-dir",
                "--verbose=debug",
                "--debug",
                "--dbg-stage-2-validate-data",
                "--output-dir=test_04_validate_data",
            ]
        )

        res_dump = await MainRoutine(args).run()
        self.assertEqual(res_dump.result_code, ResultCode.DONE)

        args = parser.parse_args(
            [
                "--db-host=%s" % params.test_db_host,
                "--db-name=%s" % params.test_target_db + "_7",
                "--db-user=%s" % params.test_db_user,
                "--db-port=%s" % params.test_db_port,
                "--db-user-password=%s" % params.test_db_user_password,
                "--mode=sync-data-restore",
                "--threads=%s" % params.test_threads,
                # "--threads=1",
                "--verbose=debug",
                "--debug",
                "--input-dir=test_04_validate_data",
            ]
        )

        res_restore = await MainRoutine(args).run()
        self.assertEqual(res_restore.result_code, ResultCode.DONE)

        passed_stages.append("test_04_validate_data")

    async def test_05_validate_full(self):
        self.assertTrue("test_04_validate_data" in passed_stages)

        parser = Context.get_arg_parser()
        args = parser.parse_args(
            [
                "--db-host=%s" % params.test_db_host,
                "--db-name=%s" % params.test_source_db,
                "--db-user=%s" % params.test_db_user,
                "--db-port=%s" % params.test_db_port,
                "--db-user-password=%s" % params.test_db_user_password,
                "--mode=dump",
                "--prepared-sens-dict-file=test_dbg_stages.py",
                "--threads=%s" % params.test_threads,
                "--clear-output-dir",
                "--verbose=debug",
                "--debug",
                "--dbg-stage-3-validate-full",
                "--output-dir=test_05_validate_full",
            ]
        )

        res_dump = await MainRoutine(args).run()
        self.assertEqual(res_dump.result_code, ResultCode.DONE)

        args = parser.parse_args(
            [
                "--db-host=%s" % params.test_db_host,
                "--db-name=%s" % params.test_target_db + "_8",
                "--db-user=%s" % params.test_db_user,
                "--db-port=%s" % params.test_db_port,
                "--db-user-password=%s" % params.test_db_user_password,
                "--mode=restore",
                "--threads=%s" % params.test_threads,
                "--verbose=debug",
                "--debug",
                "--input-dir=test_05_validate_full",
            ]
        )

        res_restore = await MainRoutine(args).run()
        self.assertEqual(res_restore.result_code, ResultCode.DONE)

        passed_stages.append("test_05_validate_full")


class PGAnonDictGenUnitTest(unittest.IsolatedAsyncioTestCase, BasicUnitTest):
    target_sens_dict = "test_prepared_sens_dict_result.py"
    target_sens_dict_expected = "test_prepared_sens_dict_result_expected.py"
    
    args = {}

    async def test_01_init(self):
        res = await self.init_env()
        self.assertEqual(res.result_code, ResultCode.DONE)

    async def test_02_create_dict(self):
        self.assertTrue("init_env" in passed_stages)

        target_no_sens_dict = "test_prepared_no_sens_dict_result.py"
        target_no_sens_dict_expected = "test_prepared_no_sens_dict_result_expected.py"

        parser = Context.get_arg_parser()
        self.args_create_dict = parser.parse_args(
            [
                f"--db-host={params.test_db_host}",
                f"--db-name={params.test_source_db}",
                f"--db-user={params.test_db_user}",
                f"--db-port={params.test_db_port}",
                f"--db-user-password={params.test_db_user_password}",
                "--mode=create-dict",
                "--scan-mode=full",
                "--meta-dict-file=test_meta_dict.py",
                f"--output-sens-dict-file={self.target_sens_dict}",
                f"--output-no-sens-dict-file={target_no_sens_dict}",
                f"--threads={params.test_threads}",
                "--scan-partial-rows=10000",
                "--verbose=debug",
                "--debug",
            ]
        )

        res = await MainRoutine(self.args_create_dict).run()
        parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
        target_sens_dict_file = os.path.join(parent_dir, "dict", self.target_sens_dict)
        target_sens_dict_expected_file = os.path.join(parent_dir, "dict", self.target_sens_dict_expected)
        target_no_sens_dict_file = os.path.join(parent_dir, "dict", target_no_sens_dict)
        target_no_sens_dict_expected_file = os.path.join(parent_dir, "dict", target_no_sens_dict_expected)

        self.assertTrue(os.path.exists(target_sens_dict_file))
        self.assertTrue(os.path.exists(target_no_sens_dict_file))

        # Checking sens dict
        with (open(target_sens_dict_file, "r", encoding="utf-8") as file1,
              open(target_sens_dict_expected_file, "r", encoding="utf-8") as file2):
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

            print(f"============> Started comparison of {self.target_sens_dict} and {self.target_sens_dict_expected}.py")

            for line in iterate_dict_level_1(d2):
                expected_result_list_of_iterate_dict.append(line)

            for line in iterate_dict_level_1(d1):
                result_list_of_iterate_dict.append(line)
                if line not in expected_result_list_of_iterate_dict:
                    flag_of_identity = False
                    print(f"check_comparison: row {line} not found in {self.target_sens_dict}")

            if flag_of_identity:
                for line in iterate_dict_level_1(d2):
                    if line not in result_list_of_iterate_dict:
                        flag_of_identity = False
                        print(f"check_comparison: row {line} not found in {self.target_sens_dict_expected}.py")

            print(f"<============ Finished comparison of {self.target_sens_dict} and {self.target_sens_dict_expected}.py")

        self.assertTrue(flag_of_identity)

        print(f"============> Started comparison of {target_no_sens_dict} and {target_no_sens_dict_expected}.py")
        # Checking no-sens dict
        with (open(target_no_sens_dict_file, "r", encoding="utf-8") as file1,
              open(target_no_sens_dict_expected_file, "r", encoding="utf-8") as file2):
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

        print(f"<============ Finished comparison of {target_no_sens_dict} and {target_no_sens_dict_expected}.py")

        self.assertEqual(res.result_code, ResultCode.DONE)
        passed_stages.append("test_02_create_dict")

    async def test_03_dump(self):
        self.assertTrue("init_env" in passed_stages)

        parser = Context.get_arg_parser()
        args = copy.deepcopy(
            parser.parse_args(
                [
                    f"--db-host={params.test_db_host}",
                    f"--db-name={params.test_source_db}",
                    f"--db-user={params.test_db_user}",
                    f"--db-port={params.test_db_port}",
                    f"--db-user-password={params.test_db_user_password}",
                    "--mode=dump",
                    f"--prepared-sens-dict-file={self.target_sens_dict}",
                    f"--threads={params.test_threads}",
                    "--clear-output-dir",
                    "--verbose=debug",
                    "--debug",
                ]
            )
        )
        self.args["dump"] = copy.deepcopy(args)
        res = await MainRoutine(args).run()
        self.assertEqual(res.result_code, ResultCode.DONE)
        passed_stages.append("test_03_dump")

    async def test_04_restore(self):
        self.assertTrue("test_03_dump" in passed_stages)

        parser = Context.get_arg_parser()
        args = parser.parse_args(
            [
                f"--db-host={params.test_db_host}",
                f"--db-name={params.test_target_db}_4",
                f"--db-user={params.test_db_user}",
                f"--db-port={params.test_db_port}",
                f"--db-user-password={params.test_db_user_password}",
                f"--threads={params.test_threads}",
                "--mode=restore",
                f"--input-dir={self.target_sens_dict.split('.')[0]}",
                "--drop-custom-check-constr",
                "--verbose=debug",
                "--debug",
            ]
        )
        self.args["restore"] = copy.deepcopy(args)
        res = await MainRoutine(args).run()
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
                args, "schm_mask_ext_exclude_2", "card_numbers", None, rows
            )
        )

        objs = [
            [
                "schm_mask_ext_exclude_2",
                "card_numbers",
                rows_in_init_env * int(params.test_scale) * 3,
            ]  # see init_env.sql
        ]
        self.assertTrue(await self.check_rows_count(args, objs))

        not_found_in_target = await self.check_list_tables_and_fields(
            self.args["dump"], self.args["restore"]
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

        prepared_no_sens_dict = "test_prepared_no_sens_dict_result.py"
        target_no_sens_dict = "test_prepared_no_sens_dict_result_repeat.py"

        parser = Context.get_arg_parser()
        self.args_create_dict = parser.parse_args(
            [
                f"--db-host={params.test_db_host}",
                f"--db-name={params.test_source_db}",
                f"--db-user={params.test_db_user}",
                f"--db-port={params.test_db_port}",
                f"--db-user-password={params.test_db_user_password}",
                "--mode=create-dict",
                "--scan-mode=full",
                "--meta-dict-file=test_meta_dict.py",
                f"--output-sens-dict-file={self.target_sens_dict}",
                f"--output-no-sens-dict-file={target_no_sens_dict}",
                f"--prepared-no-sens-dict-file={prepared_no_sens_dict}",
                f"--threads={params.test_threads}",
                "--scan-partial-rows=10000",
                "--verbose=debug",
                "--debug",
            ]
        )

        res = await MainRoutine(self.args_create_dict).run()
        parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
        target_sens_dict_file = os.path.join(parent_dir, "dict", self.target_sens_dict)
        target_sens_dict_expected_file = os.path.join(parent_dir, "dict", self.target_sens_dict_expected)
        prepared_no_sens_dict_file = os.path.join(parent_dir, "dict", prepared_no_sens_dict)
        target_no_sens_dict_file = os.path.join(parent_dir, "dict", target_no_sens_dict)

        self.assertTrue(os.path.exists(target_sens_dict_file))
        self.assertTrue(os.path.exists(target_no_sens_dict_file))

        # Checking sens dict
        with (open(target_sens_dict_file, "r", encoding="utf-8") as file1,
              open(target_sens_dict_expected_file, "r", encoding="utf-8") as file2):
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

            print(f"============> Started comparison of {self.target_sens_dict} and {self.target_sens_dict_expected}.py")

            for line in iterate_dict_level_1(d2):
                expected_result_list_of_iterate_dict.append(line)

            for line in iterate_dict_level_1(d1):
                result_list_of_iterate_dict.append(line)
                if line not in expected_result_list_of_iterate_dict:
                    flag_of_identity = False
                    print(f"check_comparison: row {line} not found in {self.target_sens_dict}")

            if flag_of_identity:
                for line in iterate_dict_level_1(d2):
                    if line not in result_list_of_iterate_dict:
                        flag_of_identity = False
                        print(f"check_comparison: row {line} not found in {self.target_sens_dict_expected}.py")

            print(f"<============ Finished comparison of {self.target_sens_dict} and {self.target_sens_dict_expected}.py")

        self.assertTrue(flag_of_identity)

        print(f"============> Started comparison of {target_no_sens_dict} and {prepared_no_sens_dict_file}.py")
        # Checking no-sens dict
        with (open(prepared_no_sens_dict_file, "r", encoding="utf-8") as file1,
              open(target_no_sens_dict_file, "r", encoding="utf-8") as file2):
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

        print(f"<============ Finished comparison of {target_no_sens_dict} and {prepared_no_sens_dict_file}.py")

        self.assertEqual(res.result_code, ResultCode.DONE)
        passed_stages.append("test_05_repeat_create_dict_with_no_sens_dict")

    async def test_06_repeat_create_dict_with_no_sens_dict_and_sens_dict(self):
        self.assertTrue("test_02_create_dict" in passed_stages)

        prepared_no_sens_dict = "test_prepared_no_sens_dict_result.py"
        target_no_sens_dict = "test_prepared_no_sens_dict_result_repeat.py"

        parser = Context.get_arg_parser()
        self.args_create_dict = parser.parse_args(
            [
                f"--db-host={params.test_db_host}",
                f"--db-name={params.test_source_db}",
                f"--db-user={params.test_db_user}",
                f"--db-port={params.test_db_port}",
                f"--db-user-password={params.test_db_user_password}",
                "--mode=create-dict",
                "--scan-mode=full",
                "--meta-dict-file=test_meta_dict.py",
                f"--output-sens-dict-file={self.target_sens_dict}",
                f"--output-no-sens-dict-file={target_no_sens_dict}",
                f"--prepared-sens-dict-file={self.target_sens_dict}",
                f"--prepared-no-sens-dict-file={prepared_no_sens_dict}",
                f"--threads={params.test_threads}",
                "--scan-partial-rows=10000",
                "--verbose=debug",
                "--debug",
            ]
        )

        res = await MainRoutine(self.args_create_dict).run()
        parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
        target_sens_dict_file = os.path.join(parent_dir, "dict", self.target_sens_dict)
        target_sens_dict_expected_file = os.path.join(parent_dir, "dict", self.target_sens_dict_expected)
        prepared_no_sens_dict_file = os.path.join(parent_dir, "dict", prepared_no_sens_dict)
        target_no_sens_dict_file = os.path.join(parent_dir, "dict", target_no_sens_dict)

        self.assertTrue(os.path.exists(target_sens_dict_file))
        self.assertTrue(os.path.exists(target_no_sens_dict_file))

        # Checking sens dict
        with (open(target_sens_dict_file, "r", encoding="utf-8") as file1,
              open(target_sens_dict_expected_file, "r", encoding="utf-8") as file2):
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

            print(f"============> Started comparison of {self.target_sens_dict} and {self.target_sens_dict_expected}.py")

            for line in iterate_dict_level_1(d2):
                expected_result_list_of_iterate_dict.append(line)

            for line in iterate_dict_level_1(d1):
                result_list_of_iterate_dict.append(line)
                if line not in expected_result_list_of_iterate_dict:
                    flag_of_identity = False
                    print(f"check_comparison: row {line} not found in {self.target_sens_dict}")

            if flag_of_identity:
                for line in iterate_dict_level_1(d2):
                    if line not in result_list_of_iterate_dict:
                        flag_of_identity = False
                        print(f"check_comparison: row {line} not found in {self.target_sens_dict_expected}.py")

            print(f"<============ Finished comparison of {self.target_sens_dict} and {self.target_sens_dict_expected}.py")

        self.assertTrue(flag_of_identity)

        print(f"============> Started comparison of {target_no_sens_dict} and {prepared_no_sens_dict_file}.py")
        # Checking no-sens dict
        with (open(prepared_no_sens_dict_file, "r", encoding="utf-8") as file1,
              open(target_no_sens_dict_file, "r", encoding="utf-8") as file2):
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

        print(f"<============ Finished comparison of {target_no_sens_dict} and {prepared_no_sens_dict_file}.py")

        self.assertEqual(res.result_code, ResultCode.DONE)
        passed_stages.append("test_06_repeat_create_dict_with_no_sens_dict_and_sens_dict")


class TmpResults:
    res_test_02 = None
    res_test_03 = None


tmp_results = TmpResults()


class PGAnonDictGenStressUnitTest(unittest.IsolatedAsyncioTestCase, BasicUnitTest):
    target_dict = "test_create_dict_result.py"
    args = {}

    async def test_01_stress_init(self):
        res = await self.init_stress_env()
        self.assertEqual(res.result_code, ResultCode.DONE)

    async def test_02_create_dict(self):
        self.assertTrue("init_stress_env" in passed_stages)

        parser = Context.get_arg_parser()
        self.args_create_dict = parser.parse_args(
            [
                "--db-host=%s" % params.test_db_host,
                "--db-name=%s" % params.test_source_db + "_stress",
                "--db-user=%s" % params.test_db_user,
                "--db-port=%s" % params.test_db_port,
                "--db-user-password=%s" % params.test_db_user_password,
                "--mode=create-dict",
                "--scan-mode=partial",
                "--meta-dict-file=test_meta_dict.py",
                "--output-sens-dict-file=stress_%s" % self.target_dict,
                # '--threads=%s' % params.test_threads,
                "--threads=4",
                "--processes=2",
                "--scan-partial-rows=100",  # ,
                # '--verbose=debug',
                # '--debug'
            ]
        )

        res = await MainRoutine(self.args_create_dict).run()
        self.assertEqual(res.result_code, ResultCode.DONE)
        tmp_results.res_test_02 = res.result_data["elapsed"]
        passed_stages.append("test_02_create_dict")

    async def test_03_create_dict(self):
        self.assertTrue("init_stress_env" in passed_stages)

        parser = Context.get_arg_parser()
        self.args_create_dict = parser.parse_args(
            [
                "--db-host=%s" % params.test_db_host,
                "--db-name=%s" % params.test_source_db + "_stress",
                "--db-user=%s" % params.test_db_user,
                "--db-port=%s" % params.test_db_port,
                "--db-user-password=%s" % params.test_db_user_password,
                "--mode=create-dict",
                "--scan-mode=full",
                "--meta-dict-file=test_meta_dict.py",
                "--output-sens-dict-file=stress_%s" % self.target_dict,
                # '--threads=%s' % params.test_threads,
                "--threads=4",
                "--processes=2",
                # '--verbose=debug',
                # '--debug'
            ]
        )

        res = await MainRoutine(self.args_create_dict).run()
        self.assertEqual(res.result_code, ResultCode.DONE)
        tmp_results.res_test_03 = res.result_data["elapsed"]
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
    args = {}

    async def test_01_init(self):
        res = await self.init_env()
        self.assertEqual(res.result_code, ResultCode.DONE)

    async def test_02_mask_dump(self):
        self.assertTrue("init_env" in passed_stages)

        parser = Context.get_arg_parser()
        args = parser.parse_args(
            [
                "--db-host=%s" % params.test_db_host,
                "--db-name=%s" % params.test_source_db,
                "--db-user=%s" % params.test_db_user,
                "--db-port=%s" % params.test_db_port,
                "--db-user-password=%s" % params.test_db_user_password,
                "--mode=dump",
                "--prepared-sens-dict-file=mask_test.py",
                "--threads=%s" % params.test_threads,
                "--clear-output-dir",
                "--verbose=debug",
                "--debug",  # , '--validate-dict'
            ]
        )

        self.args["dump"] = copy.deepcopy(args)
        res = await MainRoutine(args).run()
        self.assertEqual(res.result_code, ResultCode.DONE)
        passed_stages.append("test_02_mask_dump")

    async def test_03_mask_restore(self):
        self.assertTrue("test_02_mask_dump" in passed_stages)

        parser = Context.get_arg_parser()
        args = parser.parse_args(
            [
                "--db-host=%s" % params.test_db_host,
                "--db-name=%s" % params.test_target_db + "_5",
                "--db-user=%s" % params.test_db_user,
                "--db-port=%s" % params.test_db_port,
                "--db-user-password=%s" % params.test_db_user_password,
                "--threads=%s" % params.test_threads,
                "--mode=restore",
                "--input-dir=mask_test",
                "--drop-custom-check-constr",
                "--verbose=debug",
                "--debug",
            ]
        )

        self.args["restore"] = copy.deepcopy(args)
        res = await MainRoutine(args).run()
        self.assertEqual(res.result_code, ResultCode.DONE)

        rows = [[1, round(Decimal(101010), 2)], [2, round(Decimal(101010), 2)]]
        self.assertTrue(
            await self.check_rows(args, "public", "contracts", ["id", "amount"], rows)
        )

        rows = [[1, round(Decimal(202020), 2)], [2, round(Decimal(202020), 2)]]
        self.assertTrue(
            await self.check_rows(args, "public", "tbl_100", ["id", "amount"], rows)
        )

        source_tables, target_tables = await self.get_list_tables_with_diff_data(
            self.args["dump"], self.args["restore"]
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
