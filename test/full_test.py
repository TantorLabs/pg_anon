import copy
import unittest
import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from pg_anon import *


input_args = None
passed_stages = []
rows_in_init_env = 1512


class TestParams:
    test_db_user = 'anon_test_user'         # default value
    test_db_user_password = 'mYy5RexGsZ'
    test_db_host = '127.0.0.1'
    test_db_port = '5432'
    test_source_db = 'test_source_db'
    test_target_db = 'test_target_db'
    test_scale = '10'
    test_threads = 4

    def __init__(self):
        if os.environ.get('TEST_DB_USER') is not None:
            self.test_db_user = os.environ["TEST_DB_USER"]
        if os.environ.get('PGPASSWORD') is not None:
            self.test_db_user_password = os.environ["TEST_DB_USER_PASSWORD"]
        if os.environ.get('TEST_DB_USER_PASSWORD') is not None:
            self.test_db_user_password = os.environ["TEST_DB_USER_PASSWORD"]
        if os.environ.get('TEST_DB_HOST') is not None:
            self.test_db_host = os.environ["TEST_DB_HOST"]
        if os.environ.get('TEST_DB_PORT') is not None:
            self.test_db_port = os.environ["TEST_DB_PORT"]
        if os.environ.get('TEST_SOURCE_DB') is not None:
            self.test_source_db = os.environ["TEST_SOURCE_DB"]
        if os.environ.get('TEST_TARGET_DB') is not None:
            self.test_target_db = os.environ["TEST_TARGET_DB"]
        if os.environ.get('TEST_SCALE') is not None:
            self.test_scale = os.environ["TEST_SCALE"]
        if os.environ.get('TEST_THREADS') is not None:
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
                """ % db_name)

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
                """ % (db_name, params.test_db_user))
        except:
            print(exception_helper(show_traceback=True))

    @staticmethod
    async def init_test_env(db_conn, scale=1):
        current_dir = os.path.dirname(os.path.realpath(__file__))
        with open(os.path.join(current_dir, 'init_env.sql'), 'r', encoding="utf-8") as f:
            data = f.read()
        if int(scale) != 1:
            data = data.replace(str(rows_in_init_env), str(rows_in_init_env * int(scale)))
        await db_conn.execute(data)


class BasicUnitTest:
    async def init_env(self):
        parser = Context.get_arg_parser()
        args = parser.parse_args([
            '--db-host=%s' % params.test_db_host,
            '--db-name=postgres',
            '--db-user=%s' % params.test_db_user,
            '--db-port=%s' % params.test_db_port,
            '--db-user-password=%s' % params.test_db_user_password,
            '--mode=init',
            '--debug'
        ])

        ctx = Context(args)

        db_conn = await asyncpg.connect(**ctx.conn_params)
        await DBOperations.init_db(db_conn, params.test_source_db)
        await DBOperations.init_db(db_conn, params.test_target_db)
        await DBOperations.init_db(db_conn, params.test_target_db + "_2")
        await DBOperations.init_db(db_conn, params.test_target_db + "_3")
        await DBOperations.init_db(db_conn, params.test_target_db + "_4")
        await db_conn.close()

        sourse_db_params = ctx.conn_params.copy()
        sourse_db_params['database'] = params.test_source_db

        ctx.logger.info("============> Started init_test_env")
        db_conn = await asyncpg.connect(**sourse_db_params)
        await DBOperations.init_test_env(db_conn, params.test_scale)
        await db_conn.close()
        ctx.logger.info("<============ Finished init_test_env")

        args = parser.parse_args([
            '--db-host=%s' % params.test_db_host,
            '--db-name=%s' % params.test_source_db,
            '--db-user=%s' % params.test_db_user,
            '--db-port=%s' % params.test_db_port,
            '--db-user-password=%s' % params.test_db_user_password,
            '--mode=init',
            '--verbose=debug',
            '--debug'
        ])

        res = await MainRoutine(args).run()
        if res.result_code == ResultCode.DONE:
            passed_stages.append("test_01_init")
        return res

    async def check_rows(self, args, schema, table, rows):
        ctx = Context(args)
        db_conn = await asyncpg.connect(**ctx.conn_params)
        db_rows = await db_conn.fetch("""select * from "%s"."%s" limit 1000""" % (schema, table))
        db_rows_prepared = []
        for db_row in db_rows:
            db_row_prepared = []
            for _, v in dict(db_row).items():
                db_row_prepared.append(v)
            db_rows_prepared.append(db_row_prepared)

        for v in rows:
            if v not in db_rows_prepared:
                print("check_rows: row %s not found" % str(v))
                await db_conn.close()
                return False

        await db_conn.close()
        return True

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
                db_rows = await db_conn.fetch("""select count(1) from "%s"."%s" """ % (obj[0], obj[1]))
                if db_rows[0][0] != obj[2]:
                    failed_objs.append(obj)
                    print("check_rows_count: failed check %s, count is %d" % (str(obj), db_rows[0][0]))
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


class PGAnonUnitTest(unittest.IsolatedAsyncioTestCase, BasicUnitTest):
    async def test_01_init(self):
        res = await self.init_env()
        self.assertTrue(res.result_code == ResultCode.DONE)

    async def test_02_dump(self):
        if "test_01_init" not in passed_stages:
            self.assertTrue(False)

        parser = Context.get_arg_parser()
        args = parser.parse_args([
            '--db-host=%s' % params.test_db_host,
            '--db-name=%s' % params.test_source_db,
            '--db-user=%s' % params.test_db_user,
            '--db-port=%s' % params.test_db_port,
            '--db-user-password=%s' % params.test_db_user_password,
            '--mode=dump',
            '--dict-file=test.py',
            '--threads=%s' % params.test_threads,
            '--clear-output-dir',
            '--verbose=debug',
            '--debug'
        ])

        res = await MainRoutine(args).run()
        if res.result_code == ResultCode.DONE:
            passed_stages.append("test_02_dump")
        self.assertTrue(res.result_code == ResultCode.DONE)

    async def test_03_restore(self):
        if "test_02_dump" not in passed_stages:
            self.assertTrue(False)

        parser = Context.get_arg_parser()
        args = parser.parse_args([
            '--db-host=%s' % params.test_db_host,
            '--db-name=%s' % params.test_target_db,
            '--db-user=%s' % params.test_db_user,
            '--db-port=%s' % params.test_db_port,
            '--db-user-password=%s' % params.test_db_user_password,
            '--threads=%s' % params.test_threads,
            '--mode=restore',
            '--input-dir=test',
            '--drop-custom-check-constr',
            '--verbose=debug',
            '--debug'
        ])

        res = await MainRoutine(args).run()
        self.assertTrue(res.result_code == ResultCode.DONE)

    async def test_04_dump(self):
        if "test_01_init" not in passed_stages:
            self.assertTrue(False)

        parser = Context.get_arg_parser()
        args = parser.parse_args([
            '--db-host=%s' % params.test_db_host,
            '--db-name=%s' % params.test_source_db,
            '--db-user=%s' % params.test_db_user,
            '--db-port=%s' % params.test_db_port,
            '--db-user-password=%s' % params.test_db_user_password,
            '--mode=dump',
            '--dict-file=test_exclude.py',
            '--threads=%s' % params.test_threads,
            '--clear-output-dir',
            '--verbose=debug',
            '--debug'
        ])

        res = await MainRoutine(args).run()
        if res.result_code == ResultCode.DONE:
            passed_stages.append("test_04_dump")
        self.assertTrue(res.result_code == ResultCode.DONE)

    async def test_05restore(self):
        if "test_04_dump" not in passed_stages:
            self.assertTrue(False)

        parser = Context.get_arg_parser()
        args = parser.parse_args([
            '--db-host=%s' % params.test_db_host,
            '--db-name=%s' % params.test_target_db + "_2",
            '--db-user=%s' % params.test_db_user,
            '--db-port=%s' % params.test_db_port,
            '--db-user-password=%s' % params.test_db_user_password,
            '--threads=%s' % params.test_threads,
            '--mode=restore',
            '--input-dir=test_exclude',
            '--drop-custom-check-constr',
            '--verbose=debug',
            '--debug'
        ])

        res = await MainRoutine(args).run()
        self.assertTrue(res.result_code == ResultCode.DONE)

        args = parser.parse_args([
            '--db-host=%s' % params.test_db_host,
            '--db-name=%s' % params.test_target_db + "_2",
            '--db-user=%s' % params.test_db_user,
            '--db-port=%s' % params.test_db_port,
            '--db-user-password=%s' % params.test_db_user_password,
            '--threads=%s' % params.test_threads,
            '--dict-file=test_exclude.py',
            '--verbose=debug',
            '--debug'
        ])
        res = await MainRoutine(args).validate_target_tables()
        self.assertTrue(res.result_code == ResultCode.DONE)
        if res.result_code == ResultCode.DONE:
            passed_stages.append("test_05restore")


    async def test_06sync_struct(self):
        if "test_05restore" not in passed_stages:
            self.assertTrue(False)

        parser = Context.get_arg_parser()
        args = parser.parse_args([
            '--db-host=%s' % params.test_db_host,
            '--db-name=%s' % params.test_source_db,
            '--db-user=%s' % params.test_db_user,
            '--db-port=%s' % params.test_db_port,
            '--db-user-password=%s' % params.test_db_user_password,
            '--threads=%s' % params.test_threads,
            '--mode=sync-struct-dump',
            '--dict-file=test_sync_struct.py',
            '--verbose=debug',
            '--clear-output-dir',
            '--debug'
        ])

        res = await MainRoutine(args).run()
        self.assertTrue(res.result_code == ResultCode.DONE)

        args = parser.parse_args([
            '--db-host=%s' % params.test_db_host,
            '--db-name=%s' % params.test_target_db + "_3",      # here will be created 3 empty tables
            '--db-user=%s' % params.test_db_user,
            '--db-port=%s' % params.test_db_port,
            '--db-user-password=%s' % params.test_db_user_password,
            '--threads=%s' % params.test_threads,
            '--mode=sync-struct-restore',
            '--input-dir=test_sync_struct',
            '--verbose=debug',
            '--debug'
        ])

        res = await MainRoutine(args).run()
        self.assertTrue(res.result_code == ResultCode.DONE)

        self.assertTrue(await self.check_list_tables(args, [    # TODO: get list tables from specific dict
            ['schm_other_2', 'exclude_tbl'],
            ['schm_other_2', 'some_tbl'],
            ['schm_mask_include_1', 'tbl_123']
        ]))

        objs = [
            ["schm_other_2", "exclude_tbl", 0],
            ["schm_other_2", "some_tbl", 0],
            ["schm_mask_include_1", "tbl_123", 0]
        ]
        self.assertTrue(await self.check_rows_count(args, objs))

        if res.result_code == ResultCode.DONE:
            passed_stages.append("test_06sync_struct")

    async def test_07sync_data(self):
        # --mode=sync-data-dump ---> --mode=sync-data-restore [3 empty tables already exists]
        if "test_06sync_struct" not in passed_stages:
            self.assertTrue(False)

        parser = Context.get_arg_parser()
        args = parser.parse_args([
            '--db-host=%s' % params.test_db_host,
            '--db-name=%s' % params.test_source_db,
            '--db-user=%s' % params.test_db_user,
            '--db-port=%s' % params.test_db_port,
            '--db-user-password=%s' % params.test_db_user_password,
            '--threads=%s' % params.test_threads,
            '--mode=sync-data-dump',
            '--dict-file=test_sync_data.py',    # data will be saved to "output/test_sync_data"
            '--verbose=debug',
            '--clear-output-dir',
            '--debug'
        ])

        res = await MainRoutine(args).run()
        self.assertTrue(res.result_code == ResultCode.DONE)

        args = parser.parse_args([
            '--db-host=%s' % params.test_db_host,
            '--db-name=%s' % params.test_target_db + "_3",  # here target DB have 3 empty tables
            '--db-user=%s' % params.test_db_user,
            '--db-port=%s' % params.test_db_port,
            '--db-user-password=%s' % params.test_db_user_password,
            '--threads=%s' % params.test_threads,
            '--mode=sync-data-restore',
            '--input-dir=test_sync_data',
            '--verbose=debug',
            '--debug'
        ])

        res = await MainRoutine(args).run()
        self.assertTrue(res.result_code == ResultCode.DONE)

        self.assertTrue(await self.check_list_tables(args, [    # TODO: get list tables from specific dict
            ['schm_other_2', 'exclude_tbl'],
            ['schm_other_2', 'some_tbl'],
            ['schm_mask_include_1', 'tbl_123']
        ]))

        objs = [
            ["schm_other_2", "exclude_tbl", rows_in_init_env * int(params.test_scale)],
            ["schm_other_2", "some_tbl", rows_in_init_env * int(params.test_scale)],
            ["schm_mask_include_1", "tbl_123", rows_in_init_env * int(params.test_scale)]
        ]
        self.assertTrue(await self.check_rows_count(args, objs))

        rows = [
            [3, 't***l_3'],
            [4, 't***l_4']
        ]
        self.assertTrue(await self.check_rows(args, "schm_mask_include_1", "tbl_123", rows))

    async def test_08sync_data(self):
        # --mode=sync-data-dump ---> --mode=sync-data-restore [target DB is not empty]
        if "test_06sync_struct" not in passed_stages:
            self.assertTrue(False)

        parser = Context.get_arg_parser()
        args = parser.parse_args([
            '--db-host=%s' % params.test_db_host,
            '--db-name=%s' % params.test_source_db,
            '--db-user=%s' % params.test_db_user,
            '--db-port=%s' % params.test_db_port,
            '--db-user-password=%s' % params.test_db_user_password,
            '--threads=%s' % params.test_threads,
            '--mode=sync-data-dump',
            '--dict-file=test_sync_data_2.py',
            '--verbose=debug',
            '--clear-output-dir',
            '--debug'
        ])

        res = await MainRoutine(args).run()
        self.assertTrue(res.result_code == ResultCode.DONE)

        args = parser.parse_args([
            '--db-host=%s' % params.test_db_host,
            '--db-name=%s' % params.test_target_db,     # here target DB is NOT empty
            '--db-user=%s' % params.test_db_user,
            '--db-port=%s' % params.test_db_port,
            '--db-user-password=%s' % params.test_db_user_password,
            '--threads=%s' % params.test_threads,
            '--mode=sync-data-restore',                 # just sync data of specific tables from test_sync_data_2.py
            '--input-dir=test_sync_data_2',
            '--verbose=debug',
            '--debug'
        ])

        ctx = Context(args)
        db_conn = await asyncpg.connect(**ctx.conn_params)
        # pg_anon does not clear tables on its own
        await db_conn.execute("TRUNCATE TABLE schm_other_1.some_tbl")   # manual clean
        await db_conn.execute("TRUNCATE TABLE schm_other_2.some_tbl")
        await db_conn.close()

        res = await MainRoutine(args).run()
        self.assertTrue(res.result_code == ResultCode.DONE)

        objs = [
            ["schm_other_1", "some_tbl", rows_in_init_env * int(params.test_scale)],
            ["schm_other_2", "some_tbl", rows_in_init_env * int(params.test_scale)]
        ]
        self.assertTrue(await self.check_rows_count(args, objs))


class PGAnonValidateUnitTest(unittest.IsolatedAsyncioTestCase, BasicUnitTest):
    async def test_01_init(self):
        if "test_06sync_struct" not in passed_stages:
            self.assertTrue(False)
        res = await self.init_env()
        self.assertTrue(res.result_code == ResultCode.DONE)

    async def test_01_validate(self):
        if "test_06sync_struct" not in passed_stages:
            self.assertTrue(False)

        parser = Context.get_arg_parser()
        args = parser.parse_args([
            '--db-host=%s' % params.test_db_host,
            '--db-name=%s' % params.test_source_db,
            '--db-user=%s' % params.test_db_user,
            '--db-port=%s' % params.test_db_port,
            '--db-user-password=%s' % params.test_db_user_password,
            '--mode=dump',
            '--dict-file=test.py',
            '--threads=%s' % params.test_threads,
            '--clear-output-dir',
            '--verbose=debug',
            '--debug',
            '--validate-dict',
            '--output-dir=test_01_validate'
        ])

        res = await MainRoutine(args).run()
        if res.result_code == ResultCode.DONE:
            passed_stages.append("test_01_validate")
        self.assertTrue(res.result_code == ResultCode.DONE)

    async def test_02_validate_full(self):
        if "test_01_validate" not in passed_stages:
            self.assertTrue(False)

        parser = Context.get_arg_parser()
        args = parser.parse_args([
            '--db-host=%s' % params.test_db_host,
            '--db-name=%s' % params.test_source_db,
            '--db-user=%s' % params.test_db_user,
            '--db-port=%s' % params.test_db_port,
            '--db-user-password=%s' % params.test_db_user_password,
            '--mode=dump',
            '--dict-file=test.py',
            '--threads=%s' % params.test_threads,
            '--clear-output-dir',
            '--verbose=debug',
            '--debug',
            '--validate-full',
            '--output-dir=test_02_validate_full'
        ])

        res = await MainRoutine(args).run()
        if res.result_code == ResultCode.DONE:
            passed_stages.append("test_02_validate_full")
        self.assertTrue(res.result_code == ResultCode.DONE)


class PGAnonDictGenUnitTest(unittest.IsolatedAsyncioTestCase, BasicUnitTest):
    target_dict = 'test_create_dict_result.py'
    args = {}

    async def test_01_init(self):
        res = await self.init_env()
        self.assertTrue(res.result_code == ResultCode.DONE)

    async def test_02_create_dict(self):
        if "test_01_init" not in passed_stages:
            self.assertTrue(False)

        parser = Context.get_arg_parser()
        self.args_create_dict = parser.parse_args([
            '--db-host=%s' % params.test_db_host,
            '--db-name=%s' % params.test_source_db,
            '--db-user=%s' % params.test_db_user,
            '--db-port=%s' % params.test_db_port,
            '--db-user-password=%s' % params.test_db_user_password,
            '--mode=create-dict',
            '--scan-mode=full',
            '--dict-file=test_create_dict.py',
            '--output-dict-file=%s' % self.target_dict,
            '--threads=%s' % params.test_threads,
            # '--threads=8',
            '--scan-partial-rows=10000',
            '--verbose=debug',
            '--debug'
        ])

        res = await MainRoutine(self.args_create_dict).run()
        if res.result_code == ResultCode.DONE:
            passed_stages.append("test_02_create_dict")

    async def test_03_dump(self):
        if "test_02_create_dict" not in passed_stages:
            self.assertTrue(False)

        parser = Context.get_arg_parser()
        args = copy.deepcopy(parser.parse_args([
            '--db-host=%s' % params.test_db_host,
            '--db-name=%s' % params.test_source_db,
            '--db-user=%s' % params.test_db_user,
            '--db-port=%s' % params.test_db_port,
            '--db-user-password=%s' % params.test_db_user_password,
            '--mode=dump',
            '--dict-file=%s' % self.target_dict,
            '--threads=%s' % params.test_threads,
            '--clear-output-dir',
            '--verbose=debug',
            '--debug'
        ]))
        self.args["dump"] = copy.deepcopy(args)
        res = await MainRoutine(args).run()
        if res.result_code == ResultCode.DONE:
            passed_stages.append("test_03_dump")
        self.assertTrue(res.result_code == ResultCode.DONE)

    async def test_04_restore(self):
        if "test_03_dump" not in passed_stages:
            self.assertTrue(False)

        parser = Context.get_arg_parser()
        args = parser.parse_args([
            '--db-host=%s' % params.test_db_host,
            '--db-name=%s' % params.test_target_db + "_4",
            '--db-user=%s' % params.test_db_user,
            '--db-port=%s' % params.test_db_port,
            '--db-user-password=%s' % params.test_db_user_password,
            '--threads=%s' % params.test_threads,
            '--mode=restore',
            '--input-dir=%s' % self.target_dict.split('.')[0],
            '--drop-custom-check-constr',
            '--verbose=debug',
            '--debug'
        ])
        self.args["restore"] = copy.deepcopy(args)
        res = await MainRoutine(args).run()
        self.assertTrue(res.result_code == ResultCode.DONE)

        rows = [
            [1, 'ccd778e5850ddf15d7e9a7ad11a8bbd8', 'invalid_val_1'],
            [2, '555da16355e56e162c12c95403419eea', 'invalid_val_2']
        ]
        self.assertTrue(await self.check_rows(args, "schm_mask_ext_exclude_2", "card_numbers", rows))

        objs = [
            ["schm_mask_ext_exclude_2", "card_numbers", rows_in_init_env * int(params.test_scale) * 2]   # see init_env.sql
        ]
        self.assertTrue(await self.check_rows_count(args, objs))

        not_found_in_target = await self.check_list_tables_and_fields(self.args["dump"], self.args["restore"])
        self.assertTrue(len(not_found_in_target) == 0)

        if res.result_code == ResultCode.DONE:
            passed_stages.append("test_04_restore")


if __name__ == '__main__':
    unittest.main(exit=False)
