import unittest
import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from pg_anon import *


input_args = None
passed_stages = []


class TestParams:
    test_db_user = 'anon_test_user'         # default value
    test_db_user_password = 'mYy5RexGsZ'
    test_db_host = '127.0.0.1'
    test_db_port = '5432'
    test_source_db = 'test_source_db'
    test_target_db = 'test_target_db'
    test_target_db_2 = test_target_db + '_2'
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
            self.test_target_db_2 = self.test_target_db + "_2"
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
        with open(os.path.join(current_dir, 'init_env.sql'), 'r') as f:
            data = f.read()
        if scale != 1:
            data = data.replace('1512', str(1512 * scale))
        await db_conn.execute(data)


class PGAnonUnitTest(unittest.IsolatedAsyncioTestCase):
    async def test_01_init(self):
        parser = Context.get_arg_parser()
        args = parser.parse_args([
            '--db-host=%s' % params.test_db_host,
            '--db-name=postgres',
            '--db-user=%s' % params.test_db_user,
            '--db-port=%s' % params.test_db_port,
            '--db-user-password=%s' % params.test_db_user_password,
            '--mode=init'
        ])

        ctx = Context(args)

        db_conn = await asyncpg.connect(**ctx.conn_params)
        await DBOperations.init_db(db_conn, params.test_source_db)
        await DBOperations.init_db(db_conn, params.test_target_db)
        await DBOperations.init_db(db_conn, params.test_target_db_2)
        await db_conn.close()

        sourse_db_params = ctx.conn_params.copy()
        sourse_db_params['database'] = params.test_source_db
        db_conn = await asyncpg.connect(**sourse_db_params)

        await DBOperations.init_test_env(db_conn, 10)
        await db_conn.close()

        args = parser.parse_args([
            '--db-host=%s' % params.test_db_host,
            '--db-name=%s' % params.test_source_db,
            '--db-user=%s' % params.test_db_user,
            '--db-port=%s' % params.test_db_port,
            '--db-user-password=%s' % params.test_db_user_password,
            '--mode=init'
        ])

        res = await MainRoutine(args).run()
        if res.result_code == ResultCode.DONE:
            passed_stages.append("test_01_init")
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
            '--verbose=debug'
        ])

        ctx = Context(args)

        sourse_db_params = ctx.conn_params.copy()
        db_conn = await asyncpg.connect(**sourse_db_params)
        await DBOperations.init_test_env(db_conn, params.test_scale)
        await db_conn.close()

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
            '--verbose=debug'
        ])

        ctx = Context(args)

        target_db_params = ctx.conn_params.copy()
        db_conn = await asyncpg.connect(**target_db_params)
        await db_conn.close()

        res = await MainRoutine(args).run()
        self.assertTrue(res.result_code == ResultCode.DONE)
        if res.result_code == ResultCode.DONE:
            passed_stages.append("test_03_restore")

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
            '--verbose=debug'
        ])

        ctx = Context(args)

        sourse_db_params = ctx.conn_params.copy()
        db_conn = await asyncpg.connect(**sourse_db_params)
        await DBOperations.init_test_env(db_conn, 10)
        await db_conn.close()

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
            '--db-name=%s' % params.test_target_db_2,
            '--db-user=%s' % params.test_db_user,
            '--db-port=%s' % params.test_db_port,
            '--db-user-password=%s' % params.test_db_user_password,
            '--threads=%s' % params.test_threads,
            '--mode=restore',
            '--input-dir=test_exclude',
            '--drop-custom-check-constr',
            '--verbose=debug'
        ])

        ctx = Context(args)

        target_db_params = ctx.conn_params.copy()
        db_conn = await asyncpg.connect(**target_db_params)
        await db_conn.close()

        res = await MainRoutine(args).run()
        self.assertTrue(res.result_code == ResultCode.DONE)
        if res.result_code == ResultCode.DONE:
            passed_stages.append("test_05restore")


if __name__ == '__main__':
    unittest.main(defaultTest="PGAnonUnitTest", exit=False)
