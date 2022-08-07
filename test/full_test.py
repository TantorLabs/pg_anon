import unittest
from pg_anon import *
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))


input_args = None
passed_stages = []


class DBOperations:
    @staticmethod
    async def init_db(db_conn, db_name):
        await db_conn.execute("""
            SELECT pg_terminate_backend(pid)
            FROM pg_stat_activity
            WHERE pid <> pg_backend_pid()
                AND datname = '%s'
        """ % db_name)

        print("""DROP DATABASE IF EXISTS %s and CREATE DATABASE""" % db_name)
        await db_conn.execute("""DROP DATABASE IF EXISTS %s""" % db_name)
        await db_conn.execute("""
            CREATE DATABASE %s
                WITH
                OWNER = test_user
                ENCODING = 'UTF8'
                LC_COLLATE = 'en_US.UTF-8'
                LC_CTYPE = 'en_US.UTF-8'
                TABLESPACE = pg_default
                template = template0""" % db_name)

    @staticmethod
    async def init_test_env(db_conn, scale=1):
        current_dir = os.path.dirname(os.path.realpath(__file__))
        with open(os.path.join(current_dir, 'init_env.sql'), 'r') as f:
            data = f.read()
        if scale != 1:
            data = data.replace('15001', str(15001 * scale))
        await db_conn.execute(data)


class PGAnonUnitTest(unittest.IsolatedAsyncioTestCase):
    async def test_01_init(self):
        parser = Context.get_arg_parser()
        args = parser.parse_args([
            '--db-host=127.0.0.1',
            '--db-name=postgres',
            '--db-user=postgres',
            '--db-port=5432',
            '--db-user-password=yImTVbL3TLxF',
            '--mode=init'
        ])

        ctx = Context(args)

        db_conn = await asyncpg.connect(**ctx.conn_params)
        await DBOperations.init_db(db_conn, 'sourse_db')
        await DBOperations.init_db(db_conn, 'target_db')
        await db_conn.close()

        sourse_db_params = ctx.conn_params.copy()
        sourse_db_params['database'] = 'sourse_db'
        db_conn = await asyncpg.connect(**sourse_db_params)

        await DBOperations.init_test_env(db_conn, 10)
        await db_conn.close()

        args = parser.parse_args([
            '--db-host=127.0.0.1',
            '--db-name=sourse_db',
            '--db-user=postgres',
            '--db-port=5432',
            '--db-user-password=yImTVbL3TLxF',
            '--mode=init'
        ])

        res = await MainRoutine(args).run()
        if res.result_code == "done":
            passed_stages.append("test_01_init")
        self.assertTrue(res.result_code == "done")

    async def test_02_dump(self):
        if "test_01_init" not in passed_stages:
            self.assertTrue(False)

        parser = Context.get_arg_parser()
        args = parser.parse_args([
            '--db-host=127.0.0.1',
            '--db-name=sourse_db',
            '--db-user=postgres',
            '--db-port=5432',
            '--db-user-password=yImTVbL3TLxF',
            '--mode=dump',
            '--dict-file=test.json',
            '--threads=1',
            '--clear-output-dir'
        ])

        ctx = Context(args)

        sourse_db_params = ctx.conn_params.copy()
        db_conn = await asyncpg.connect(**sourse_db_params)
        await DBOperations.init_test_env(db_conn, 10)
        await db_conn.close()

        res = await MainRoutine(args).run()
        if res.result_code == "done":
            passed_stages.append("test_02_dump")
        self.assertTrue(res.result_code == "done")

    async def test_03_restore(self):
        if "test_02_dump" not in passed_stages:
            self.assertTrue(False)

        parser = Context.get_arg_parser()
        args = parser.parse_args([
            '--db-host=127.0.0.1',
            '--db-name=target_db',
            '--db-user=postgres',
            '--db-port=5432',
            '--db-user-password=yImTVbL3TLxF',
            '--mode=restore',
            '--input-dir=test'
        ])

        ctx = Context(args)

        target_db_params = ctx.conn_params.copy()
        db_conn = await asyncpg.connect(**target_db_params)
        await db_conn.close()

        res = await MainRoutine(args).run()
        self.assertTrue(res.result_code == "done")
        if res.result_code == "done":
            passed_stages.append("test_03_restore")

    async def test_03_validate(self):
        self.assertTrue(False)


if __name__ == '__main__':
    unittest.main(defaultTest="PGAnonUnitTest", exit=False)
