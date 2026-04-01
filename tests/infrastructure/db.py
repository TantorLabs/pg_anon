from asyncpg import Record

from pg_anon.common.db_utils import create_connection
from pg_anon.common.dto import ConnectionParams
from tests.infrastructure.params import TestParams


class DBManager:
    def __init__(self, params: TestParams) -> None:
        self.params = params

    def _connection_params(self, db_name: str) -> ConnectionParams:
        return ConnectionParams(
            host=self.params.test_db_host,
            port=int(self.params.test_db_port),
            database=db_name,
            user=self.params.test_db_user,
            password=self.params.test_db_user_password,
        )

    async def create_db(self, db_name: str) -> None:
        """Drop (if exists) and create a fresh database."""
        await self.execute("postgres", f"""
            SELECT pg_terminate_backend(pid)
            FROM pg_stat_activity
            WHERE pid <> pg_backend_pid()
                AND datname = '{db_name}'
        """)
        await self.execute("postgres", f"DROP DATABASE IF EXISTS {db_name}")
        await self.execute("postgres", f"""
            CREATE DATABASE {db_name}
                WITH
                OWNER = {self.params.test_db_user}
                ENCODING = 'UTF8'
                LC_COLLATE = 'en_US.UTF-8'
                LC_CTYPE = 'en_US.UTF-8'
                template = template0
        """)

    async def drop_db(self, db_name: str) -> None:
        """Drop database if exists. Skipped when KEEP_TEST_DBS is set."""
        if self.params.keep_test_dbs:
            return
        await self.execute("postgres", f"""
            SELECT pg_terminate_backend(pid)
            FROM pg_stat_activity
            WHERE pid <> pg_backend_pid()
                AND datname = '{db_name}'
        """)
        await self.execute("postgres", f"DROP DATABASE IF EXISTS {db_name}")

    async def execute(self, db_name: str, query: str) -> None:
        """Execute SQL (DDL/DML) without returning results."""
        db_conn = await create_connection(self._connection_params(db_name))
        try:
            await db_conn.execute(query)
        finally:
            await db_conn.close()

    async def fetch(self, db_name: str, query: str) -> list[Record]:
        """Execute query and return results."""
        db_conn = await create_connection(self._connection_params(db_name))
        try:
            return await db_conn.fetch(query)
        finally:
            await db_conn.close()
