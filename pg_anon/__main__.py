import asyncio

from pg_anon.pg_anon import run_pg_anon

if __name__ == "__main__":
    asyncio.run(run_pg_anon())
