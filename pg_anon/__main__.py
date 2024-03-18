import asyncio

from pg_anon import MainRoutine


def _run_pg_anon():
    asyncio.run(MainRoutine().run())


if __name__ == "__main__":
    _run_pg_anon()
