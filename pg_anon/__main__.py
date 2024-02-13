import asyncio

from pg_anon import MainRoutine


def _run_pg_anon():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(MainRoutine().run())


if __name__ == "__main__":
    _run_pg_anon()
