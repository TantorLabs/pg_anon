from __future__ import annotations

import queue
import time
from typing import TYPE_CHECKING

import aioprocessing

from pg_anon.common.constants import QUEUE_POLL_TIMEOUT
from pg_anon.common.errors import ErrorCode, PgAnonError

if TYPE_CHECKING:
    import multiprocessing
    from collections.abc import Callable

    from pg_anon.context import Context


async def init_process(
    name: str,
    ctx: Context,
    target_func: Callable,
    tasks: list,
    stop_event: multiprocessing.synchronize.Event,
    *args,  # noqa: ANN002
    **kwargs,  # noqa: ANN003
) -> list | None:
    """Start a subprocess and collect its results via an async queue."""
    start_t = time.time()
    ctx.logger.info("================> Process [%s] started. Input items: %s", name, len(tasks))
    aio_queue = aioprocessing.AioQueue()

    p = aioprocessing.AioProcess(
        target=target_func,
        args=(name, aio_queue, tasks, stop_event, *args),
        kwargs=kwargs,
    )
    p.start()

    res = None
    try:
        while True:
            try:
                result = await aio_queue.coro_get(timeout=QUEUE_POLL_TIMEOUT)
            except queue.Empty:
                if not p.is_alive():
                    raise PgAnonError(
                        ErrorCode.OPERATION_FAILED,
                        f"Process [{name}] terminated unexpectedly (exit code: {p.exitcode})",
                    ) from None
                continue

            if result is None:
                break

            # Check if subprocess sent an exception
            if isinstance(result, list) and result and isinstance(result[0], Exception):
                raise result[0]

            res = result
    finally:
        await p.coro_join()

    end_t = time.time()
    elapsed = round(end_t - start_t, 2)
    result_item_log = str(len(res)) if res is not None else "0"
    ctx.logger.info(
        "<================ Process [%s] finished, elapsed: %s sec. Result %s item(s)",
        name,
        elapsed,
        result_item_log,
    )
    return res
