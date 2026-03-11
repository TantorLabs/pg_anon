import queue
import multiprocessing
import time
from typing import Callable, List, Optional

import aioprocessing

from pg_anon.common.constants import QUEUE_POLL_TIMEOUT
from pg_anon.common.errors import PgAnonError, ErrorCode


async def init_process(
    name: str,
    ctx,
    target_func: Callable,
    tasks: List,
    stop_event: Optional[multiprocessing.Event] = None,
    *args,
    **kwargs
):
    from pg_anon.context import Context
    ctx: Context

    start_t = time.time()
    ctx.logger.info(f"================> Process [{name}] started. Input items: {len(tasks)}")
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
                        f"Process [{name}] terminated unexpectedly (exit code: {p.exitcode})"
                    )
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
        f"<================ Process [{name}] finished, elapsed: {elapsed} sec. Result {result_item_log} item(s)"
    )
    return res
