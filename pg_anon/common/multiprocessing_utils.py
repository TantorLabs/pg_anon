import time
from typing import List, Callable

import aioprocessing


async def init_process(name: str, ctx, target_func: Callable, tasks: List, *args, **kwargs):
    from pg_anon.context import Context

    ctx: Context
    start_t = time.time()
    ctx.logger.info(f"================> Process [{name}] started. Input items: {len(tasks)}")
    queue = aioprocessing.AioQueue()

    p = aioprocessing.AioProcess(
        target=target_func,
        args=(name, ctx, queue, tasks, *args),
        kwargs=kwargs,
    )
    p.start()
    res = None
    while True:
        result = await queue.coro_get()
        if result is None:
            break
        res = result
    await p.coro_join()
    end_t = time.time()
    elapsed = round(end_t - start_t, 2)
    result_item_log = str(len(res)) if res is not None else "0"
    ctx.logger.info(
        f"<================ Process [{name}] finished, elapsed: {elapsed} sec. Result {result_item_log} item(s)"
    )
    return res
