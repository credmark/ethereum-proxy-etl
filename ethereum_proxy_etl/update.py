import asyncio
from typing import Callable

from db import async_engine
from detect import (check_ara_proxy, check_comptroller_proxy,
                    check_eip_897_proxy, check_eip_1822_proxy,
                    check_eip_1967_beacon_proxy, check_eip_1967_direct_proxy,
                    check_gnosis_safe_proxy, check_one_to_one_proxy,
                    check_oz_proxy, check_p_proxy_proxy)
from sqlalchemy.sql import text

engine = async_engine()

proxies = [
    {'name': 'eip_1967_direct', 'method': check_eip_1967_direct_proxy, },
    {'name': 'eip_1967_beacon', 'method': check_eip_1967_beacon_proxy, },
    # {'name': 'eip_1167_minimal', 'method': check_eip_1167_minimal_proxy, }, # it is fixed.
    {'name': 'oz', 'method': check_oz_proxy, },
    {'name': 'eip_1822', 'method': check_eip_1822_proxy, },
    {'name': 'eip_897', 'method': check_eip_897_proxy, },
    {'name': 'gnosis_safe', 'method': check_gnosis_safe_proxy, },
    {'name': 'comptroller', 'method': check_comptroller_proxy, },
    {'name': 'ara', 'method': check_ara_proxy, },
    {'name': 'p_proxy', 'method': check_p_proxy_proxy, },
    {'name': 'one_to_one', 'method': check_one_to_one_proxy, },
    # {'name': 'many_to_one', 'method': check_many_to_one_proxy, }, # TODO: handle bytecode
]

BATCH_SIZE = 10000

queue = asyncio.Queue(maxsize=4)
to_update = []


async def handle_batch(rows: list[tuple], method):
    keys = [row[1] for row in rows]
    new_addrs = await method(keys)
    for idx, row in enumerate(rows):
        new_impl = new_addrs[idx]
        old_impl = row[2]
        if new_impl != old_impl:
            to_update.append((row[0], new_impl))


async def worker():
    print('Starting worker')
    while True:
        args = await queue.get()
        try:
            await handle_batch(*args)
        except Exception as err:
            print('Got exception when processing batch')
            print(err)
        finally:
            queue.task_done()


async def check_proxy(proxy_type: str, method: Callable[[str], str]):
    async with engine.connect() as conn:
        conn = await conn.execution_options(yield_per=BATCH_SIZE)
        stmt = text(
            f"SELECT id, proxy_address, implementation_address FROM public.proxy_contracts WHERE proxy_type='{proxy_type}'")
        async with conn.stream(stmt) as result:
            idx = 0
            async for partition in result.partitions(BATCH_SIZE):
                await queue.put((partition, method))
                print(f"added {proxy_type}-{idx} to queue")
                idx += 1
                break


async def update():
    if not to_update:
        print('Nothing to update')
        return

    print(to_update)
    # async with engine.connect() as conn:
    #     for (row_id, implementation_address) in to_update:
    #         stmt = text(
    #             f"UPDATE public.proxy_contracts SET implementation_address = '{implementation_address}' WHERE id={row_id}")
    #         await conn.execute(stmt)


async def check_updates():
    workers = [asyncio.create_task(worker()) for _ in range(1)]

    for proxy in proxies:
        await check_proxy(proxy['name'], proxy['method'])

    await queue.join()

    for task in workers:
        task.cancel()
    await asyncio.gather(*workers, return_exceptions=True)

    await update()


asyncio.run(check_updates())