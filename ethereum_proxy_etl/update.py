import asyncio
from typing import Callable

from sqlalchemy import update

from db import async_engine, ProxyContracts
from detect import (check_ara_proxy, check_comptroller_proxy,
                    check_eip_897_proxy, check_eip_1822_proxy,
                    check_eip_1967_beacon_proxy, check_eip_1967_direct_proxy,
                    check_gnosis_safe_proxy, check_one_to_one_proxy,
                    check_oz_proxy, check_p_proxy_proxy)
from sqlalchemy.sql import text
from sqlalchemy.ext.asyncio import async_sessionmaker


engine = async_engine()
async_session = async_sessionmaker(engine)

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


async def handle_batch(rows: list[tuple], method):
    to_update = []
    keys = [row[1] for row in rows]
    new_addrs = await method(keys)
    for idx, row in enumerate(rows):
        new_impl = new_addrs[idx]
        old_impl = row[2]
        if new_impl is not None and new_impl != old_impl:
            to_update.append(
                {"id": row[0], "implementation_address": new_impl})

    if len(to_update) == 0:
        return

    async with async_session.begin() as session:
        await session.execute(update(ProxyContracts), to_update)


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


async def update_existing_async():
    workers = [asyncio.create_task(worker()) for _ in range(1)]

    for proxy in proxies:
        await check_proxy(proxy['name'], proxy['method'])

    await queue.join()

    for task in workers:
        task.cancel()
    await asyncio.gather(*workers, return_exceptions=True)


def update_existing():
    asyncio.run(update_existing_async())
