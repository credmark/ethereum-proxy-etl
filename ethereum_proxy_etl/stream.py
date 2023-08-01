import asyncio
from datetime import datetime, timezone
from typing import Callable, Literal, TypedDict

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.sql import text

from .db import ProxyContracts, async_engine
from .detect import (check_ara_proxy, check_comptroller_proxy,
                     check_eip_897_proxy, check_eip_1167_minimal_proxy,
                     check_eip_1822_proxy, check_eip_1967_beacon_proxy,
                     check_eip_1967_direct_proxy, check_gnosis_safe_proxy,
                     check_many_to_one_proxy, check_one_to_one_proxy,
                     check_oz_proxy, check_p_proxy_proxy)

# No of types of proxy markers to process at a time
MARKER_CONCURRENCY = 5

# No of rows to fetch in a batch from cursor and process at a time
BATCH_SIZE = 10000

# Max no of batches available for workers
QUEUE_SIZE = 2

# Max no of workers to process a batch
WORKERS = 1

engine = async_engine()

async_session = async_sessionmaker(engine)

updated_at = datetime.now(timezone.utc).replace(tzinfo=None)

queue = asyncio.Queue(maxsize=QUEUE_SIZE)

Marker = TypedDict(
    'Marker',
    {
        'type': Literal['bytecode'] | Literal['function'],
        'name': str,
        'select': str,
        'method': Callable[[str], str],
        'marker': str
    }
)

markers: list[Marker] = [
    {'type': 'bytecode', 'name': 'eip_1967_direct', 'select': 'address', 'method': check_eip_1967_direct_proxy,
        'marker': '360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc'},
    {'type': 'bytecode', 'name': 'eip_1967_beacon', 'select': 'address', 'method': check_eip_1967_beacon_proxy,
        'marker': 'a3f0ad74e5423aebfd80d3ef4346578335a9a72aeaee59ff6cb3582b35133d50'},
    {'type': 'bytecode', 'name': 'eip_1167_minimal', 'select': 'bytecode', 'method': check_eip_1167_minimal_proxy,
        'marker': '0x363d3d373d3d3d363d'},
    {'type': 'bytecode', 'name': 'oz', 'select': 'address', 'method': check_oz_proxy,
        'marker': '7050c9e0f4ca769c69bd3a8ef740bc37934f8e2c036e5a723fd8ee048ed3f8c3'},
    {'type': 'bytecode', 'name': 'eip_1822', 'select': 'address', 'method': check_eip_1822_proxy,
        'marker': 'c5f16f0fcc639fa48a6947836d9850f504798523bf8c9a3a87d5876cf622bcf7', },
    {'type': 'function', 'name': 'eip_897',
        'select': 'address', 'method': check_eip_897_proxy, 'marker': '0x5c60da1b'},
    {'type': 'function', 'name': 'gnosis_safe',
        'select': 'address', 'method': check_gnosis_safe_proxy, 'marker': '0xa619486e'},
    {'type': 'function', 'name': 'comptroller',
        'select': 'address', 'method': check_comptroller_proxy, 'marker': '0xbb82aa5e'},
    {'type': 'bytecode', 'name': 'ara', 'select': 'address', 'method': check_ara_proxy,
        'marker': '696f2e6172612e70726f78792e696d706c656d656e746174696f6e', },
    {'type': 'bytecode', 'name': 'p_proxy', 'select': 'address', 'method': check_p_proxy_proxy,
        'marker': '494d504c454d454e544154494f4e5f534c4f54', },
    {'type': 'bytecode', 'name': 'one_to_one', 'select': 'address', 'method': check_one_to_one_proxy,
        'marker': '913bd12b32b36f36cedaeb6e043912bceb97022755958701789d3108d33a045a', },
    {'type': 'bytecode', 'name': 'many_to_one', 'select': 'bytecode', 'method': check_many_to_one_proxy,
        'marker': '0x60806040523661001357610011610017565b005b6100115b61001f61002f565b61002f61002a610031565b6101', },
]


async def handle_batch(_batch_id: str, proxy_type: str, check_proxy: Callable[[str], str], partition: list[tuple]):
    # print(f"processing {batch_id} - {len(partition)}")

    async def check_proxy_no_err(key: str):
        try:
            return await check_proxy(key)
        except ValueError:
            return None

    keys = [row.key for row in partition]

    implementation_addr = await check_proxy_no_err(keys)

    batch = []
    for idx, row in enumerate(partition):
        if not implementation_addr[idx]:
            continue

        batch.append({
            "proxy_address": row.address,
            "proxy_type": proxy_type,
            "implementation_address": implementation_addr[idx],
            "updated_at": updated_at
        })

    if len(batch) > 0:
        async with async_session.begin() as session:
            insert_stmt = insert(ProxyContracts)
            upsert_stmt = insert_stmt.on_conflict_do_update(
                index_elements=[ProxyContracts.proxy_address],
                set_=dict(proxy_type=insert_stmt.excluded.proxy_type,
                          implementation_address=insert_stmt.excluded.implementation_address,
                          updated_at=insert_stmt.excluded.updated_at),
                where=text(
                    "(proxy_contracts.proxy_type = 'eip_897' AND excluded.proxy_type = 'eip_1967_beacon') OR (proxy_contracts.proxy_type = 'eip_1967_direct' AND excluded.proxy_type = 'eip_897')")
            )

            await session.execute(upsert_stmt, batch)

    # print(f'batch-{batch_id} is processed. Inserted {len(batch)}.')


async def worker(idx: int):
    print(f'Starting worker #{idx}')
    while True:
        args = await queue.get()
        try:
            await handle_batch(*args)
        except Exception as err:
            batch_id = args[0]
            batch = args[-1]
            print(
                f'Got exception when processing batch. batch_id={batch_id}, batch=')
            print(batch)
            print(err)
        finally:
            queue.task_done()


async def execute_marker(marker: Marker, start_block: int, end_block: int):
    # print(f"start {marker['name']}")
    async with engine.connect() as conn:
        conn = await conn.execution_options(yield_per=BATCH_SIZE)
        if marker['type'] == 'bytecode':
            stmt = text(
                f"SELECT {marker['select']} as key, address "
                f"FROM public.contracts "
                f"WHERE POSITION('{marker['marker']}' in bytecode) > 0 "
                f"AND block_number >= {start_block} AND block_number <= {end_block}"
            )
        elif marker['type'] == 'function':
            stmt = text(
                f"SELECT {marker['select']} as key, address "
                f"FROM public.contracts "
                f"WHERE '{marker['marker']}' = ANY(function_sighashes)"
                f"AND block_number >= {start_block} AND block_number <= {end_block}"
            )

        async with conn.stream(stmt) as result:
            idx = 0
            async for partition in result.partitions(BATCH_SIZE):
                await queue.put((f"{marker['name']}-{idx}",
                                 marker['name'],
                                 marker['method'],
                                 partition))
                # print(f"added {marker['name']}-{idx} to queue")
                idx += 1
        # print(f"end {marker['name']}")


async def stream_async(start_block: int, end_block: int):
    workers = [asyncio.create_task(worker(i)) for i in range(WORKERS)]

    marker_tasks = set()
    for marker in markers:
        if len(marker_tasks) >= MARKER_CONCURRENCY:
            # Wait for some download to finish before adding a new one
            _done, marker_tasks = await asyncio.wait(marker_tasks, return_when=asyncio.FIRST_COMPLETED)
        marker_tasks.add(asyncio.create_task(
            execute_marker(marker, start_block, end_block)))
    # Wait for the remaining downloads to finish
    await asyncio.wait(marker_tasks)

    await queue.join()

    for task in workers:
        task.cancel()
    await asyncio.gather(*workers, return_exceptions=True)


def stream(start_block: int, end_block: int):
    asyncio.run(stream_async(start_block, end_block))
