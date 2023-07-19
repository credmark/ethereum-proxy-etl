import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Callable, Literal, TypedDict

import pandas as pd
from db import snowflake_connection
from detect import (check_ara_proxy, check_comptroller_proxy,
                    check_eip_897_proxy, check_eip_1167_minimal_proxy,
                    check_eip_1822_proxy, check_eip_1967_beacon_proxy,
                    check_eip_1967_direct_proxy, check_gnosis_safe_proxy,
                    check_many_to_one_proxy, check_one_to_one_proxy,
                    check_oz_proxy, check_p_proxy_proxy)
from pandas import DataFrame

conn = snowflake_connection()

# sem = asyncio.Semaphore(10)
queue = asyncio.Queue(maxsize=4)


updated_at = datetime.now(timezone.utc).isoformat()


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


async def handle_batch(id: str, proxy_type: str, check_proxy: Callable[[str], str], df: DataFrame):
    print(f"processing {id} - {len(df)}")

    async def check_proxy_no_err(key: str):
        try:
            return await check_proxy(key)
        except ValueError as err:
            return None

    keys = list(df['KEY'])
    # print(len(keys))
    try:
        proxy_addr = await check_proxy_no_err(keys)
        # print(proxy_addr)
    except Exception as err:
        print('got err')
        print(err)
        raise err

    new_df = df[['ADDRESS']].copy().rename(columns={
        'ADDRESS': 'proxy_address'
    })

    new_df['proxy_type'] = proxy_type

    proxy_addr = pd.Series(proxy_addr)
    new_df['implementation_address'] = proxy_addr.values
    new_df['updated_at'] = updated_at
    new_df = new_df.dropna()
    new_df.to_csv(f'batch-{id}.csv', index=False)
    print(f'batch-{id}.csv is written. Processed {len(df)}.')


async def worker():
    print('Starting worker')
    while True:
        args = await queue.get()
        try:
            await handle_batch(*args)
        except Exception as err:
            print('Got exception when processing batch')
            print(err)
            id = args[0]
            df = args[-1]
            df.to_csv(f'error-{id}.csv')
        finally:
            queue.task_done()


async def execute_marker(executor: ThreadPoolExecutor, marker: Marker):
    loop = asyncio.get_running_loop()
    cur = conn.cursor()

    if marker['type'] == 'bytecode':
        cur.execute(
            f"SELECT {marker['select']} as key, address FROM ETHEREUM_V2.CORE_RAW.CONTRACTS WHERE CONTAINS(BYTECODE, '{marker['marker']}') AND BLOCK_NUMBER <= 17690000")
    elif marker['type'] == 'function':
        cur.execute(
            f"SELECT {marker['select']} as key, address FROM ETHEREUM_V2.CORE_RAW.CONTRACTS WHERE ARRAY_CONTAINS('{marker['marker']}'::variant, SPLIT(TRIM(FUNCTION_SIGHASHES, '{{}}'), ',')) AND BLOCK_NUMBER <= 17690000")
    batches = await loop.run_in_executor(executor, cur.fetch_pandas_batches)
    for idx, batch in enumerate(batches):
        await queue.put((f"{marker['name']}-{idx}", marker['name'], marker['method'], batch))
        print(f"added {marker['name']}-{idx} to queue")


async def main(executor: ThreadPoolExecutor):
    workers = [asyncio.create_task(worker()) for _ in range(1)]

    async with asyncio.TaskGroup() as tg:
        for marker in markers:
            tg.create_task(execute_marker(executor, marker))

    await queue.join()

    for task in workers:
        task.cancel()
    await asyncio.gather(*workers, return_exceptions=True)


def backfill():
    with ThreadPoolExecutor() as exec:
        asyncio.run(main(exec))
