import asyncio
from collections import defaultdict
from typing import Any, cast

from env import ETH_NODE_URL
from eth_typing import BlockIdentifier
from eth_utils import to_bytes, to_text
from ethereum_dasm.evmdasm import Contract, EvmCode
from web3 import AsyncHTTPProvider, AsyncWeb3, Web3
from web3._utils.encoding import FriendlyJsonSerde
from web3._utils.request import async_make_post_request
from web3.types import RPCResponse


class NodeBatchProvider(AsyncHTTPProvider):
    def encode_rpc_requests(self, method: str, params_list: list[Any]) -> bytes:
        rpc_dict = [{
            "jsonrpc": "2.0",
            "method": method,
            "params": params or [],
            "id": next(self.request_counter),
        } for params in params_list]
        encoded = FriendlyJsonSerde().json_encode(rpc_dict)
        return to_bytes(text=encoded)

    def decode_rpc_responses(self, raw_response: bytes) -> list[RPCResponse]:
        text_response = to_text(raw_response)
        return cast(list[RPCResponse], FriendlyJsonSerde().json_decode(text_response))

    async def batch_requests(self, method: str, params: list[Any]) -> list[RPCResponse]:
        self.logger.debug(
            f"Making request HTTP. URI: {self.endpoint_uri}, Method: {method}"
        )
        request_data = self.encode_rpc_requests(method, params)
        raw_response = await async_make_post_request(
            self.endpoint_uri, request_data, **self.get_request_kwargs()  # pylint: disable=not-a-mapping
        )
        response = self.decode_rpc_responses(raw_response)
        response = sorted(response, key=lambda d: d['id'])
        self.logger.debug(
            f"Getting response HTTP. URI: {self.endpoint_uri}, "
            f"Method: {method}, Response: {response}"
        )
        return response


node_provider = NodeBatchProvider(ETH_NODE_URL)

w3 = AsyncWeb3(provider=AsyncHTTPProvider(ETH_NODE_URL))

# obtained as bytes32(uint256(keccak256('eip1967.proxy.implementation')) - 1)
EIP_1967_LOGIC_SLOT = '0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc'

# obtained as bytes32(uint256(keccak256('eip1967.proxy.beacon')) - 1)
EIP_1967_BEACON_SLOT = '0xa3f0ad74e5423aebfd80d3ef4346578335a9a72aeaee59ff6cb3582b35133d50'

# obtained as keccak256("org.zeppelinos.proxy.implementation")
OPEN_ZEPPELIN_IMPLEMENTATION_SLOT = '0x7050c9e0f4ca769c69bd3a8ef740bc37934f8e2c036e5a723fd8ee048ed3f8c3'

# obtained as keccak256("PROXIABLE")
EIP_1822_LOGIC_SLOT = '0xc5f16f0fcc639fa48a6947836d9850f504798523bf8c9a3a87d5876cf622bcf7'

# obtained as keccak256("IMPLEMENTATION_SLOT")
P_PROXY_LOGIC_SLOT = '0xf603533e14e17222e047634a2b3457fe346d27e294cedf9d21d74e5feea4a046'

# obtained as keccak256("io.ara.proxy.implementation")
ARA_LOGIC_SLOT = '0x55d4ae77629b578ae16bda1fda2417c0e86f2ddce7a638d267c1caee0d7a6c9a'

# obtained as bytes32(uint256(keccak256("IMPLEMENTATION_ADDRESS")) + 1)
ONE_TO_ONE_LOGIC_SLOT = '0x913bd12b32b36f36cedaeb6e043912bceb97022755958701789d3108d33a045a'


EIP_1167_BEACON_METHODS = [
    # bytes4(keccak256("implementation()")) padded to 32 bytes
    '0x5c60da1b00000000000000000000000000000000000000000000000000000000',
    # bytes4(keccak256("childImplementation()")) padded to 32 bytes
    # some implementations use this over the standard method name so that
    # the beacon contract is not detected as an EIP-897 proxy itself
    '0xda52571600000000000000000000000000000000000000000000000000000000',
]

EIP_897_INTERFACE = [
    # bytes4(keccak256("implementation()")) padded to 32 bytes
    '0x5c60da1b00000000000000000000000000000000000000000000000000000000',
]

GNOSIS_SAFE_PROXY_INTERFACE = [
    # bytes4(keccak256("masterCopy()")) padded to 32 bytes
    '0xa619486e00000000000000000000000000000000000000000000000000000000',
]

COMPTROLLER_PROXY_INTERFACE = [
    # bytes4(keccak256("comptrollerImplementation()")) padded to 32 bytes
    '0xbb82aa5e00000000000000000000000000000000000000000000000000000000',
]

MANY_TO_ONE_HANDLER_METHODS = [
    # bytes4(keccak256("fallback()")) padded to 32 bytes
    '0x552079dc00000000000000000000000000000000000000000000000000000000',
]


async def check_eip_1167_minimal_proxy(bytecode: str | list[str]):
    if isinstance(bytecode, list):
        addrs = []
        for b in bytecode:
            try:
                addr = parse_1167_bytecode(b)
                addrs.append(read_address(addr))
            except ValueError:
                addrs.append(None)
        return addrs

    addr = parse_1167_bytecode(bytecode)
    return read_address(addr)


async def check_eip_1967_direct_proxy(proxy_addr: str | list[str]):
    return await get_stored_addr_at(proxy_addr, EIP_1967_LOGIC_SLOT)


async def check_eip_1967_beacon_proxy(proxy_addr: str | list[str]):
    beacon_addr = await get_stored_addr_at(proxy_addr, EIP_1967_BEACON_SLOT)
    if isinstance(proxy_addr, list):
        implementations = []
        method_0 = await call_for_addr(beacon_addr, EIP_1167_BEACON_METHODS[0])
        method_1 = await call_for_addr(beacon_addr, EIP_1167_BEACON_METHODS[1])
        for idx, _ in enumerate(proxy_addr):
            implementations.append(
                method_1[idx] if method_0[idx] is None else method_0[idx])
        return implementations

    try:
        return await call_for_addr(beacon_addr, EIP_1167_BEACON_METHODS[0])
    except:
        return await call_for_addr(beacon_addr, EIP_1167_BEACON_METHODS[1])


async def check_oz_proxy(proxy_addr: str):
    return await get_stored_addr_at(proxy_addr, OPEN_ZEPPELIN_IMPLEMENTATION_SLOT)


async def check_eip_1822_proxy(proxy_addr: str):
    return await get_stored_addr_at(proxy_addr, EIP_1822_LOGIC_SLOT)


async def check_p_proxy_proxy(proxy_addr: str):
    return await get_stored_addr_at(proxy_addr, P_PROXY_LOGIC_SLOT)


async def check_ara_proxy(proxy_addr: str):
    return await get_stored_addr_at(proxy_addr, ARA_LOGIC_SLOT)


async def check_one_to_one_proxy(proxy_addr: str):
    return await get_stored_addr_at(proxy_addr, ONE_TO_ONE_LOGIC_SLOT)


async def check_eip_897_proxy(proxy_addr: str):
    return await call_for_addr(proxy_addr, EIP_897_INTERFACE[0])


async def check_gnosis_safe_proxy(proxy_addr: str):
    return await call_for_addr(proxy_addr, GNOSIS_SAFE_PROXY_INTERFACE[0])


async def check_comptroller_proxy(proxy_addr: str):
    return await call_for_addr(proxy_addr, COMPTROLLER_PROXY_INTERFACE[0])


async def check_many_to_one_proxy(bytecode: str):
    is_single = isinstance(bytecode, str)
    if is_single:
        bytecode = [bytecode]
    addrs = []
    for b in bytecode:
        try:
            addr = parse_many_to_one_bytecode(b)
            addrs.append(read_address(addr))
        except ValueError:
            addrs.append(None)
    res = await call_for_addr(addrs, MANY_TO_ONE_HANDLER_METHODS[0])
    if is_single:
        return res[0]
    return res


def divide_chunks(l: list, n: int):
    for i in range(0, len(l), n):
        yield l[i:i + n]


async def get_stored_addr_at(addr: str | list[str], location: str):
    if isinstance(addr, list):
        responses = await asyncio.gather(*[get_stored_addrs_at(chunk, location) for chunk in divide_chunks(addr, 100)])
        return sum(responses, [])

    res = await w3.eth.get_storage_at(
        Web3.to_checksum_address(addr),
        int(location, 16))
    return read_address(res.hex())


async def get_stored_addrs_at(addrs: list[str], location: str) -> list[str | None]:
    responses = await node_provider.batch_requests(
        'eth_getStorageAt',
        [[Web3.to_checksum_address(addr), location, 'latest']
         for addr in addrs]
    )

    storage_list = []
    for response in responses:
        try:
            storage_list.append(read_address(response['result']))
        except ValueError:
            storage_list.append(None)
    return storage_list


async def call_for_addr(addr: str | list[str], data: Any):
    if isinstance(addr, list):
        responses = await asyncio.gather(*[call_for_addrs(chunk, data) for chunk in divide_chunks(addr, 100)])
        return sum(responses, [])

    res = await w3.eth.call({
        'to': Web3.to_checksum_address(addr),
        'data': data
    })
    return read_address(res.hex())


async def call_for_addrs(addrs: list[str | None], data: list[Any], block: BlockIdentifier = 'latest') -> list[str | None]:
    index_map = defaultdict(list)
    filtered = []

    for idx, addr in enumerate(addrs):
        if addr is None:
            continue
        if addr not in filtered:
            filtered.append(addr)
        index_map[addr].append(idx)

    responses = await node_provider.batch_requests(
        'eth_call',
        [[{
            'to': Web3.to_checksum_address(addr),
            'data': data
        }, 'latest' if not block else block]
            for addr in filtered]
    )

    results = [None] * len(addrs)
    for idx, response in enumerate(responses):
        orig_idx = index_map[idx]
        try:
            if 'result' not in response:
                raise ValueError('Invalid call')
            for orig_idx in index_map[filtered[idx]]:
                results[orig_idx] = read_address(response['result'])
        except ValueError:
            pass
    return results


def read_address(addr) -> str:
    if not isinstance(addr, str) or addr == '0x':
        raise ValueError('Invalid address')

    if len(addr) == 66:
        addr = '0x' + addr[-40:]

    if not Web3.is_address(addr):
        raise ValueError('Invalid web3 address')

    ZERO_ADDRESS = '0x0000000000000000000000000000000000000000'
    if addr == ZERO_ADDRESS:
        raise ValueError('Zero address')

    return addr


EIP_1167_BYTECODE_PREFIX = '0x363d3d373d3d3d363d'
EIP_1167_BYTECODE_SUFFIX = '57fd5bf3'


def parse_1167_bytecode(bytecode: str) -> str:
    if not bytecode.startswith(EIP_1167_BYTECODE_PREFIX):
        raise ValueError('Not an EIP-1167 bytecode')

    # detect length of address (20 bytes non-optimized, 0 < N < 20 bytes for vanity addresses)
    push_n_hex = bytecode[len(EIP_1167_BYTECODE_PREFIX)                          :len(EIP_1167_BYTECODE_PREFIX) + 2]

    # push1 ... push20 use opcodes 0x60 ... 0x73
    address_length = int(push_n_hex, base=16) - 0x5f

    if address_length < 1 or address_length > 20:
        raise ValueError('Not an EIP-1167 bytecode')
    # address length is in bytes, 2 hex chars make up 1 byte
    addressFromBytecode = bytecode[len(
        EIP_1167_BYTECODE_PREFIX) + 2:len(EIP_1167_BYTECODE_PREFIX) + 2 + address_length * 2]

    SUFFIX_OFFSET_FROM_ADDRESS_END = 22
    if not bytecode[len(EIP_1167_BYTECODE_PREFIX) + 2 + address_length * 2 + SUFFIX_OFFSET_FROM_ADDRESS_END:].startswith(EIP_1167_BYTECODE_SUFFIX):
        raise ValueError('Not an EIP-1167 bytecode')

    # padStart is needed for vanity addresses
    return '0x' + addressFromBytecode.zfill(40)


MANY_TO_ONE_PREFIX = '0x60806040523661001357610011610017565b005b6100115b61001f61002f565b61002f61002a610031565b6101'


def parse_many_to_one_bytecode(bytecode: str) -> str:
    if not bytecode.startswith(MANY_TO_ONE_PREFIX):
        raise ValueError('Not a many-to-one bytecode')

    evm_code = EvmCode(contract=Contract(bytecode=bytecode),
                       static_analysis=False, dynamic_analysis=False)
    evm_code.disassemble(bytecode)
    basic_blocks = evm_code.basicblocks
    for block in basic_blocks:
        for inst in block.instructions:
            if inst.name == 'PUSH32':
                addr_prefix = inst.operand[:-40]
                addr = inst.operand[-40:]
                if int(addr_prefix, 16) == 0 and int(addr, 16) != 0:
                    return '0x' + addr

    raise ValueError('Not a many-to-one bytecode')
