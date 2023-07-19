import pytest
from ethereum_proxy_etl.detect import (check_ara_proxy, check_eip_1967_direct_proxy,
                                       check_eip_1967_beacon_proxy,
                                       check_eip_1167_minimal_proxy, check_many_to_one_proxy, check_one_to_one_proxy,
                                       check_oz_proxy,
                                       check_eip_1822_proxy,
                                       check_eip_897_proxy,
                                       check_gnosis_safe_proxy,
                                       check_comptroller_proxy, check_p_proxy_proxy)


@pytest.mark.asyncio
async def test_minimal():
    addr = await check_eip_1167_minimal_proxy(
        '0x363d3d373d3d3d363d73f62849f9a0b5bf2913b396098f7c7019b51a820a5af43d82803e903d91602b57fd5bf3000000000000000000000000000000000000000000000000000000000000007a6900000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000')
    assert '0xf62849f9a0b5bf2913b396098f7c7019b51a820a' == addr


@pytest.mark.asyncio
async def test_eip_1967_direct_proxy():
    addr = await check_eip_1967_direct_proxy('0xA7AeFeaD2F25972D80516628417ac46b3F2604Af')
    assert '0x4bd844f72a8edd323056130a86fc624d0dbcf5b0' == addr


@pytest.mark.asyncio
async def test_eip_1967_beacon_proxy():
    addr = await check_eip_1967_beacon_proxy('0xDd4e2eb37268B047f55fC5cAf22837F9EC08A881')
    assert '0xe5c048792dcf2e4a56000c8b6a47f21df22752d1' == addr


@pytest.mark.asyncio
async def test_eip_1967_beacon_variant_proxy():
    addr = await check_eip_1967_beacon_proxy('0x114f1388fAB456c4bA31B1850b244Eedcd024136')
    assert '0x36b799160cdc2d9809d108224d1967cc9b7d321c' == addr


@pytest.mark.asyncio
async def test_oz_proxy():
    addr = await check_oz_proxy('0x00fdae9174357424a78afaad98da36fd66dd9e03')
    assert '0xeb6cb99538bcf417f7a64a4ad81fce9b9714cde8' == addr


@pytest.mark.asyncio
async def test_eip_1822_proxy():
    addr = await check_eip_1822_proxy('0x39fbbabf11738317a448031930706cd3e612e1b9')
    assert '0x43c3983778ec88f0c383134e812c0c58a819add0' == addr


@pytest.mark.asyncio
async def test_eip_897_proxy():
    addr = await check_eip_897_proxy('0x8260b9eC6d472a34AD081297794d7Cc00181360a')
    assert '0xe4e4003afe3765aca8149a82fc064c0b125b9e5a' == addr


@pytest.mark.asyncio
async def test_gnosis_safe_proxy():
    addr = await check_gnosis_safe_proxy('0x0DA0C3e52C977Ed3cBc641fF02DD271c3ED55aFe')
    assert '0xd9db270c1b5e3bd161e8c8503c55ceabee709552' == addr


@pytest.mark.asyncio
async def test_comptroller_proxy():
    addr = await check_comptroller_proxy('0x3d9819210A31b4961b30EF54bE2aeD79B9c9Cd3B')
    assert '0xbafe01ff935c7305907c33bf824352ee5979b526' == addr


@pytest.mark.asyncio
async def test_ara_proxy():
    addr = await check_ara_proxy('0xa92e7c82b11d10716ab534051b271d2f6aef7df5')
    assert '0xb8ca408aff631b65021850cd7ebf8eac7f3c0312' == addr


@pytest.mark.asyncio
async def test_p_proxy_proxy():
    addr = await check_p_proxy_proxy('0xa9536b9c75a9e0fae3b56a96ac8edf76abc91978')
    assert '0x20fa1b7557b00518ece235c01e1e380bf2fc33cd' == addr


@pytest.mark.asyncio
async def test_one_to_one_proxy():
    addr = await check_one_to_one_proxy('0xF00A38376C8668fC1f3Cd3dAeef42E0E44A7Fcdb')
    assert '0x78b4f45b4a2afa333c7be1dbc7f2c9f056615327' == addr


@pytest.mark.asyncio
async def test_many_to_one_proxy():
    addr = await check_many_to_one_proxy('0x60806040523661001357610011610017565b005b6100115b61001f61002f565b61002f61002a610031565b6101b0565b565b60405160009081906060906001600160a01b037f000000000000000000000000ffde4785e980a99fe10e6a87a67d243664b91b25169083818181855afa9150503d806000811461009d576040519150601f19603f3d011682016040523d82523d6000602084013e6100a2565b606091505b50915091508181906101325760405162461bcd60e51b81526004018080602001828103825283818151815260200191508051906020019080838360005b838110156100f75781810151838201526020016100df565b50505050905090810190601f1680156101245780820380516001836020036101000a031916815260200191505b509250505060405180910390fd5b50600081806020019051602081101561014a57600080fd5b505190506001600160a01b0381166101a9576040805162461bcd60e51b815260206004820152601760248201527f4552525f4e554c4c5f494d504c454d454e544154494f4e000000000000000000604482015290519081900360640190fd5b9250505090565b3660008037600080366000845af43d6000803e8080156101cf573d6000f35b3d6000fdfea26469706673582212209b0f8ebe5564b0d1fb938189635d5a7b33088937e2d48e4ff88b4fcf7c850bb164736f6c634300060c0033')
    assert '0x48923befe0b63b6611111fbc15e45a5ef8a4224f' == addr
