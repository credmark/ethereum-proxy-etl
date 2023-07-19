from ethereum_proxy_etl.detect import parse_1167_bytecode

def test_parse_1167_bytecode():
    addr = parse_1167_bytecode('0x363d3d373d3d3d363d73f62849f9a0b5bf2913b396098f7c7019b51a820a5af43d82803e903d91602b57fd5bf3000000000000000000000000000000000000000000000000000000000000007a6900000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000')
    assert addr == '0xf62849f9a0b5bf2913b396098f7c7019b51a820a'