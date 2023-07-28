# Ethereum Proxy ETL

## Streaming

Set the following env vars:

- POSTGRES_USERNAME
- POSTGRES_PASSWORD
- POSTGRES_HOST
- POSTGRES_PORT
- POSTGRES_DATABASE

- ETH_NODE_URL

Stream for `from_block` -> `to_block`:

```py
from ethereum_proxy_etl.stream import stream

stream(start_block, end_block)
```

## Update

Updates implementation address of existing proxy contracts.

Set the following env vars:

- POSTGRES_USERNAME
- POSTGRES_PASSWORD
- POSTGRES_HOST
- POSTGRES_PORT
- POSTGRES_DATABASE

- ETH_NODE_URL

```py
from ethereum_proxy_etl.update import update_existing

update_existing()
```
