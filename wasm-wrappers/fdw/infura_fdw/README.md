---
source: https://docs.infura.io/
documentation: https://docs.infura.io/api/networks/ethereum
author: supabase
tags:
  - wasm
  - official
---

# Infura

Infura provides blockchain infrastructure that allows applications to access Ethereum network data through JSON-RPC APIs.

## Available Versions

| Version | Release Date |
| ------- | ----------- |
| 0.1.0   | TBD         |

## Preparation

1. Create an Infura account and project
2. Get your Project ID from the project settings
3. Create a foreign data wrapper and server

```sql
create extension if not exists wrappers with schema extensions;

create foreign data wrapper wasm_wrapper
  handler wasm_handler
  validator wasm_validator;

-- Using direct project ID
create server infura_server
  foreign data wrapper wasm_wrapper
  options (
    project_id 'your-project-id'
  );

-- Using vault for project ID
insert into vault.secrets (name, secret)
values ('infura', 'your-project-id')
returning key_id;

create server infura_server
  foreign data wrapper wasm_wrapper
  options (
    project_id_id 'key-id-from-above'
  );
```

## Entities

### Block Number

```sql
create foreign table infura.block_number (
    number bigint
)
    server infura_server
    options (
        table 'eth_blockNumber'
    );
```

### Blocks

```sql
create foreign table infura.blocks (
    number bigint,
    hash text,
    parent_hash text,
    nonce text,
    miner text,
    difficulty bigint,
    total_difficulty bigint,
    size bigint,
    gas_limit bigint,
    gas_used bigint,
    timestamp bigint
)
    server infura_server
    options (
        table 'eth_getBlockByNumber'
    );
```

## Supported Data Types

| Postgres Type | Infura Type | Description |
| ------------ | ----------- | ----------- |
| bigint       | hex number  | Used for block numbers, gas values, and timestamps |
| text         | string/hex  | Used for hashes, addresses, and other hex strings |

## Limitations

- Only supports HTTP endpoints (no WebSocket support)
- Rate limits apply based on your Infura plan
- Some complex queries may timeout due to blockchain data size
- Currently only supports eth_blockNumber and eth_getBlockByNumber methods
- All numeric values are returned as bigint, which may not capture the full range of some Ethereum values

## Examples

```sql
-- Get latest block number
SELECT number FROM infura.block_number;

-- Get block details
SELECT number, hash, miner, timestamp
FROM infura.blocks
WHERE number = (SELECT number FROM infura.block_number);

-- Get gas usage over time
SELECT
    number,
    gas_used,
    gas_limit,
    (gas_used::float / gas_limit::float * 100)::numeric(5,2) as gas_usage_percent
FROM infura.blocks
ORDER BY number DESC
LIMIT 10;
```
