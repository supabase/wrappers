---
source:
documentation:
author: JohnCari(https://github.com/JohnCari)
tags:
  - wasm
  - community
---

# Infura

[Infura](https://www.infura.io/) provides reliable, scalable blockchain infrastructure for Ethereum, Polygon, and other EVM-compatible networks via JSON-RPC APIs.

The Infura Wrapper is a WebAssembly (Wasm) foreign data wrapper which allows you to read blockchain data (blocks, transactions, balances, logs) directly from your Postgres database.

## Available Versions

| Version | Wasm Package URL                                                                                  | Checksum | Required Wrappers Version |
| ------- | ------------------------------------------------------------------------------------------------- | -------- | ------------------------- |
| 0.1.0   | `https://github.com/supabase/wrappers/releases/download/wasm_infura_fdw_v0.1.0/infura_fdw.wasm`   | `6cb829b851ea8cbd70cb893958826824388a4d9477305a16f2f489bcd569b35e` | >=0.5.0                   |

## Preparation

Before you can query Infura, you need to enable the Wrappers extension and store your credentials in Postgres.

### Enable Wrappers

Make sure the `wrappers` extension is installed on your database:

```sql
create extension if not exists wrappers with schema extensions;
```

### Enable the Infura Wrapper

Enable the Wasm foreign data wrapper:

```sql
create foreign data wrapper wasm_wrapper
  handler wasm_fdw_handler
  validator wasm_fdw_validator;
```

### Store your credentials (optional)

By default, Postgres stores FDW credentials inside `pg_catalog.pg_foreign_server` in plain text. Anyone with access to this table will be able to view these credentials. Wrappers is designed to work with [Vault](https://supabase.com/docs/guides/database/vault), which provides an additional level of security for storing credentials. We recommend using Vault to store your credentials.

```sql
-- Save your Infura API key in Vault and retrieve the created `key_id`
select vault.create_secret(
  '<Infura API Key>',
  'infura',
  'Infura API key for blockchain data access'
);
```

### Connecting to Infura

We need to provide Postgres with the credentials to access Infura and any additional options. We can do this using the `create server` command:

=== "With Vault"

    ```sql
    create server infura_server
      foreign data wrapper wasm_wrapper
      options (
        fdw_package_url 'https://github.com/supabase/wrappers/releases/download/wasm_infura_fdw_v0.1.0/infura_fdw.wasm',
        fdw_package_name 'supabase:infura-fdw',
        fdw_package_version '0.1.0',
        fdw_package_checksum '6cb829b851ea8cbd70cb893958826824388a4d9477305a16f2f489bcd569b35e',
        api_key_id '<key_ID>', -- The Key ID from above.
        network 'mainnet'  -- optional, defaults to mainnet
      );
    ```

=== "Without Vault"

    ```sql
    create server infura_server
      foreign data wrapper wasm_wrapper
      options (
        fdw_package_url 'https://github.com/supabase/wrappers/releases/download/wasm_infura_fdw_v0.1.0/infura_fdw.wasm',
        fdw_package_name 'supabase:infura-fdw',
        fdw_package_version '0.1.0',
        fdw_package_checksum '6cb829b851ea8cbd70cb893958826824388a4d9477305a16f2f489bcd569b35e',
        api_key '<your-infura-api-key>',
        network 'mainnet'  -- optional, defaults to mainnet
      );
    ```

Note the `fdw_package_*` options are required, which specify the Wasm package metadata. You can get the available package version list from [above](#available-versions).

### Supported Networks

The `network` option supports the following values:

| Network | Value | Chain ID |
| ------- | ----- | -------- |
| Ethereum Mainnet | `mainnet` | 1 |
| Ethereum Sepolia | `sepolia` | 11155111 |
| Polygon PoS | `polygon-mainnet` | 137 |
| Polygon Amoy | `polygon-amoy` | 80002 |
| Arbitrum One | `arbitrum-mainnet` | 42161 |
| Optimism | `optimism-mainnet` | 10 |
| Base | `base-mainnet` | 8453 |
| Linea | `linea-mainnet` | 59144 |

### Create a schema

We recommend creating a schema to hold all the foreign tables:

```sql
create schema if not exists infura;
```

## Options

The full list of foreign table options are below:

- `resource` - Resource type to query, required. One of: `blocks`, `transactions`, `balances`, `logs`, `chain_info`

## Entities

Below are all the entities supported by this FDW. Each entity maps to a specific JSON-RPC method on the Infura API.

We can use SQL [import foreign schema](https://www.postgresql.org/docs/current/sql-importforeignschema.html) to import foreign table definitions from Infura.

For example, using below SQL can automatically create foreign tables in the `infura` schema.

```sql
-- create all the foreign tables
import foreign schema infura from server infura_server into infura;

-- or, create selected tables only
import foreign schema infura
   limit to ("eth_blocks", "eth_transactions")
   from server infura_server into infura;

-- or, create all foreign tables except selected tables
import foreign schema infura
   except ("eth_blocks")
   from server infura_server into infura;
```

### Blocks

Query Ethereum block data using `eth_getBlockByNumber`.

Ref: [Infura API docs](https://docs.infura.io/api/networks/ethereum/json-rpc-methods/eth_getblockbynumber)

#### Operations

| Object | Select | Insert | Update | Delete | Truncate |
| ------ | :----: | :----: | :----: | :----: | :------: |
| blocks |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

#### Usage

```sql
create foreign table infura.eth_blocks (
  number numeric,
  hash text,
  parent_hash text,
  timestamp timestamp,
  miner text,
  gas_used numeric,
  gas_limit numeric,
  transaction_count bigint,
  base_fee_per_gas numeric,
  attrs jsonb
)
  server infura_server
  options (
    resource 'blocks'
  );
```

#### Query Pushdown

- `number` - Filter by block number. For example, `WHERE number = 19000000`.

### Transactions

Query transaction data using `eth_getTransactionByHash`.

Ref: [Infura API docs](https://docs.infura.io/api/networks/ethereum/json-rpc-methods/eth_gettransactionbyhash)

#### Operations

| Object       | Select | Insert | Update | Delete | Truncate |
| ------------ | :----: | :----: | :----: | :----: | :------: |
| transactions |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

#### Usage

```sql
create foreign table infura.eth_transactions (
  hash text,
  block_number numeric,
  block_hash text,
  from_address text,
  to_address text,
  value numeric,
  gas numeric,
  gas_price numeric,
  nonce numeric,
  input text,
  transaction_index numeric,
  attrs jsonb
)
  server infura_server
  options (
    resource 'transactions'
  );
```

#### Query Pushdown

- `hash` - **Required**. Filter by transaction hash: `WHERE hash = '0x...'`

!!! note
    Transaction queries require a `hash` filter in the WHERE clause.

### Balances

Query account balances using `eth_getBalance`.

Ref: [Infura API docs](https://docs.infura.io/api/networks/ethereum/json-rpc-methods/eth_getbalance)

#### Operations

| Object   | Select | Insert | Update | Delete | Truncate |
| -------- | :----: | :----: | :----: | :----: | :------: |
| balances |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

#### Usage

```sql
create foreign table infura.eth_balances (
  address text,
  balance text,
  block text
)
  server infura_server
  options (
    resource 'balances'
  );
```

#### Query Pushdown

- `address` - **Required**. Filter by account address: `WHERE address = '0x...'`
- `block` - Optional block number (defaults to `latest`)

!!! note
    Balance queries require an `address` filter in the WHERE clause. The balance is returned in ETH (1 ETH = 10^18 Wei).

### Logs

Query event logs using `eth_getLogs`.

Ref: [Infura API docs](https://docs.infura.io/api/networks/ethereum/json-rpc-methods/eth_getlogs)

#### Operations

| Object | Select | Insert | Update | Delete | Truncate |
| ------ | :----: | :----: | :----: | :----: | :------: |
| logs   |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

#### Usage

```sql
create foreign table infura.eth_logs (
  address text,
  block_number numeric,
  block_hash text,
  transaction_hash text,
  transaction_index numeric,
  log_index numeric,
  data text,
  topics jsonb,
  removed boolean,
  attrs jsonb
)
  server infura_server
  options (
    resource 'logs'
  );
```

#### Query Pushdown

- `address` - Filter by contract address
- `block_hash` - Filter by block hash

### Chain Info

Query chain metadata using `eth_chainId`, `eth_blockNumber`, and `eth_gasPrice`.

#### Operations

| Object     | Select | Insert | Update | Delete | Truncate |
| ---------- | :----: | :----: | :----: | :----: | :------: |
| chain_info |   ✅   |   ❌   |   ❌   |   ❌   |    ❌    |

#### Usage

```sql
create foreign table infura.eth_chain_info (
  network text,
  chain_id numeric,
  block_number numeric,
  gas_price numeric
)
  server infura_server
  options (
    resource 'chain_info'
  );
```

## Supported Data Types

| Postgres Data Type | Blockchain Data Type |
| ------------------ | -------------------- |
| numeric            | Hex uint64 (parsed) |
| text               | Hex uint256 (parsed) |
| text               | Hex address/hash     |
| bigint             | Integer              |
| boolean            | Boolean              |
| timestamp          | Unix timestamp       |
| jsonb              | JSON object          |

## Limitations

This section describes important limitations and considerations when using this FDW:

- **Read-only**: Blockchain data is immutable. This FDW only supports SELECT operations.
- **Rate limiting**: Infura API has rate limits. Consider using materialized views for frequently accessed data.
- **Large numeric values**: Ethereum values (like Wei balances) can be extremely large. Use `text` type for these columns.
- **Block range limits**: For `eth_getLogs`, Infura may limit the block range you can query at once.

## Examples

Below are some examples on how to use Infura foreign tables.

### Query latest block

```sql
create foreign table infura.eth_blocks (
  number numeric,
  hash text,
  parent_hash text,
  timestamp timestamp,
  miner text,
  gas_used numeric,
  gas_limit numeric,
  transaction_count bigint,
  base_fee_per_gas numeric,
  attrs jsonb
)
  server infura_server
  options (
    resource 'blocks'
  );

-- Query the latest block
select * from infura.eth_blocks;

-- Query a specific block
select * from infura.eth_blocks where number = 19000000;
```

### Query account balance

```sql
create foreign table infura.eth_balances (
  address text,
  balance text,
  block text
)
  server infura_server
  options (
    resource 'balances'
  );

-- Query Vitalik's wallet balance (in ETH)
select
  address,
  balance as balance_eth
from infura.eth_balances
where address = '0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045';
```

### Query transaction details

```sql
create foreign table infura.eth_transactions (
  hash text,
  block_number numeric,
  block_hash text,
  from_address text,
  to_address text,
  value numeric,
  gas numeric,
  gas_price numeric,
  nonce numeric,
  input text,
  transaction_index numeric,
  attrs jsonb
)
  server infura_server
  options (
    resource 'transactions'
  );

-- Query a specific transaction
select
  from_address,
  to_address,
  value / 1e18 as value_eth
from infura.eth_transactions
where hash = '0x5c504ed432cb51138bcf09aa5e8a410dd4a1e204ef84bfed1be16dfba1b22060';
```

### Query event logs

```sql
create foreign table infura.eth_logs (
  address text,
  block_number numeric,
  block_hash text,
  transaction_hash text,
  transaction_index numeric,
  log_index numeric,
  data text,
  topics jsonb,
  removed boolean,
  attrs jsonb
)
  server infura_server
  options (
    resource 'logs'
  );

-- Query logs from a specific contract
select * from infura.eth_logs
where address = '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48';
```

### Query chain information

```sql
create foreign table infura.eth_chain_info (
  network text,
  chain_id numeric,
  block_number numeric,
  gas_price numeric
)
  server infura_server
  options (
    resource 'chain_info'
  );

-- Get current chain status
select
  network,
  chain_id,
  block_number,
  gas_price / 1e9 as gas_price_gwei
from infura.eth_chain_info;
```

### Query Polygon network

```sql
-- Create a separate server for Polygon
create server polygon_server
  foreign data wrapper wasm_wrapper
  options (
    fdw_package_url 'https://github.com/supabase/wrappers/releases/download/wasm_infura_fdw_v0.1.0/infura_fdw.wasm',
    fdw_package_name 'supabase:infura-fdw',
    fdw_package_version '0.1.0',
    fdw_package_checksum '6cb829b851ea8cbd70cb893958826824388a4d9477305a16f2f489bcd569b35e',
    api_key '<your-infura-api-key>',
    network 'polygon-mainnet'
  );

create foreign table infura.polygon_blocks (
  number numeric,
  hash text,
  parent_hash text,
  timestamp timestamp,
  miner text,
  gas_used numeric,
  gas_limit numeric,
  transaction_count bigint,
  base_fee_per_gas numeric,
  attrs jsonb
)
  server polygon_server
  options (
    resource 'blocks'
  );

select * from infura.polygon_blocks;
```
