# CLAUDE.md - AI Assistant Guide for Wrappers

## Project Overview

Wrappers is a development framework for PostgreSQL Foreign Data Wrappers (FDWs), written in Rust. It enables querying external data sources (APIs, databases, files) as if they were regular PostgreSQL tables. The project is maintained by [Supabase](https://supabase.com).

**Documentation**: https://fdw.dev/ | **API Docs**: https://docs.rs/supabase-wrappers

## Repository Structure

```
wrappers/
├── supabase-wrappers/           # Core FDW framework library (crates.io: supabase-wrappers)
├── supabase-wrappers-macros/    # Procedural macros (#[wrappers_fdw])
├── wrappers/                    # Native FDW implementations (PostgreSQL extension)
│   └── src/fdw/                 # Individual FDW implementations
├── wasm-wrappers/               # WebAssembly-based FDW implementations (separate workspace)
│   └── fdw/                     # Individual Wasm FDW crates
└── docs/                        # MkDocs documentation site
```

## Workspace Configuration

The project uses Cargo workspaces with the following structure:

- **Main workspace** (`Cargo.toml`): Contains `supabase-wrappers`, `supabase-wrappers-macros`, and `wrappers`
- **Wasm workspace** (`wasm-wrappers/fdw/Cargo.toml`): Separate workspace for Wasm FDWs (excluded from main)

**Rust version**: 1.88.0 (specified in `workspace.package`)
**pgrx version**: 0.16.1 (PostgreSQL extension framework)

## Key Components

### supabase-wrappers (Core Framework)

The core library providing the `ForeignDataWrapper` trait (`supabase-wrappers/src/interface.rs`):

```rust
pub trait ForeignDataWrapper<E: Into<ErrorReport>> {
    // Required methods
    fn new(server: ForeignServer) -> Result<Self, E>;
    fn begin_scan(&mut self, quals: &[Qual], columns: &[Column], sorts: &[Sort], limit: &Option<Limit>, options: &HashMap<String, String>) -> Result<(), E>;
    fn iter_scan(&mut self, row: &mut Row) -> Result<Option<()>, E>;
    fn end_scan(&mut self) -> Result<(), E>;

    // Optional methods for modification
    fn begin_modify(&mut self, options: &HashMap<String, String>) -> Result<(), E>;
    fn insert(&mut self, row: &Row) -> Result<(), E>;
    fn update(&mut self, rowid: &Cell, new_row: &Row) -> Result<(), E>;
    fn delete(&mut self, rowid: &Cell) -> Result<(), E>;
    fn end_modify(&mut self) -> Result<(), E>;

    // Optional methods
    fn re_scan(&mut self) -> Result<(), E>;
    fn get_rel_size(...) -> Result<(i64, i32), E>;
    fn import_foreign_schema(...) -> Result<Vec<String>, E>;
    fn validator(options: Vec<Option<String>>, catalog: Option<Oid>) -> Result<(), E>;
}
```

### Key Data Types (interface.rs)

- `Cell`: Enum representing data values (Bool, I8-I64, F32-F64, String, Date, Timestamp, Json, Uuid, arrays)
- `Row`: Collection of column names and cells
- `Column`: Column metadata (name, number, type_oid)
- `Qual`: WHERE clause predicate (field, operator, value, use_or)
- `Sort`: ORDER BY specification
- `Limit`: LIMIT/OFFSET values

### supabase-wrappers-macros

Provides the `#[wrappers_fdw]` attribute macro that generates:
- `<name>_fdw_handler()` - FDW handler entry point
- `<name>_fdw_validator()` - Option validation function
- `<name>_fdw_meta()` - Metadata function

Usage:
```rust
#[wrappers_fdw(
    version = "0.1.0",
    author = "Supabase",
    website = "https://example.com",
    error_type = "MyFdwError"  // Required
)]
pub struct MyFdw { ... }
```

## Available FDWs

### Native FDWs (wrappers/src/fdw/)

| FDW | Feature Flag | Supports Write |
|-----|--------------|----------------|
| BigQuery | `bigquery_fdw` | Yes |
| ClickHouse | `clickhouse_fdw` | Yes |
| Stripe | `stripe_fdw` | Yes |
| S3 Vectors | `s3vectors_fdw` | Yes |
| S3 | `s3_fdw` | No |
| Firebase | `firebase_fdw` | No |
| Airtable | `airtable_fdw` | No |
| Auth0 | `auth0_fdw` | No |
| AWS Cognito | `cognito_fdw` | No |
| DuckDB | `duckdb_fdw` | No |
| Apache Iceberg | `iceberg_fdw` | No |
| Logflare | `logflare_fdw` | No |
| Redis | `redis_fdw` | No |
| SQL Server | `mssql_fdw` | No |
| HelloWorld | `helloworld_fdw` | No (demo) |

### Wasm FDWs (wasm-wrappers/fdw/)

Cal.com, Calendly, Clerk, Cloudflare D1, HubSpot, Infura, Notion, Orb, Paddle, Shopify, Slack, Snowflake

## Development Workflows

### Prerequisites

```bash
# Install Rust toolchain
rustup install 1.88.0
rustup default 1.88.0

# Install pgrx
cargo install --locked cargo-pgrx --version 0.16.1

# Initialize pgrx with PostgreSQL
cargo pgrx init --pg15 /usr/lib/postgresql/15/bin/pg_config

# For Wasm development
cargo install --locked cargo-component --version 0.21.1
rustup target add wasm32-unknown-unknown
```

### Building

```bash
# Build native FDWs
cd wrappers
cargo build --features "native_fdws pg15"

# Build specific FDW
cargo build --features "stripe_fdw pg15"

# Build Wasm FDWs
cd wasm-wrappers/fdw
cargo component build --release --target wasm32-unknown-unknown
```

### Running Interactive Development

```bash
cd wrappers
cargo pgrx run pg15 --features stripe_fdw
```

Then in psql:
```sql
create extension if not exists wrappers;
create foreign data wrapper stripe_wrapper
  handler stripe_fdw_handler
  validator stripe_fdw_validator;
```

### Testing

```bash
# Start test containers
cd wrappers
docker compose -f .ci/docker-compose-native.yaml up -d

# Run all native FDW tests
cargo pgrx test --features "native_fdws pg15"

# Run specific FDW tests
cargo pgrx test --features "stripe_fdw pg15"

# Run Wasm FDW tests (requires building Wasm packages first)
docker compose -f .ci/docker-compose-wasm.yaml up -d
cargo pgrx test --features "wasm_fdw pg15"
```

### Code Quality

```bash
# Format check
cargo fmt --check

# Clippy (must pass with -D warnings)
RUSTFLAGS="-D warnings" cargo clippy --all --tests --no-deps --features native_fdws,helloworld_fdw
```

### Installing Extension

```bash
cargo pgrx install --pg-config /path/to/pg_config --features stripe_fdw
```

## Code Patterns and Conventions

### Error Handling Pattern

Each FDW defines a custom error type:

```rust
use thiserror::Error;

#[derive(Error, Debug)]
enum MyFdwError {
    #[error("API error: {0}")]
    ApiError(String),
    #[error("Invalid option: {0}")]
    InvalidOption(String),
}

impl From<MyFdwError> for ErrorReport {
    fn from(e: MyFdwError) -> Self {
        ErrorReport::new(PgSqlErrorCode::ERRCODE_FDW_ERROR, e.to_string(), "")
    }
}
```

### FDW Implementation Pattern

```rust
use supabase_wrappers::prelude::*;

#[wrappers_fdw(
    version = "0.1.0",
    author = "Author",
    website = "https://example.com",
    error_type = "MyFdwError"
)]
pub struct MyFdw {
    // State fields
    client: Option<Client>,
    rows: Vec<Row>,
    row_idx: usize,
}

impl ForeignDataWrapper<MyFdwError> for MyFdw {
    fn new(server: ForeignServer) -> Result<Self, MyFdwError> {
        // Initialize from server options
        Ok(Self { client: None, rows: vec![], row_idx: 0 })
    }

    fn begin_scan(
        &mut self,
        quals: &[Qual],
        columns: &[Column],
        sorts: &[Sort],
        limit: &Option<Limit>,
        options: &HashMap<String, String>,
    ) -> Result<(), MyFdwError> {
        // Fetch data, apply pushdown
        self.row_idx = 0;
        Ok(())
    }

    fn iter_scan(&mut self, row: &mut Row) -> Result<Option<()>, MyFdwError> {
        if self.row_idx < self.rows.len() {
            row.replace_with(self.rows[self.row_idx].clone());
            self.row_idx += 1;
            Ok(Some(()))
        } else {
            Ok(None)
        }
    }

    fn end_scan(&mut self) -> Result<(), MyFdwError> {
        self.rows.clear();
        Ok(())
    }
}
```

### File Organization for FDWs

Each native FDW follows this structure:
```
wrappers/src/fdw/<name>_fdw/
├── mod.rs           # Error types and module exports
├── <name>_fdw.rs    # Main implementation
└── tests.rs         # pgrx tests
```

### Query Pushdown

FDWs receive pushdown hints through `begin_scan`:
- `quals`: WHERE predicates to filter at source
- `sorts`: ORDER BY to sort at source
- `limit`: LIMIT/OFFSET to limit at source

Use `Qual::deparse()` to convert to SQL-like strings.

## PostgreSQL Version Support

Supported via feature flags: `pg13`, `pg14`, `pg15` (default), `pg16`, `pg17`, `pg18`

## CI/CD

GitHub Actions workflows in `.github/workflows/`:
- `test_wrappers.yml`: Tests native and Wasm FDWs
- `test_supabase_wrappers.yml`: Tests core framework
- `release.yml`: Releases native FDWs
- `release_wasm_fdw.yml`: Releases Wasm FDWs
- `coverage.yml`: Code coverage
- `docs.yml`: Documentation deployment

## Common Tasks

### Adding a New Native FDW

1. Create directory `wrappers/src/fdw/<name>_fdw/`
2. Add feature flag in `wrappers/Cargo.toml`
3. Add to `native_fdws` feature list
4. Implement `ForeignDataWrapper` trait
5. Add tests in `tests.rs`
6. Add documentation in `docs/catalog/`

### Debugging

Use pgrx `notice!` macro for debug output:
```rust
use pgrx::notice;
notice!("Debug: {:?}", value);
```

### Working with Options

Options come from `CREATE SERVER` and `CREATE FOREIGN TABLE`:
```rust
fn begin_scan(&mut self, ..., options: &HashMap<String, String>) {
    let table_name = options.get("table").unwrap_or(&"default".to_string());
}
```

## Important Notes

- Native FDW contributions are not currently accepted until API stabilizes (v1.0)
- Wasm FDWs are preferred for community contributions
- Materialized views with foreign tables may cause backup restoration issues
- Use `rowid_column` table option to enable INSERT/UPDATE/DELETE operations
