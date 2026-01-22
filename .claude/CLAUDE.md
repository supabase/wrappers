# Wrappers

Postgres Foreign Data Wrapper (FDW) framework in Rust, by Supabase.

## Project Structure

```
wrappers/                    # Native FDWs (pgrx-based)
  src/fdw/                   # Individual FDW implementations
supabase-wrappers/           # Core FDW framework library
supabase-wrappers-macros/    # Proc macros for FDW development
wasm-wrappers/               # WASM-based FDWs (separate from workspace)
  fdw/                       # Individual WASM FDW implementations
  wit/                       # WebAssembly Interface Types
```

## Build Commands

```bash
# Native wrappers (requires pgrx)
cargo build
cargo pgrx install --pg-config [path] --features stripe_fdw

# WASM wrappers
cd wasm-wrappers/fdw/<name>
cargo component build --release
```

## Testing

- Native FDW tests: `cargo pgrx test`
- WASM FDW tests: SQL files in `wasm-wrappers/fdw/<name>/tests/`

## Key Files

- `/SECURITY.md` - Platform-wide security documentation
- `supabase-wrappers/src/utils.rs` - Shared utilities (credential masking)
- `wrappers/src/fdw/wasm_fdw/` - WASM FDW host implementation

## Security Requirements

- All WASM FDWs require `fdw_package_checksum` option
- Use `sanitize_error_message()` to mask credentials in errors
- HTTP FDWs support `max_response_size` option (default 10MB)

## Adding a New Native FDW

1. Create module in `wrappers/src/fdw/<name>_fdw/`
2. Implement `ForeignDataWrapper` trait
3. Add feature flag to `wrappers/Cargo.toml`
4. Use `sanitize_error_message()` for error handling

## Adding a New WASM FDW

1. Create directory `wasm-wrappers/fdw/<name>_fdw/`
2. Implement the WIT interface from `wasm-wrappers/wit/`
3. Add SQL tests in `tests/` subdirectory
