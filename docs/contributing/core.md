[`supabase/wrappers`](https://github.com/supabase/wrappers) is open source. PRs and issues are welcome.

## Development

Requirements:

- [Rust](https://www.rust-lang.org/)
- [Cargo](https://doc.rust-lang.org/cargo/)
- [Docker](https://www.docker.com/)
- [pgrx](https://github.com/pgcentralfoundation/pgrx)

### Testing

Tests are located in each FDW's `tests.rs` file. For example, Stripe FDW tests are in `wrappers/src/fdw/stripe_fdw/tests.rs` file.

To run the tests locally, we need to start the mock containers first:

```bash
cd wrappers
docker-compose -f .ci/docker-compose.yaml up -d
```

And then build all Wasm FDW packages:

```bash
find ../wasm-wrappers/fdw/ -name "Cargo.toml" -exec cargo component build --release --target wasm32-unknown-unknown --manifest-path {} \;
```

Now we can run all the tests:

```bash
cargo pgrx test --features "all_fdws pg15"
```

You can also run the native or Wasm FDW tests individually:

```base
# run native FDW tests only
cargo pgrx test --features "native_fdws pg15"

# or run Wasm FDW tests only
cargo pgrx test --features "wasm_fdw pg15"
```

### Interactive PSQL Development

To reduce the iteration cycle, you may want to launch a psql prompt with `wrappers` installed to experiment a single FDW like below:

```bash
cd wrappers
cargo pgrx run pg15 --features clickhouse_fdw
```

Try out the SQLs in the psql prompt with the `wrappers` extension installed like below:

```sql
create extension if not exists wrappers;

create foreign data wrapper wasm_wrapper
  handler wasm_fdw_handler
  validator wasm_fdw_validator;
```

For debugging, you can make use of [`notice!` macros](https://docs.rs/pgrx/latest/pgrx/macro.notice.html) to print out statements while using your wrapper in `psql`.
