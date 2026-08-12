# zarr_fdw — Progress Backup (Session checkpoint)

> Cross-session checkpoint for the `zarr_fdw` work on the `zarr-fdw` branch.
> This file is git-ignored on purpose (see `.gitignore` entry `research/`).

## 1. Where we are right now (snapshot)

- **Goal:** native Rust PG FDW that queries cloud-native Zarr arrays (e.g.
  Sentinel-2 cubes) from Postgres with predicate pushdown on `time`/`x`/`y`,
  without materializing the cube. Read-only, single-array, S3 backend MVP.
- **State:** the wrapper **compiles cleanly** (`cargo check` lib + `--all-targets`,
   0 errors, 0 warnings) against **PG 15** inside a container. All **23 pure unit
   tests pass** in-crate, and the **3 `#[pg_test]` DDL tests pass** under the
   pgrx-tests PG15 harness (container built + run this session; see §5). `cargo fmt
   --check` is clean and the repo clippy line (`-D warnings`, `native_fdws`) passes
   with `zarr_fdw` included.
- **Blocked recap:** `pgrx-pg-sys 0.16.1` cannot bind against locally installed
  **PostgreSQL 18.4** headers (`HeapTupleHeaderData` becomes opaque; `no field
  't_bits'` errors in `pgrx-pg-sys/src/submodules/htup.rs`). This is a
  **dependency/PG-version incompatibility**, not a zarr_fdw code bug. The repo's CI
  targets **PG 15**, which pgrx 0.16.1 fully supports — hence the container below.

## 2. What was implemented

All code lives in `wrappers/src/fdw/zarr_fdw/`:

| File | Purpose |
|------|---------|
| `mod.rs` | `ZarrFdwError` enum + `From<…>` impls, module declarations |
| `zarr_fdw.rs` | Main `#[wrappers_fdw] struct ZarrFdw` — `new`, `begin_scan`,
  `iter_scan`, `re_scan`, `end_scan`, `validator`; qual→value-range translation,
  lazy per-chunk fetch/decode, flat `(x, y, time, value)` row emission |
| `chunk.rs` | Pure chunk math: `index_bounds_from_value_range` (asc/desc coords),
  `axis_chunk_ranges`, `enumerate_chunks` (C order), `chunk_key` |
| `meta.rs` | Zarr v2 `.zarray` parsing + validation (`from_bytes`, `parse`,
  `validate_coordinate`), `chunks_per_axis`, dimension separator handling |
| `store.rs` | S3 store (`ZarrStore`, `StoreUrl::parse`), credentials from
  `CREATE SERVER` options, `get_object_sync` via a tokio block_on |
| `decode.rs` | `DType` parse (`<f4`, `|u1`, `<i2`, …), `Codec` (zstd/raw MVP),
  `coord_bytes_to_f64`, `value_cell` bytes→`Cell` |
| `tests.rs` | `#[pg_test]` DDL smoke tests (1 positive + 2 validator negatives) |

Wiring: `wrappers/Cargo.toml` gained a `zarr_fdw` feature (added to `native_fdws`),
`wrappers/src/fdw/mod.rs` gained `#[cfg(feature = "zarr_fdw")] mod zarr_fdw;`.

### Design decisions (MVP)
- Single **value column** at most (everything non-coordinate gets the array scalar).
- Rows keyed by coordinates; **x and y columns are required** in `tgt_cols`
  (`MissingCoordinateColumn` error otherwise).
- `array_group` table option picks the array sub-path inside the store.
- `time_unit`/`time_origin` table options map raw `time` coords ⇄ timestamptz.
- Chunks are always fetched whole; QUAL pruning happens at **chunk-list selection**;
  within-chunk masking via the `sub_lo/sub_hi/sub_idx` window.
- Edge chunks unpadded: byte length = product of *effective* shape × item size.
- Coordinates: separate 1D Zarr arrays read as siblings of the cube array.
- Spatial PostGIS quals (`ST_Intersects`, `geom && box`) do **not** reach the FDW
  (framework only passes simple Var-op-Const quals) → deferred to v1.

### Bug fixes this session (part of validation)
1. `zarr_fdw.rs: read_coordinate_array` chained `validate_coordinate()?` (returns
   `Result<(), _>`) onto `parse()`, turning `meta` into `()` → split the calls.
2. `mod.rs` was missing the `InvalidOptionValue` variant used by `zarr_fdw.rs`
   (and pinned by the summary) → added it; dropped unused `InvalidTableOption`
   and `ArrayNotFound` variants (would fail CI `-D warnings` clippy).
3. `store.rs` `impl TryFrom<&str> for StoreUrl` leaked private `ZarrFdwError`
   (E0446) → removed the impl + its test; made `StoreUrl`/`ZarrStore`
   `pub(crate)`.
4. `chunk.rs`: removed unused `IndexBounds::len` and `num_chunks` (dead-code
   warnings under `-D warnings`).
5. Added the required-x/y check in `begin_scan` so `MissingCoordinateColumn`
   is actually reachable.

## 3. Verified so far

- `cargo check -p wrappers --no-default-features --features zarr_fdw,pg15` → **clean**.
- `cargo check … --all-targets` → **clean** (tests/config etc. compile).
- `cargo test -p wrappers … --lib -- --skip pg_zarr` → **23 passed, 0 failed**.
- Full crate e2e (`CREATE EXTENSION` + scan) NOT yet run — needs the pg_test
  harness (§5).

## 4. The "local PG15 test container" approach

Host machine has **PG 18.4 only** (Arch `postgresql`), which pgrx 0.16.1 can't
bind. Solution: an ephemeral **Debian bookworm `rust:1.88`** container that ships
**PG 15 + clang + cargo-pgrx + sudo** and a `~/.pgrx/config.toml` mapping
`pg15`, used for all local build/test verification. This mirrors CI
(`.github/workflows/test_wrappers.yml` uses `postgresql-15` + `cargo pgrx init
--pg15`).

The Dockerfile is now tracked in the repo at
**`wrappers/.ci/pgrx-15.Dockerfile`** (see §6). Local working copy:
`/tmp/zarr_pg15/Dockerfile`.

### Usage
```bash
# 1. build once (installs PG15, clang, cargo-pgrx 0.16.1, sudo; ~5-10 min)
docker build -t wrappers-pgrx-pg15 -f wrappers/.ci/pgrx-15.Dockerfile wrappers

# 2. compile check against PG15 (fast, incremental if you keep a persistent target)
docker run --rm \
  -e CARGO_TARGET_DIR=/tmp/target \
  -v "$PWD":/work -w /work \
  wrappers-pgrx-pg15 \
  cargo check -p wrappers --no-default-features --features zarr_fdw,pg15

# 3. pure unit tests
docker run --rm \
  -e CARGO_TARGET_DIR=/tmp/target \
  -v "$PWD":/work -w /work \
  wrappers-pgrx-pg15 \
  cargo test -p wrappers --no-default-features --features zarr_fdw,pg15 --lib -- --skip pg_zarr

# 4. pg_test DDL tests (extension install as root, postmaster via sudo as 'builder')
docker run --rm \
  -e CARGO_TARGET_DIR=/tmp/target \
  -e CARGO_PGRX_TEST_RUNAS=builder \
  -v "$PWD":/work -w /work \
  wrappers-pgrx-pg15 \
  cargo test -p wrappers --no-default-features --features zarr_fdw,pg15 --lib pg_zarr -- --test-threads=1
```

Notes:
- Persist the compiled artifacts between runs with `-v /tmp/pgrx-target:/tmp/target`
  (root-owned; `--all-targets` reuse works because cargo fingerprints live in the
  target dir, no registry needed after the first build).
- pgrx-tests' `initialize_test_framework` runs `cargo-pgrx install --test
  --pg-config /usr/lib/postgresql/15/bin/pg_config --features '<same features> pg_test'`,
  then `initdb`/`pg_ctl start` as the **non-root** `builder` user (PG refuses to
  run as root) via `CARGO_PGRX_TEST_RUNAS=builder` + `sudo`.
- We discovered the host `~/.cargo` has **no `bin/cargo`** (rustup lives
  elsewhere), so don't mount host `~/.cargo` over the image's `/usr/local/cargo`.

### Environment details that bit us (keep for future sessions)
- pgrx needs `~/.pgrx/config.toml` (host copy exists, maps `pg18`); the container
  maps `pg15`. `pgrx-pg-sys` reads `$PGRX_HOME` default `~/.pgrx`.
- Running the pg-test **binary directly** (not via `cargo test`) breaks the
  framework (`Could not initialize test framework: Unable to get target directory
  from cargo metadata` + `Could not obtain test mutex` cascade) — always invoke
  through `cargo test` so pgrx's sysinfo parent-process scan picks up the
  `--features` args.

## 5. In-flight / next steps (not done)

1. ~~Finish container build~~ — **DONE this session.** `docker build -t
   wrappers-pgrx-pg15 ...` completed (~2 min for the `cargo install cargo-pgrx
   0.16.1` step). Image built and validated.
2. ~~Run the pg_test suite~~ — **DONE this session.** All **3 `#[pg_test]` DDL
   tests pass** (see §7). Along the way fixed two real bugs (below).
3. ~~Sanity: fmt + clippy~~ — **DONE this session.** `cargo fmt --check` clean
   across the whole repo; `RUSTFLAGS="-D warnings" cargo clippy --all --tests
   --no-deps --features native_fdws,helloworld_fdw` passes (zarr_fdw included).
4. Nice-to-haves: FDW README/Doc page, README `CREATE SERVER`/`CREATE FOREIGN
   TABLE` example, real e2e scan vs. MinIO + a small Zarr store, CTE-friendly
   `begin_aggregate_scan` (later).
5. Anything touching `wrappers/Cargo.toml` / `fdw/mod.rs` should stay minimal and
   match upstream patterns; keep `research/`-only scratch docs git-ignored.

### Two bugs fixed this session (part of validation)
- **`tests.rs` `#[pg_test(error=…)]` mismatch.** pgrx's `error=` attribute does an
  **exact** (case-sensitive) match on the Postgres error text, not a substring
  match. `zarr_validator_rejects_bad_time_unit` expected `"must be one of: …"`
  but the validator emits `"invalid value for option 'fortnights': must be one
  of: …"` (see `mod.rs` `InvalidOptionValue`). Changed the test's `error=` to the
  full exact string → test now passes.
- **Clippy `-D warnings` (5 lints).** Fixed: redundant `'static` on `FDW_NAME`
  const; two `needless_range_loop`s (the `eff`/`bounds` loops now use
  `.iter().enumerate()`); two `implicit_saturating_sub` (`p = p.saturating_sub(1)`
  in `chunk.rs`). All behavior-preserving.

### CRITICAL: build target must NOT live on `/tmp` (tmpfs)
`/tmp` on the host is a **tmpfs (~8 GiB)**. The full `pgrx` + `aws-sdk-s3` +
`aws-lc-sys`/`ring` C build writes gigabytes of artifacts **and** temp `.s` files
into `CARGO_TARGET_DIR`. When `CARGO_TARGET_DIR` pointed at a bind mount under
`/tmp`, the tmpfs filled to 100%, and the pgrx-tests framework then failed with a
**spurious** `Could not obtain test mutex. A previous test may have hard-aborted
while holding it.` (the real cause was disk-full, not a lock).
**Fix:** mount `CARGO_TARGET_DIR` from real disk, e.g. `/home/<you>/pgrx-target`,
and also set `TMPDIR=/target/tmp` inside the container so `cc` temp files stay off
the tmpfs. The `wrappers/.ci/pgrx-15.Dockerfile` usage notes were updated to say
this. (Clearing `test-pgdata` alone does NOT fix it — the tmpfs is the root cause.)

## 6. Repo additions this checkpoint

- `wrappers/.ci/pgrx-15.Dockerfile` (tracked) — the test container. **Updated
  this session:** added `rustup component add rustfmt clippy` to the image, and
  rewrote the usage notes to use a real-disk `CARGO_TARGET_DIR` + `TMPDIR`.
- `research/zarr_fdw_progress.md` (untracked/ignored) — this checkpoint.
- Prior session already added `research/zarr_fdw_spec.md` (ignored),
  `.gitignore` `research/` entry.
- Working build cache now lives at `/home/mpsy/pgrx-target` (real disk), not
  `/tmp/pgrx-target`.

### Current `git status` (branch `zarr-fdw`)
```
M .gitignore
M wrappers/Cargo.toml
M wrappers/src/fdw/mod.rs
M wrappers/.ci/pgrx-15.Dockerfile
?? wrappers/src/fdw/zarr_fdw/        (6 new files)
```
No commits made on this topic yet; commit when the user asks.