# pgrx + PG15 test container for the wrappers crate
#
# Debian bookworm (rust:1.97.1) with PostgreSQL 15, clang, CMake and
# cargo-pgrx 0.19.2, so native FDWs can be compiled and tested against the PG
# version the repo's CI targets (see .github/workflows/test_wrappers.yml).
#
# Image contents:
#   - rust:1.97.1 toolchain (matches workspace.package.rust-version)
#   - PostgreSQL 15 server + -dev headers, clang (bindgen), sudo
#   - cargo-pgrx 0.19.2 (used by the pgrx-tests framework)
#   - ~/.pgrx/config.toml mapping pg15 -> /usr/lib/postgresql/15/bin/pg_config
#   - non-root `builder` user (uid 1000): PostgreSQL refuses to run as root, so
#     pgrx-tests runs its postmaster as `builder` while installing the
#     extension as root (CARGO_PGRX_TEST_RUNAS=builder).
#
# Build (from repo root):
#   podman build -t wrappers-pgrx-pg15 -f wrappers/.ci/pgrx-15.Dockerfile wrappers
#
# Keep a persistent target dir for incremental repeats. IMPORTANT: put it on
# real disk, NOT under /tmp — /tmp is a tmpfs (typically ~8 GiB) and the full
# pgrx + aws-sdk-s3 + aws-lc-sys/ring build overflows it, which surfaces as a
# spurious pgrx-tests "Could not obtain test mutex" error. Use e.g.:
#   mkdir -p /home/<you>/pgrx-target/tmp
# and run with:
#   -e CARGO_TARGET_DIR=/target -e TMPDIR=/target/tmp \
#   -v /home/<you>/pgrx-target:/target
#
# Compile check:
#   podman run --rm \
#     -e CARGO_TARGET_DIR=/target -e TMPDIR=/target/tmp \
#     -v /home/<you>/pgrx-target:/target \
#     -v "$PWD":/work -w /work wrappers-pgrx-pg15 \
#     cargo check -p wrappers --no-default-features --features zarr_fdw,pg15
#
# Unit tests (pure #[test] modules):
#   podman run --rm \
#     -e CARGO_TARGET_DIR=/tmp/target -v /tmp/pgrx-target:/tmp/target \
#     -v "$PWD":/work -w /work wrappers-pgrx-pg15 \
#     cargo test -p wrappers --no-default-features --features zarr_fdw,pg15 \
#       --lib -- --skip pg_zarr
#
# #[pg_test] DDL tests (throws up a throwaway PG15 serving the extension):
#   podman run --rm \
#     -e CARGO_TARGET_DIR=/tmp/target -e CARGO_PGRX_TEST_RUNAS=builder \
#     -v /tmp/pgrx-target:/tmp/target -v "$PWD":/work -w /work wrappers-pgrx-pg15 \
#     cargo test -p wrappers --no-default-features --features zarr_fdw,pg15 \
#       --lib pg_zarr -- --test-threads=1
#
# Notes:
#   - Always launch through `cargo test` (not the test binary directly): the
#     pgrx-tests framework walks the process tree to discover the cargo
#     `--features` it must hand to `cargo-pgrx install --test`.
#   - Do NOT mount the host ~/.cargo over /usr/local/cargo (host rustup keeps
#     its bin/ elsewhere, shadowing cargo with an empty directory).
#   - sudo + the `builder` user + "chmod o+rx /root" let pgrx install as root
#     while the postmaster runs as a regular user.

FROM rust:1.97.1-bookworm

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      clang \
      cmake \
      postgresql-15 \
      postgresql-15-postgis-3 \
      postgresql-15-postgis-3-scripts \
      postgresql-server-dev-15 \
      sudo \
 && rm -rf /var/lib/apt/lists/*

RUN chmod o+rx /root \
 && useradd -m -u 1000 builder \
 && mkdir -p /tmp/pgtarget \
 && chown -R builder:builder /tmp/pgtarget

RUN cargo install --locked cargo-pgrx --version 0.19.2 \
 && cargo pgrx init --pg15 /usr/lib/postgresql/15/bin/pg_config \
 && rustup component add rustfmt clippy \
 && rm -rf /usr/local/cargo/registry/src /usr/local/cargo/registry/cache
