FROM postgres:14
RUN apt-get update
ENV build_deps ca-certificates \
  git \
  build-essential \
  libpq-dev \
  postgresql-server-dev-14 \
  curl \
  libreadline6-dev \
  zlib1g-dev
RUN apt-get install -y --no-install-recommends $build_deps pkg-config cmake
WORKDIR /home/app
ENV HOME=/home/app
ENV PATH=/home/app/.cargo/bin:$PATH
RUN chown postgres:postgres /home/app
USER postgres
RUN \
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --no-modify-path --profile minimal --default-toolchain nightly && \
  rustup --version && \
  rustc --version && \
  cargo --version
# PGX
RUN cargo install cargo-pgx
RUN cargo pgx init --pg14 $(which pg_config)
USER root

# Install the extension
COPY supabase-wrappers supabase-wrappers
COPY supabase-wrappers-macros supabase-wrappers-macros
WORKDIR /home/app/wrappers
COPY wrappers/Cargo.toml Cargo.toml
COPY wrappers/Cargo.lock Cargo.lock
COPY wrappers/wrappers.control wrappers.control
COPY wrappers/bin bin
COPY wrappers/src src
RUN cargo pgx install --features clickhouse_fdw --features pg14
RUN cargo pgx install --features stripe_fdw --features pg14


COPY wrappers/test test
RUN chown -R postgres:postgres /home/app

USER postgres
