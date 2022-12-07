FROM postgres:15
RUN apt-get update
ENV build_deps ca-certificates \
  git \
  build-essential \
  libpq-dev \
  postgresql-server-dev-15 \
  curl \
  libreadline6-dev \
  zlib1g-dev
RUN apt-get install -y --no-install-recommends $build_deps pkg-config cmake
WORKDIR /home/app
ENV HOME=/home/app
ENV PATH=/home/app/.cargo/bin:$PATH
ENV LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH
RUN chown postgres:postgres /home/app
USER postgres

# Install Rust
RUN \
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --no-modify-path --profile minimal --default-toolchain nightly && \
  rustup --version && \
  rustc --version && \
  cargo --version

# PGX
RUN cargo install cargo-pgx --version 0.6.0
RUN cargo pgx init --pg15 $(which pg_config)
USER root

# Install libsodium
RUN \
  curl https://download.libsodium.org/libsodium/releases/libsodium-1.0.18-stable.tar.gz --output libsodium.tar.gz && \
  tar zxvf libsodium.tar.gz && \
  cd libsodium-stable && \
  ./configure && make && make install

# Install pgsodium
RUN \
  git clone https://github.com/michelp/pgsodium.git && \
  cd pgsodium && \
  make && make install && \
  cp getkey_scripts/pgsodium_getkey_urandom.sh /usr/share/postgresql/15/extension/pgsodium_getkey.sh

# Install Vault
RUN \
  git clone https://github.com/supabase/vault.git && \
  cd vault && \
  make install

# Install the extension
COPY supabase-wrappers supabase-wrappers
COPY supabase-wrappers-macros supabase-wrappers-macros
WORKDIR /home/app/wrappers
COPY wrappers/Cargo.toml Cargo.toml
COPY wrappers/Cargo.lock Cargo.lock
COPY wrappers/wrappers.control wrappers.control
COPY wrappers/bin bin
COPY wrappers/src src
RUN cargo pgx install --features pg15,clickhouse_fdw,firebase_fdw,bigquery_fdw,stripe_fdw

COPY wrappers/test test
RUN chown -R postgres:postgres /home/app

USER postgres
