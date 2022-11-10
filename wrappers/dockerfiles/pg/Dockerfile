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

# Cache as much as possible
COPY supabase-wrappers supabase-wrappers
COPY supabase-wrappers-macros supabase-wrappers-macros
RUN mkdir wrappers
COPY wrappers/Cargo.toml wrappers/
COPY wrappers/Cargo.lock wrappers/
COPY wrappers/wrappers.control wrappers/
COPY wrappers/.cargo wrappers/
RUN mkdir wrappers/src
# dummy file
COPY wrappers/dockerfiles/pg/lib.rs wrappers/src/lib.rs

WORKDIR /home/app/wrappers
RUN cargo pgx install --features bigquery_fdw --features pg14

# Copy in the actual source
# TODO: HERE HERE HERE
COPY wrappers/src src
COPY wrappers/bin bin
COPY wrappers/test test
RUN cargo pgx install --features bigquery_fdw --features pg14

RUN chown -R postgres:postgres /home/app
USER postgres

#COPY Cargo.toml
#COPY supabase-wrappers .
#COPY supabase-wrappers-macros .
#RUN mkdir wrappers
#COPY wrappers/Cargo.toml wrappers/Cargo.toml
#RUN cargo pgx install --features bigquery_fdw --features pg14
#RUN cargo pgx install --features bigquery_fdw --features pg14

