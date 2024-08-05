# Developing a Wasm Wrapper

## Fork the Wasm FDW Template

1. Go to [github.com/supabase-community/postgres-wasm-fdw](https://github.com/supabase-community/postgres-wasm-fdw)
2. Fork the repo to your own GitHub account by by clicking the `Use this template` button on top right of this page
3. Clone the repo to your local machine.

## Install pre-reqs

The template uses [The WebAssembly Component Model](https://component-model.bytecodealliance.org/) to build the WebAssembly package, so we need to install various dependencies.

- Install the Rust Toolchain if it is not already installed on your machine.
- Add `wasm32-unknown-unknown` target:
  ```bash
  rustup target add wasm32-unknown-unknown
  ```
- Install the [WebAssembly Component Model subcommand](https://github.com/bytecodealliance/cargo-component):
  ```bash
  cargo install cargo-component
  ```

## Implement your logic

- [ ] Todo

## Release the Wasm FDW package

To create a release of the Wasm FDW package, create a version tag and then push it. This will trigger a workflow to build the package and create a release on your repo.

```bash
git tag v0.1.0
git push origin v0.1.0
```

## Install your FDW

You can install the FDW on any Postgres instance that supports Wrappers `>=0.4.1`.

```sql
select *
from pg_available_extension_versions
where name = 'wrappers';
```

### Enable Wrappers

Enable the Wrappers extension and initialize the Wasm FDW:

```sql
create extension if not exists wrappers with schema extensions;

create foreign data wrapper wasm_wrapper
  handler wasm_fdw_handler
  validator wasm_fdw_validator;
```

### Install your Wrapper from GitHub

Create foreign server and foreign table like below,

```sql
create server example_server
  foreign data wrapper wasm_wrapper
  options (
    -- change below fdw_package_* options accordingly, find examples in the README.txt in your releases
    fdw_package_url 'https://github.com/supabase-community/wasm-fdw-example/releases/download/v0.1.0/wasm_fdw_example.wasm',
    fdw_package_name 'my-company:example-fdw',
    fdw_package_version '0.1.0',
    fdw_package_checksum '67bbe7bfaebac6e8b844813121099558ffe5b9d8ac6fca8fe49c20181f50eba8',
    api_url 'https://api.github.com'
  );

create schema github;

create foreign table github.events (
  id text,
  type text,
  actor jsonb,
  repo jsonb,
  payload jsonb,
  public boolean,
  created_at timestamp
)
  server example_server
  options (
    object 'events',
    rowid_column 'id'
  );
```

### Query your wrapper

Query the foreign table to see what's happening on GitHub:

```sql
select
  id,
  type,
  actor->>'login' as login,
  repo->>'name' as repo,
  created_at
from
  github.events
limit 5;
```

<img width="812" alt="image" src="https://github.com/user-attachments/assets/53e963cb-6e8f-44f8-9f2e-f0edc73ddf3a">

:clap: :clap: Congratulations! You have built your first Wasm FDW.
