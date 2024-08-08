# Developing a Wasm Wrapper

This guide will create a GitHub FDW which you can use to query the [GitHub Events API](https://api.github.com/events) using Postgres.

## Install pre-requisites

The template uses [The WebAssembly Component Model](https://component-model.bytecodealliance.org/) to build the WebAssembly package, so we need to install various dependencies.

- Install the [Rust Toolchain](https://www.rust-lang.org/tools/install) if it is not already installed on your machine.
- Add `wasm32-unknown-unknown` target:
  ```bash
  rustup target add wasm32-unknown-unknown
  ```
- Install the [WebAssembly Component Model subcommand](https://github.com/bytecodealliance/cargo-component):
  ```bash
  cargo install cargo-component --locked --version 0.13.2
  ```

## Fork the Wasm FDW Template

Our quickstart template will allow you to create a new Wasm Wrapper that can query the GitHub Events API. To get started:

1. Go to [github.com/supabase-community/postgres-wasm-fdw](https://github.com/supabase-community/postgres-wasm-fdw)
2. Fork the repo to your own GitHub account by by clicking the `Use this template` button on top right of this page
3. Clone the repo to your local machine.

## Rename and create a new version

Open `Cargo.toml` and update the following lines to any name and version you want:

```toml
[package]
name = "wasm_fdw_example" # The name of your wrapper. Must be Postgres compatible.
version = "0.1.0" # The version number.

[package.metadata.component]
package = "my-company:example-fdw" # A namespaced identifier
```

In our example, we're going to update it to these credentials:

```toml
[package]
name = "github_fdw_quickstart" # The name of your wrapper. Must be Postgres compatible.
version = "0.2.0" # The version number.

[package.metadata.component]
package = "my-company:github-fdw-quickstart" # A namespaced identifier
```

Open `wit/world.wit` and update the package name and version:

```
package my-company:github-fdw-quickstart@0.2.0;
```

Save and commit your code:

```
git add .
git commit -m 'rename package'
git push
```

## Release the Wasm FDW package

To create a release of the Wasm FDW package, create a version tag and then push it.

```bash
git tag v0.2.0
git push origin v0.2.0
```

This will trigger a GitHub workflow to build the package and create a release on your repo. It may take a few minutes:

![GitHub Build](/wrappers/assets/wasm-build.png)

After the GitHub action has completed, expand the `Create README.txt` step, which will have your installation instructions at the bottom - something like:

```
env:
    PROJECT: github_fdw_quickstart
    PACKAGE: my-company:github-fdw-quickstart
    VERSION: 0.2.0
    CHECKSUM: 338674c4c983aa6dbc2b6e63659076fe86d847ca0da6d57a61372b44e0fe4ac9
```

## Install your FDW

You can install the FDW on any Postgres [platform that supports](/wrappers/#supported-platforms) Wrappers `>=0.4.1`.

<details>

<summary>Check compatibility</summary>

```sql
select *
from pg_available_extension_versions
where name = 'wrappers';
```

</details>

### Enable Wrappers

Enable the Wrappers extension and initialize the Wasm FDW on any platform that [supports Wrappers](/wrappers/#supported-platforms):

```sql
create extension if not exists wrappers with schema extensions;

create foreign data wrapper wasm_wrapper
  handler wasm_fdw_handler
  validator wasm_fdw_validator;
```

### Install your Wrapper from GitHub

Create foreign server and foreign table like below. You will need to update the `options` to match your release above.

```sql
create server github_api
foreign data wrapper wasm_wrapper
options (
  fdw_package_url 'https://github.com/<ORG>/<REPO>/releases/download/v0.2.0/github_fdw_quickstart.wasm',
  fdw_package_name 'my-company:github-fdw-quickstart',
  fdw_package_version '0.2.0',
  fdw_package_checksum '338674c4c983aa6dbc2b6e63659076fe86d847ca0da6d57a61372b44e0fe4ac9',
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
server github_api
options (
  object 'events',
  rowid_column 'id'
);
```

!!! tip

    You can copy the DDL from released `READE.txt` file, which can be downloaded from the release page like: `https://github.com/<ORG>/<REPO>/releases/tag/v0.2.0`

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

👏 👏 Congratulations! You have built your first Wasm FDW.

## Clean up

Run the following code to remove the FDW:

```sql

drop schema github cascade;
drop server github_api;
```
