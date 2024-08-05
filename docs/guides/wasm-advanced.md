# Developing a Wasm Wrapper (Advanced)

If you followed the Quick Start, you should have:

1. Built a GitHub FDW
2. Installed it on Postgres
3. Used it to query GitHub events

This guide will show you how to develop the Wasm Wrapper locally to iterate faster. We will adapt GitHub wrapper and instead we will query a Google Sheet.

## Install pre-requisites

- If you haven't already completed the quickstart, do that first.
- For local development:
  - install the [Supabase CLI](https://supabase.com/docs/guides/cli/getting-started) (version `>= 1.187.10` is needed).
  - install Docker

## Create a Google Sheet

In this example, we're going to create a Wrapper that can query a Google Sheet.

1. Open [this](https://docs.google.com/spreadsheets/d/1OWi0x39w9FhVFP0EmSRRWWKkzhVXpYeTZJLmvaSKy-o/edit?gid=0#gid=0) Google Sheet.
2. Go to `File` > `Make a copy`
3. In your new Sheet, click `Share` (top right), and then change General Access to `Anyone with the link`.

You should have your own Google Sheet:

![Google Sheet](/wrappers/assets/google-sheet.png)

## Rename your Wrapper

Open `Cargo.toml` and update the Wrapper details:

```toml
[package]
name = "sheets_fdw" # The name of your wrapper. Must be Postgres compatible.
version = "0.3.0" # The version number.

[package.metadata.component]
package = "my-company:sheets-fdw" # A namespaced identifier
```

## Modify the source code

!!! tip

    You can refer to the Paddle [source code](https://github.com/supabase/wrappers/blob/main/wasm-wrappers/fdw/paddle_fdw/src/lib.rs) to help with your development.

We need to query the Google Sheet using the JSON API:

```bash
curl -L \
  'https://docs.google.com/spreadsheets/d/1OWi0x39w9FhVFP0EmSRRWWKkzhVXpYeTZJLmvaSKy-o/gviz/tq?tqx=out:json'
```

To do this, we need our Wrapper to accept a `sheet_id` instead of a GitHub `api_url`.

Replace the `init()` function with the following code:

```rs title="lib.rs"
fn init(ctx: &Context) -> FdwResult {
    Self::init_instance();
    let this = Self::this_mut();

    let opts = ctx.get_options(OptionsType::Server);
    this.base_url = opts.require_or("api_url", "https://docs.google.com/spreadsheets/d");
    let sheet_id = match opts.get("sheet_id") {
        Some(key) => key,
        None => opts.require("sheet_id")?,
    };

    // Construct the full URL
    this.base_url = format!(
        "{}/{}/gviz/tq?tqx=out:json",
        this.base_url, sheet_id
    );

    Ok(())
}
```

## Developing locally

We'll use the CLI to develop locally. This will be faster than the GitHub release workflow.

### Start Supabase

After the CLI is installed, start the Supabase services. If this is your first time it might take some time to download all the services.

```bash
supabase init # you only need to run this once
supabase start
```

### Build your wrapper

And then run the script to build the Wasm FDW package and copy it to Supabase database container:

```bash
./local-dev.sh
```

!!! tip

    You can also use it with [cargo watch](https://crates.io/crates/cargo-watch): `cargo watch -s ./local-dev.sh`

### Installing the wrapper

Visit SQL Editor in your [local browser](http://127.0.0.1:54323/project/default/sql/1), create foreign server and foreign table like below:

```sql
create extension if not exists wrappers with schema extensions;

create foreign data wrapper wasm_wrapper
  handler wasm_fdw_handler
  validator wasm_fdw_validator;

create server example_server
  foreign data wrapper wasm_wrapper
  options (
    -- use 'file://' schema to reference the local wasm file in container
    fdw_package_url 'file:///sheets_fdw.wasm',
    fdw_package_name 'my-company:sheets-fdw',
    fdw_package_version '0.2.0',
    api_url 'https://api.github.com'
  );

create schema google;

create foreign table google.sheets (
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

!!! NOTE

    The foreign server option `fdw_package_checksum` is not needed for local development.

Now you can query the foreign table like below to see result:

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

## Wrap up

When you're ready, you can follow the [Release process](/wrappers/guides/create-wasm-wrapper/#release-the-wasm-fdw-package) in the quickstart guide to release a new version of your wrapper.
