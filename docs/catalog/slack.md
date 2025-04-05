---
source: https://api.slack.com/
documentation: https://api.slack.com/docs
author: supabase
tags:
  - wasm
  - communication
---

# Slack

[Slack](https://slack.com/) is a messaging app for business that connects people to the information they need. By bringing people together to work as one unified team, Slack transforms the way organizations communicate.

The Slack Wrapper is a WebAssembly (Wasm) foreign data wrapper which allows you to query Slack workspaces, channels, messages, and users directly from your Postgres database.

## Available Versions

| Version | Wasm Package URL                                                                              | Checksum            |
| ------- | --------------------------------------------------------------------------------------------- | ------------------- |
| 0.1.0   | `https://github.com/supabase/wrappers/releases/download/wasm_slack_fdw_v0.1.0/slack_fdw.wasm` | _Not yet available_ |

## Preparation

Before you can query Slack, you need to enable the Wrappers extension and store your credentials in Postgres.

### Enable Wrappers

Make sure the `wrappers` extension is installed on your database:

```sql
create extension if not exists wrappers with schema extensions;
```

### Enable the Slack Wrapper

Enable the Wasm foreign data wrapper:

```sql
create foreign data wrapper wasm_wrapper
  handler wasm_fdw_handler
  validator wasm_fdw_validator;
```

### Create a Slack API Token

1. Visit the [Slack API Apps page](https://api.slack.com/apps)
2. Click "Create New App" and select "From scratch"
3. Name your app and select the workspace to install it
4. Navigate to "OAuth & Permissions" in the sidebar
5. Under "Scopes", add the following Bot Token Scopes:
   - `channels:history` - Read messages in public channels
   - `channels:read` - View basic channel information
   - `users:read` - View users in workspace
   - `users:read.email` - View email addresses
   - `files:read` - View files shared in channels
   - `reactions:read` - View emoji reactions
6. Install the app to your workspace
7. Copy the "Bot User OAuth Token" that starts with `xoxb-`

### Store credentials (optional)

By default, Postgres stores FDW credentials inside `pg_catalog.pg_foreign_server` in plain text. Anyone with access to this table will be able to view these credentials. Wrappers is designed to work with [Vault](https://supabase.com/docs/guides/database/vault), which provides an additional level of security for storing credentials. We recommend using Vault to store your credentials.

```sql
-- Save your Slack API token in Vault and retrieve the `key_id`
insert into vault.secrets (name, secret)
values (
  'slack',
  'xoxb-your-slack-token' -- Slack Bot User OAuth Token
)
returning key_id;
```

### Connecting to Slack

We need to provide Postgres with the credentials to access Slack and any additional options. We can do this using the `create server` command:

=== "With Vault"

    ```sql
    create server slack_server
      foreign data wrapper wasm_wrapper
      options (
        fdw_package_url 'https://github.com/supabase/wrappers/releases/download/wasm_slack_fdw_v0.1.0/slack_fdw.wasm',
        fdw_package_name 'supabase:slack-fdw',
        fdw_package_version '0.1.0',
        fdw_package_checksum '_not_yet_available_',
        api_token_id '<key_ID>', -- The Key ID from Vault
        workspace 'your-workspace' -- Optional workspace name
      );
    ```

=== "Without Vault"

    ```sql
    create server slack_server
      foreign data wrapper wasm_wrapper
      options (
        fdw_package_url 'https://github.com/supabase/wrappers/releases/download/wasm_slack_fdw_v0.1.0/slack_fdw.wasm',
        fdw_package_name 'supabase:slack-fdw',
        fdw_package_version '0.1.0',
        fdw_package_checksum '_not_yet_available_',
        api_token 'xoxb-your-slack-token',
        workspace 'your-workspace' -- Optional workspace name
      );
    ```

The full list of server options are below:

- `fdw_package_*`: required. Specify the Wasm package metadata. You can get the available package version list from [above](#available-versions).
- `api_token` | `api_token_id` 
    - `api_token`:Slack Bot User OAuth Token, required if not using Vault.
    - `api_token_id`: Vault secret key ID storing the Slack token, required if using Vault.
- `workspace` - Slack workspace name, optional.


### Create a schema

We recommend creating a schema to hold all the foreign tables:

```sql
create schema if not exists slack;
```


## Entities

The Slack Wrapper supports data reads from the Slack API.

### Messages

This represents messages from channels, DMs, and group messages.

Ref: [Slack conversations.history API](https://api.slack.com/methods/conversations.history)

#### Operations

| Object   | Select | Insert | Update | Delete | Truncate |
| -------- | :----: | :----: | :----: | :----: | :------: |
| messages |   ✅    |   ❌    |   ❌    |   ❌    |    ❌     |

#### Usage

```sql
create foreign table slack.messages (
  ts text,
  user_id text,
  channel_id text,
  text text,
  thread_ts text,
  reply_count integer
)
  server slack_server
  options (
    resource 'messages'
  );
```

#### Notes

- The `ts` field is the message timestamp ID and is used as the primary key
- Supports query pushdown for channel filtering
- Requires the `channels:history` scope

### Channels

This represents all channels in the workspace.

Ref: [Slack conversations.list API](https://api.slack.com/methods/conversations.list)

#### Operations

| Object   | Select | Insert | Update | Delete | Truncate |
| -------- | :----: | :----: | :----: | :----: | :------: |
| channels |   ✅    |   ❌    |   ❌    |   ❌    |    ❌     |

#### Usage

```sql
create foreign table slack.channels (
  id text,
  name text,
  is_private boolean,
  created timestamp,
  creator text
)
server slack_server
options (
  resource 'channels'
);
```

#### Notes

- The `id` field is the channel ID and is used as the primary key
- Requires the `channels:read` scope

### Users

This represents all users in the workspace.

Ref: [Slack users.list API](https://api.slack.com/methods/users.list)

#### Operations

| Object | Select | Insert | Update | Delete | Truncate |
| ------ | :----: | :----: | :----: | :----: | :------: |
| users  |   ✅    |   ❌    |   ❌    |   ❌    |    ❌     |

#### Usage

```sql
create foreign table slack.users (
  id text,
  name text,
  real_name text,
  email text,
  is_admin boolean,
  is_bot boolean
)
server slack_server
options (
  resource 'users'
);
```

#### Notes

- The `id` field is the user ID and is used as the primary key
- Requires the `users:read` scope
- Email field requires the `users:read.email` scope

### Files

This represents files shared in the workspace.

Ref: [Slack files.list API](https://api.slack.com/methods/files.list)

#### Operations

| Object | Select | Insert | Update | Delete | Truncate |
| ------ | :----: | :----: | :----: | :----: | :------: |
| files  |   ✅    |   ❌    |   ❌    |   ❌    |    ❌     |

#### Usage

```sql
create foreign table slack.files (
  id text,
  name text,
  title text,
  mimetype text,
  size bigint,
  url_private text,
  user_id text,
  created timestamp
)
server slack_server
options (
  resource 'files'
);
```

#### Notes

- The `id` field is the file ID and is used as the primary key
- Requires the `files:read` scope
- Supports query pushdown for filtering by channel or user

### Team Info

This represents information about the team/workspace.

Ref: [Slack team.info API](https://api.slack.com/methods/team.info)

#### Operations

| Object    | Select | Insert | Update | Delete | Truncate |
| --------- | :----: | :----: | :----: | :----: | :------: |
| team-info |   ✅    |   ❌    |   ❌    |   ❌    |    ❌     |

#### Usage

```sql
create foreign table slack.team_info (
  id text,
  name text,
  domain text,
  email_domain text
)
server slack_server
options (
  resource 'team-info'
);
```

#### Notes

- Returns a single row with team information
- No special scopes required beyond authentication

## Query Pushdown Support

This FDW supports the following condition pushdowns:

| Resource  | Supported Filters                   |
| --------- | ----------------------------------- |
| messages  | channel_id, oldest, latest          |
| users     | *(no filter support)*               |
| channels  | types (public/private)              |
| files     | channel_id, user_id, ts_from, ts_to |
| team-info | *(no filter support)*               |

## Supported Data Types

| Postgres Data Type | Slack JSON Type         |
| ------------------ | ----------------------- |
| text               | string                  |
| boolean            | boolean                 |
| integer            | number                  |
| bigint             | number                  |
| timestamp          | string (Unix timestamp) |

## Limitations

This section describes important limitations and considerations when using this FDW:

- Large result sets may experience slower performance due to pagination requirements
- Rate limits are enforced by the Slack API (Tier 2: 20+ requests/minute)
- Messages older than the workspace retention policy are not accessible
- Private channels require the `groups:history` and `groups:read` scopes
- Direct messages require the `im:history` and `im:read` scopes

## Examples

Below are some examples on how to use Slack foreign tables.

### Basic Example

This example will create a "foreign table" inside your Postgres database and query its data.

```sql
create foreign table slack.channels (
  id text,
  name text,
  is_private boolean,
  created timestamp,
  creator text
)
server slack_server
options (
  resource 'channels'
);

-- Query all channels
select * from slack.channels;
```

### Messages from a Specific Channel

```sql
create foreign table slack.messages (
  ts text,
  user_id text,
  channel_id text,
  text text,
  thread_ts text,
  reply_count integer
)
server slack_server
options (
  resource 'messages'
);

-- Get messages from a specific channel
select * from slack.messages 
where channel_id = 'C01234ABCDE'
limit 100;
```

### User Information

```sql
create foreign table slack.users (
  id text,
  name text,
  real_name text,
  email text,
  is_admin boolean,
  is_bot boolean
)
server slack_server
options (
  resource 'users'
);

-- List all admin users
select * from slack.users where is_admin = true;
```

### Join Tables Together

```sql
-- Find all messages from a specific user
select m.*, u.real_name 
from slack.messages m
join slack.users u on m.user_id = u.id
where u.name = 'johndoe'
limit 50;
```