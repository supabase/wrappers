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

| Version | Wasm Package URL                                                                              | Checksum                                                           |
| ------- | --------------------------------------------------------------------------------------------- | ------------------------------------------------------------------ |
| 0.0.6   | `https://github.com/supabase/wrappers/releases/download/wasm_slack_fdw_v0.0.6/slack_fdw.wasm` | `349cb556f87a0233e25eb608a77e840531bc87f1acf9916856268bdcdd9973e2` |
| 0.1.0   | `https://github.com/supabase/wrappers/releases/download/wasm_slack_fdw_v0.0.6/slack_fdw.wasm` | `(Coming soon)`                                                    |

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
   - `team:read` - View information about the workspace
   - `usergroups:read` - View user groups and their members
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
        fdw_package_url 'https://github.com/supabase/wrappers/releases/download/wasm_slack_fdw_v0.0.6/slack_fdw.wasm',
        fdw_package_name 'supabase:slack-fdw',
        fdw_package_version '0.0.6',
        fdw_package_checksum '(checksum will be generated on release)',
        api_token_id '<key_ID>', -- The Key ID from Vault
        workspace 'your-workspace' -- Optional workspace name
      );
    ```

=== "Without Vault"

    ```sql
    create server slack_server
      foreign data wrapper wasm_wrapper
      options (
        fdw_package_url 'https://github.com/supabase/wrappers/releases/download/wasm_slack_fdw_v0.0.6/slack_fdw.wasm',
        fdw_package_name 'supabase:slack-fdw',
        fdw_package_version '0.0.6',
        fdw_package_checksum '(checksum will be generated on release)',
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
  -- Basic information
  id text,
  name text,
  
  -- Name and profile fields
  real_name text,
  display_name text,
  display_name_normalized text,
  real_name_normalized text,
  
  -- Contact information
  email text,
  phone text,
  skype text,
  
  -- Role information
  is_admin boolean,
  is_owner boolean,
  is_primary_owner boolean,
  is_bot boolean,
  is_app_user boolean,
  is_restricted boolean,
  is_ultra_restricted boolean,
  deleted boolean,
  
  -- Status information
  status_text text,
  status_emoji text,
  status_expiration bigint,
  title text,
  
  -- Team information
  team_id text,
  team text,
  
  -- Time zone information
  tz text,
  tz_label text,
  tz_offset integer,
  locale text,
  
  -- Avatar/image URLs
  image_24 text,
  image_48 text,
  image_72 text,
  image_192 text,
  image_512 text,
  
  -- Miscellaneous
  color text,
  updated bigint
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
- Supports query pushdown for filtering by `name`, `email`, and `team_id` directly via the Slack API
- Supports sorting by `name`, `real_name`, and `email`
- Supports LIMIT and OFFSET clauses for pagination

### User Groups

This represents user groups (a.k.a. user groups) in the workspace.

Ref: [Slack usergroups.list API](https://api.slack.com/methods/usergroups.list)

#### Operations

| Object     | Select | Insert | Update | Delete | Truncate |
| ---------- | :----: | :----: | :----: | :----: | :------: |
| usergroups |   ✅    |   ❌    |   ❌    |   ❌    |    ❌     |

#### Usage

```sql
create foreign table slack.usergroups (
  id text,
  team_id text,
  name text,
  handle text,
  description text,
  is_external boolean,
  date_create bigint,
  date_update bigint,
  date_delete bigint,
  auto_type text,
  created_by text,
  updated_by text,
  deleted_by text,
  user_count integer,
  channel_count integer
)
server slack_server
options (
  resource 'usergroups'
);

-- Get all user groups
select * from slack.usergroups;

-- Get user groups sorted by name
select * from slack.usergroups order by name;
```

#### Notes

- The `id` field is the user group ID and is used as the primary key
- Requires the `usergroups:read` scope
- Supports query pushdown for filtering by `team_id` and `include_disabled`
- Supports sorting by `name`, `handle`, `date_create`, and `date_update`
- Supports LIMIT and OFFSET clauses for pagination

### User Group Members

This represents the membership relationship between users and user groups.

Ref: [Slack usergroups.users.list API](https://api.slack.com/methods/usergroups.users.list)

#### Operations

| Object            | Select | Insert | Update | Delete | Truncate |
| ----------------- | :----: | :----: | :----: | :----: | :------: |
| usergroup_members |   ✅    |   ❌    |   ❌    |   ❌    |    ❌     |

#### Usage

```sql
create foreign table slack.usergroup_members (
  usergroup_id text,
  usergroup_name text,
  usergroup_handle text,
  user_id text
)
server slack_server
options (
  resource 'usergroup_members'
);

-- Get all user group memberships
select * from slack.usergroup_members;
```

#### Notes

- The `usergroup_id` and `user_id` fields form a composite primary key
- Requires the `usergroups:read` scope
- Supports LIMIT and OFFSET clauses for pagination
- To avoid excessive API calls and potential rate limiting, consider using materialized views instead of direct joins

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

| Resource          | Supported Filters                   | Sorting                                | Limit/Offset |
| ----------------- | ----------------------------------- | -------------------------------------- | ------------ |
| messages          | channel_id, oldest, latest          | No                                     | Yes*         |
| users             | name, email, team_id                | name, real_name, email                 | Yes          |
| usergroups        | team_id, include_disabled           | name, handle, date_create, date_update | Yes          |
| usergroup_members | *(no filter support)*               | No                                     | Yes          |
| channels          | types (public/private)              | No                                     | Yes*         |
| files             | channel_id, user_id, ts_from, ts_to | No                                     | No           |
| team-info         | *(no filter support)*               | No                                     | No           |

\* Pagination is supported through cursor-based pagination from the Slack API

## Supported Data Types

| Postgres Data Type | Slack JSON Type         |
| ------------------ | ----------------------- |
| text               | string                  |
| boolean            | boolean                 |
| integer            | number                  |
| bigint             | number                  |
| timestamp          | string (Unix timestamp) |

## Required Slack API Scopes

Each entity type in the Slack FDW requires specific API scopes. If you get a `missing_scope` error, you may need to add additional scopes and reinstall your app to the workspace.

| Entity Type       | Required Scopes                     | Error If Missing                                    |
| ----------------- | ----------------------------------- | --------------------------------------------------- |
| users             | `users:read`                        | "The token used is not granted the required scopes" |
| users (emails)    | `users:read.email`                  | "missing_scope" on email fields                     |
| usergroups        | `usergroups:read`                   | "missing_scope" when querying user groups           |
| usergroup_members | `usergroups:read`                   | "missing_scope" when querying user group members    |
| channels          | `channels:read`                     | "missing_scope" when querying channels              |
| messages          | `channels:history`, `channels:read` | "missing_scope" when querying messages              |
| files             | `files:read`                        | "missing_scope" when querying files                 |
| team-info         | `team:read`                         | "missing_scope" when querying team info             |

### Adding Scopes to an Existing App

If you need to add scopes to an existing Slack app:

1. Go to [Your Slack Apps](https://api.slack.com/apps)
2. Select your app
3. Click "OAuth & Permissions" in the sidebar
4. Under "Bot Token Scopes", click "Add an OAuth Scope"
5. Add any missing scopes from the table above
6. Scroll up and click "Reinstall to Workspace"
7. Approve the new permissions
8. Copy the new Bot User OAuth Token (it may have changed)
9. Update your token in your database server configuration

## Limitations

This section describes important limitations and considerations when using this FDW:

- Large result sets may experience slower performance due to pagination requirements
- Rate limits are enforced by the Slack API (Tier 2: 20+ requests/minute)
- Messages older than the workspace retention policy are not accessible
- Private channels require the `groups:history` and `groups:read` scopes
- Direct messages require the `im:history` and `im:read` scopes

## Troubleshooting

### Error: "Slack API error: missing_scope"

This error occurs when your Slack token doesn't have all the required OAuth scopes for the resource you're trying to access.

**Solution:**
1. Check the [Required Slack API Scopes](#required-slack-api-scopes) section above to see which scopes are needed for the entity you're querying
2. Follow the steps in the "Adding Scopes to an Existing App" section to add the missing scopes
3. Reinstall your app to the workspace
4. Update your token in the PostgreSQL server configuration

### Error: "channel_id is required for querying messages"

When querying the `messages` table, you must include a `WHERE channel_id = 'CXXXXXXXX'` clause.

**Solution:**
```sql
-- Incorrect (will error):
SELECT * FROM slack.messages LIMIT 10;

-- Correct:
SELECT * FROM slack.messages WHERE channel_id = 'C01234ABCDE' LIMIT 10;
```

### Error: "Rate limit exceeded"

Slack's API enforces rate limits (Tier 2: ~20 requests/minute). 

**Solution:**
- Add more specific WHERE clauses to reduce the number of API calls
- Use smaller LIMIT values
- Consider materialized views for frequent queries:
```sql
CREATE MATERIALIZED VIEW slack_users AS 
SELECT * FROM slack.users;

-- Query the materialized view instead
SELECT * FROM slack_users;

-- Refresh when needed (less frequently)
REFRESH MATERIALIZED VIEW slack_users;
```

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

### User Information

```sql
create foreign table slack.users (
  -- Basic subset of fields
  id text,
  name text,
  real_name text,
  email text,
  is_admin boolean,
  is_bot boolean,
  
  -- Additional useful fields
  display_name text,
  phone text,
  title text,
  status_text text,
  status_emoji text,
  team_id text,
  image_192 text
)
server slack_server
options (
  resource 'users'
);

-- List all admin users
select * from slack.users where is_admin = true;

-- Get users ordered by name
select * from slack.users order by name;

-- Get top 5 non-bot users ordered by real_name 
select * from slack.users where is_bot = false order by real_name limit 5;

-- Filter users by email domain
select * from slack.users where email like '%@example.com' order by name;

-- Find user by name (pushes filter down to API)
select * from slack.users where name = 'johndoe';

-- Show user with their profile picture
select id, name, real_name, image_192 as profile_picture from slack.users 
where name = 'johndoe';
```

### User Groups Example

```sql
-- Create tables
create foreign table slack.usergroups (
  id text,
  name text,
  handle text,
  description text,
  user_count integer
)
server slack_server
options (
  resource 'usergroups'
);

create foreign table slack.usergroup_members (
  usergroup_id text,
  usergroup_name text,
  usergroup_handle text,
  user_id text
)
server slack_server
options (
  resource 'usergroup_members'
);

-- Get all user groups
select name, handle, description, user_count 
from slack.usergroups 
order by name;

-- Get all members of a user group (just IDs)
select * from slack.usergroup_members 
where usergroup_id = 'S01234ABCDE';
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