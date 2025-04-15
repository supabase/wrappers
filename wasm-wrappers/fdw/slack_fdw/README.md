# Slack WASM Foreign Data Wrapper

This is a WASM-based Foreign Data Wrapper (FDW) for integrating Slack data into PostgreSQL through Supabase Wrappers.

## Overview

The Slack FDW allows querying Slack workspace data directly from PostgreSQL using SQL. It supports all major Slack entities:

- **Users**: Workspace members with detailed profile information
- **User Groups**: User groups (aka user groups) with metadata
- **User Group Members**: Relationships between users and groups
- **Messages**: Channel messages with thread support
- **Channels**: Public and private channels in the workspace
- **Files**: Files shared in workspace channels
- **Team Info**: Details about the Slack workspace

## Architecture

This FDW is implemented in Rust and compiled to WebAssembly (WASM) to run within PostgreSQL using Supabase Wrappers. It follows a layered architecture:

```
+------------------------------------------+
| PostgreSQL        (SQL queries)          |
+------------------------------------------+
| Supabase Wrappers (WASM host interface)  |
+------------------------------------------+
| Slack FDW         (Resource handling)    |
|   |-- API Layer   (Slack API calls)      |
|   |-- Data Models (Response structures)  |
|   |-- Query Push  (Filter optimization)  |
+------------------------------------------+
| Slack API         (HTTP endpoints)       |
+------------------------------------------+
```

## Project Structure

- **src/lib.rs**: Core FDW implementation with control flow
- **src/models.rs**: Data structures for Slack API responses
- **src/api.rs**: Test utilities for Slack API interactions
- **wit/world.wit**: WebAssembly interface definitions
- **Cargo.toml**: Rust dependencies and build configuration

## Implementation Details

### Entity Models

All entity models are defined in `models.rs`:

1. **Users**:
   - `User` struct with profile, status, role properties
   - Mapping to PostgreSQL via `user_to_row()`
   - Support for filtering, sorting, and pagination

2. **User Groups**:
   - `UserGroup` struct with metadata and user lists
   - Mapping via `usergroup_to_row()`
   - Supports filtering by team_id and sorting

3. **User Group Members**:
   - `UserGroupMembership` struct for group-user relationships
   - Many-to-many relationship handling
   - Pagination support

4. **Messages**:
   - `Message` struct with threading and reaction support
   - Channel-based filtering (`channel_id` required)
   - Pagination with cursors for large result sets

5. **Channels**:
   - `Channel` struct with metadata
   - Support for filtering by type (public/private)
   - Pagination support

6. **Files**:
   - `File` struct with metadata and permissions
   - Filtering by user, channel, and date ranges
   - Pagination support

7. **Team Info**:
   - `TeamInfo` struct with workspace details
   - Single-row resource with domain and icon metadata

### Data Flow

1. **Request Handling**:
   - SQL queries map to resource types via table options
   - WHERE clauses are analyzed for query pushdown
   - PostgreSQL types are mapped to Slack API parameters

2. **API Communication**:
   - Authenticated requests to Slack API endpoints
   - Rate limiting and error handling
   - Response parsing into typed Rust structures

3. **Result Processing**:
   - Pagination handling for large result sets
   - Structure conversion to PostgreSQL rows
   - Null handling for optional fields

### Query Optimization

The FDW implements query pushdown for:

| Resource          | Supported Filters                   | Sorting                                | Limit/Offset |
|-------------------|-------------------------------------|----------------------------------------|--------------|
| messages          | channel_id, oldest, latest          | No                                     | Yes*         |
| users             | name, email, team_id                | name, real_name, email                 | Yes          |
| usergroups        | team_id, include_disabled           | name, handle, date_create, date_update | Yes          |
| usergroup_members | *(no filter support)*               | No                                     | Yes          |
| channels          | types (public/private)              | No                                     | Yes*         |
| files             | channel_id, user_id, ts_from, ts_to | No                                     | No           |
| team-info         | *(no filter support)*               | No                                     | No           |

\* Pagination is supported through cursor-based pagination from the Slack API

## Usage

### Server Setup

```sql
CREATE SERVER slack_server
FOREIGN DATA WRAPPER wasm_wrapper
OPTIONS (
    fdw_package_url 'https://github.com/supabase/wrappers/releases/download/wasm_slack_fdw_v0.0.6/slack_fdw.wasm',
    fdw_package_name 'supabase:slack-fdw',
    fdw_package_version '0.0.6',
    fdw_package_checksum '(checksum will be generated on release)',
    api_token 'xoxb-your-slack-token',
    workspace 'your-workspace-name'
);
```

### Table Definitions

```sql
-- Users table
CREATE FOREIGN TABLE slack.users (
  id text,
  name text,
  real_name text,
  display_name text,
  email text,
  is_admin boolean,
  is_bot boolean,
  status_text text,
  team_id text
)
SERVER slack_server
OPTIONS (
  resource 'users'
);

-- Messages table
CREATE FOREIGN TABLE slack.messages (
  ts text,
  user_id text,
  channel_id text,
  text text,
  thread_ts text,
  reply_count integer
)
SERVER slack_server
OPTIONS (
  resource 'messages'
);

-- Channels table
CREATE FOREIGN TABLE slack.channels (
  id text,
  name text,
  is_private boolean,
  created bigint,
  creator text
)
SERVER slack_server
OPTIONS (
  resource 'channels'
);

-- Files table
CREATE FOREIGN TABLE slack.files (
  id text,
  name text,
  title text,
  mimetype text,
  size bigint,
  url_private text,
  user_id text,
  created bigint
)
SERVER slack_server
OPTIONS (
  resource 'files'
);

-- Team info table
CREATE FOREIGN TABLE slack.team_info (
  id text,
  name text,
  domain text,
  email_domain text
)
SERVER slack_server
OPTIONS (
  resource 'team-info'
);

-- User groups table
CREATE FOREIGN TABLE slack.usergroups (
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
SERVER slack_server
OPTIONS (
  resource 'usergroups'
);

-- User group members table
CREATE FOREIGN TABLE slack.usergroup_members (
  usergroup_id text,
  usergroup_name text,
  usergroup_handle text,
  user_id text
)
SERVER slack_server
OPTIONS (
  resource 'usergroup_members'
);
```

### Query Examples

```sql
-- Get all users
SELECT * FROM slack.users;

-- Get messages from a specific channel
SELECT * FROM slack.messages WHERE channel_id = 'C01234ABCDE';

-- Get all channels
SELECT * FROM slack.channels;

-- Get files uploaded by a specific user
SELECT * FROM slack.files WHERE user_id = 'U01234ABCDE';

-- Get team information
SELECT * FROM slack.team_info;

-- Get all user groups
SELECT * FROM slack.usergroups;

-- Get members of a specific user group
SELECT * FROM slack.usergroup_members WHERE usergroup_id = 'S01234ABCDE';
```

## Configuration Options

### Server Options

| Option | Description | Required |
|--------|-------------|----------|
| api_token | Slack OAuth token (starts with xoxp- or xoxb-) | Yes |
| api_token_id | Vault key ID for Slack token (alternative to api_token) | Yes (if api_token not provided) |
| workspace | Slack workspace name for logging | No |

### Table Options

| Option | Description | Required |
|--------|-------------|----------|
| resource | Resource type to query (users, messages, channels, etc.) | Yes |

## Slack API Rate Limits

This FDW implements rate limiting handling according to Slack's guidelines:
- Automatic retry with backoff for rate-limited requests (429 responses)
- Respects Retry-After headers when provided
- Batch size limits to avoid excessive API usage

## Required Slack API Scopes

Each entity type requires specific OAuth scopes:

| Entity Type       | Required Scopes                     | Error If Missing                                  |
|-------------------|------------------------------------|-------------------------------------------------|
| users             | `users:read`                       | "The token used is not granted the required scopes" |
| users (emails)    | `users:read.email`                 | "missing_scope" on email fields                  |
| usergroups        | `usergroups:read`                  | "missing_scope" when querying user groups        |
| usergroup_members | `usergroups:read`                  | "missing_scope" when querying user group members |
| channels          | `channels:read`                    | "missing_scope" when querying channels           |
| messages          | `channels:history`, `channels:read` | "missing_scope" when querying messages           |
| files             | `files:read`                       | "missing_scope" when querying files              |
| team-info         | `team:read`                        | "missing_scope" when querying team info          |
| reactions         | `reactions:read`                   | Missing reactions data in responses              |

For production use, we recommend installing your app with all of these scopes from the beginning to avoid permission issues later.

## Development

### Building

```bash
cargo build --target wasm32-wasi
```

### Running Tests

```bash
cargo test
```

### Adding New Entities

1. Add model structs in `models.rs`
2. Implement row conversion method in `lib.rs` (`entity_to_row`)
3. Implement fetch method in `lib.rs` (`fetch_entity`)
4. Update resource handling in `begin_scan`, `iter_scan`, `re_scan`, and `end_scan`

## Maintenance Tasks

If extending or modifying this FDW, consider:

1. **API Changes**: Slack API evolves - check latest endpoints and fields
2. **Query Pushdown**: Add new filter conditions when possible
3. **Error Handling**: Rate limiting and API errors require careful handling
4. **Response Mapping**: Ensure consistent handling of optional fields
5. **Pagination**: All list operations should support pagination

## Status

All core features are implemented. Future enhancements could include:
- Support for more advanced filters
- Additional Slack entities (emoji, stars, etc.)
- Cache support for performance optimization
- Statistics for query optimization