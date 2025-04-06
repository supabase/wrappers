# Slack WASM Foreign Data Wrapper

This is a WASM-based Foreign Data Wrapper (FDW) for integrating Slack data into PostgreSQL through Supabase Wrappers.

## Implementation Plan

1. **Setup Project Structure**
   - [x] Create basic folder structure
   - [x] Initialize Cargo.toml with required dependencies
   - [x] Setup WIT files for WASM component interface

2. **Define API Interfaces**
   - [x] Identify key Slack API endpoints to implement
   - [x] Define data models for Slack resources (messages, channels, users, etc.)
   - [x] Map API responses to PostgreSQL table structures

3. **Implement Core FDW Components**
   - [x] Implement connection handling with Slack API
   - [x] Setup authentication using Slack OAuth tokens
   - [x] Implement table definitions and schema handling
   - [x] Develop query planning and execution logic

4. **Implement Query Capabilities**
   - [x] Enable basic queries for Slack resources
   - [x] Implement filtering capability to push down WHERE conditions
   - [x] Support pagination for large result sets
   - [x] Add timestamp-based filtering for messages

5. **Error Handling and Validation**
   - [x] Implement proper error handling for API limits and failures
   - [x] Add validation for connection parameters
   - [x] Handle rate limiting gracefully

6. **Testing**
   - [x] Create unit tests for core functionality
   - [x] Setup integration tests with Slack API
   - [x] Test query performance and optimization
   
   > **Note on Testing**: Comprehensive tests have been implemented for model conversions and API operations. Due to Rust version compatibility issues with wit-bindgen, some tests may not run in all environments. The test suite is designed for use in the CI/CD pipeline or with a compatible Rust environment.

7. **Documentation**
   - [x] Document installation process
   - [x] Create usage guide with SQL examples
   - [x] Add configuration options reference

8. **Finalization**
   - [x] Ensure code quality and performance optimization
   - [x] Update project documentation
   - [ ] Submit PR for review

## Usage (Future)

```sql
-- Create server
CREATE SERVER slack_server
FOREIGN DATA WRAPPER wasm_fdw
OPTIONS (
    wasm_library 'slack_fdw',
    api_token 'xoxp-your-slack-token'
);

-- Create user mapping
CREATE USER MAPPING FOR CURRENT_USER
SERVER slack_server;

-- Create foreign tables
CREATE FOREIGN TABLE slack_messages (
    ts TEXT,
    user_id TEXT,
    channel_id TEXT,
    text TEXT,
    thread_ts TEXT,
    reply_count INTEGER
)
SERVER slack_server
OPTIONS (
    resource 'messages'
);

-- Query examples
SELECT * FROM slack_messages 
WHERE channel_id = 'C01234ABCDE' 
LIMIT 10;
```

## Configuration Options

### Server Options

| Option | Description | Default | Required |
|--------|-------------|---------|----------|
| api_token | Slack OAuth token (starts with xoxp- or xoxb-) | - | Yes |
| workspace | Slack workspace name | - | No |
| rate_limit | Maximum requests per minute (1-1000) | 60 | No |
| error_retry_count | Number of retries for failed API calls (0-10) | 3 | No |
| timeout_seconds | Request timeout in seconds (1-300) | 30 | No |

### Table Options

#### Messages Table

| Option | Description | Default | Required |
|--------|-------------|---------|----------|
| channel_id | Channel ID for messages | - | No |
| include_threads | Include threaded replies | false | No |
| default_channel | Default channel to use when none specified | - | No |
| max_results_per_channel | Maximum messages per channel (1-1000) | 100 | No |

#### Users Table

| Option | Description | Default | Required |
|--------|-------------|---------|----------|
| include_bots | Include bot users | true | No |
| include_deleted | Include deleted users | false | No |

#### Files Table

| Option | Description | Default | Required |
|--------|-------------|---------|----------|
| types | Comma-separated list of file types to include | all | No |
| max_results | Maximum files to return (1-1000) | 100 | No |

#### Channels Table

| Option | Description | Default | Required |
|--------|-------------|---------|----------|
| types | Comma-separated list of channel types | public_channel | No |
| exclude_archived | Exclude archived channels | true | No |

## Development

This wrapper is built using Rust and the Supabase Wrappers WASM interface.