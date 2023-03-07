# AWS S3 Foreign Data Wrapper

This is a foreign data wrapper for [AWS S3](https://aws.amazon.com/s3/). It is developed using [Wrappers](https://github.com/supabase/wrappers) and only supports data scan at this moment.

## Basic usage

These steps outline how to use the AWS S3 FDW:

1. Clone this repo

```bash
git clone https://github.com/supabase/wrappers.git
```

2. Run it using pgx with feature:

```bash
cd wrappers/wrappers
cargo pgx run --features s3_fdw
```

3. Create the extension, foreign data wrapper and related objects:

```sql
-- create extension
create extension wrappers;

-- create foreign data wrapper and enable 'S3Fdw'
create foreign data wrapper s3_wrapper
  handler s3_fdw_handler
  validator s3_fdw_validator;

-- create a wrappers S3 server and specify connection info
-- Here we're using the service account key stored in Vault, if you don't want
-- to use Vault, you can directly specify the service account key using `sa_key`
-- option. For example,
--
create server s3_server
  foreign data wrapper s3_wrapper
  options (
    aws_access_key_id 'your_aws_access_key_id',
    aws_secret_access_key 'your_aws_secret_access_key',
    aws_region 'ap-southeast-2',
    is_mock 'false'
  );

create foreign table biostats (
  name text,
  sex text,
  age text,
  height text,
  weight text
)
  server s3_server
  options (
    uri 's3://bo-test/biostats.csv',
    format 'csv',
    has_header 'true'
  );
```
