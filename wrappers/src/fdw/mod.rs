#[cfg(feature = "helloworld_fdw")]
mod helloworld_fdw;

#[cfg(feature = "bigquery_fdw")]
mod bigquery_fdw;

#[cfg(feature = "clickhouse_fdw")]
mod clickhouse_fdw;

#[cfg(feature = "stripe_fdw")]
mod stripe_fdw;

#[cfg(feature = "firebase_fdw")]
mod firebase_fdw;

#[cfg(feature = "airtable_fdw")]
mod airtable_fdw;

#[cfg(feature = "s3_fdw")]
mod s3_fdw;

#[cfg(feature = "logflare_fdw")]
mod logflare_fdw;

#[cfg(feature = "auth0_fdw")]
mod auth0_fdw;

#[cfg(feature = "mssql_fdw")]
mod mssql_fdw;

#[cfg(feature = "redis_fdw")]
mod redis_fdw;

#[cfg(feature = "cognito_fdw")]
mod cognito_fdw;

#[cfg(feature = "notion_fdw")]
mod notion_fdw;
