use supabase_wrappers::wrappers_magic;

mod fdw;

#[cfg(feature = "helloworld_fdw")]
use fdw::HelloWorldFdw;

#[cfg(feature = "bigquery_fdw")]
use fdw::BigQueryFdw;

#[cfg(feature = "clickhouse_fdw")]
use fdw::ClickHouseFdw;

#[cfg(feature = "stripe_fdw")]
use fdw::StripeFdw;

#[cfg(feature = "firebase_fdw")]
use fdw::FirebaseFdw;

#[cfg(feature = "airtable_fdw")]
use fdw::AirtableFdw;

// define FDWs
wrappers_magic!(
    #[cfg(feature = "helloworld_fdw")]
    HelloWorldFdw,
    #[cfg(feature = "bigquery_fdw")]
    BigQueryFdw,
    #[cfg(feature = "clickhouse_fdw")]
    ClickHouseFdw,
    #[cfg(feature = "stripe_fdw")]
    StripeFdw,
    #[cfg(feature = "firebase_fdw")]
    FirebaseFdw,
    #[cfg(feature = "airtable_fdw")]
    AirtableFdw,
);
