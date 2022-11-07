use cfg_if::cfg_if;

cfg_if! {
    if #[cfg(feature = "helloworld_fdw")] {
        mod helloworld_fdw;
        pub(crate) use helloworld_fdw::HelloWorldFdw;
    }
}

cfg_if! {
    if #[cfg(feature = "bigquery_fdw")] {
        mod bigquery_fdw;
        pub(crate) use bigquery_fdw::BigQueryFdw;
    }
}

cfg_if! {
    if #[cfg(feature = "clickhouse_fdw")] {
        mod clickhouse_fdw;
        pub(crate) use clickhouse_fdw::ClickHouseFdw;
    }
}

cfg_if! {
    if #[cfg(feature = "stripe_fdw")] {
        mod stripe_fdw;
        pub(crate) use stripe_fdw::StripeFdw;
    }
}

cfg_if! {
    if #[cfg(feature = "firebase_fdw")] {
        mod firebase_fdw;
        pub(crate) use firebase_fdw::FirebaseFdw;
    }
}
