use cfg_if::cfg_if;

cfg_if! {
    if #[cfg(feature = "helloworld_fdw")] {
        mod helloworld_fdw;
    }
}

cfg_if! {
    if #[cfg(feature = "bigquery_fdw")] {
        mod bigquery_fdw;
    }
}

cfg_if! {
    if #[cfg(feature = "clickhouse_fdw")] {
        mod clickhouse_fdw;
    }
}

cfg_if! {
    if #[cfg(feature = "stripe_fdw")] {
        mod stripe_fdw;
    }
}

cfg_if! {
    if #[cfg(feature = "firebase_fdw")] {
        mod firebase_fdw;
    }
}

cfg_if! {
    if #[cfg(feature = "airtable_fdw")] {
        mod airtable_fdw;
    }
}

cfg_if! {
    if #[cfg(feature = "s3_fdw")] {
        mod s3_fdw;
    }
}

cfg_if! {
    if #[cfg(feature = "openai_fdw")] {
        mod openai_fdw;
    }
}
