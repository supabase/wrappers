use crate::stats as host_stats;
use pgrx::JsonB;

use super::FdwHost;

const _: () = {
    use super::super::bindings::v1::supabase::wrappers::stats;

    impl stats::Host for FdwHost {
        fn inc_stats(&mut self, fdw_name: String, metric: stats::Metric, inc: i64) {
            host_stats::inc_stats(&fdw_name, host_stats::Metric::from(metric), inc);
        }

        fn get_metadata(&mut self, fdw_name: String) -> Option<String> {
            host_stats::get_metadata(&fdw_name).map(|m| m.0.to_string())
        }

        fn set_metadata(&mut self, fdw_name: String, metadata: Option<String>) {
            let jsonb = metadata.map(|m| JsonB(serde_json::from_str(&m).unwrap_or_default()));
            host_stats::set_metadata(&fdw_name, jsonb);
        }
    }
};

const _: () = {
    use super::super::bindings::v1::supabase::wrappers::stats as stats_v1;
    use super::super::bindings::v2::supabase::wrappers::stats;

    impl From<stats::Metric> for stats_v1::Metric {
        fn from(m: stats::Metric) -> Self {
            match m {
                stats::Metric::CreateTimes => stats_v1::Metric::CreateTimes,
                stats::Metric::RowsIn => stats_v1::Metric::RowsIn,
                stats::Metric::RowsOut => stats_v1::Metric::RowsOut,
                stats::Metric::BytesIn => stats_v1::Metric::BytesIn,
                stats::Metric::BytesOut => stats_v1::Metric::BytesOut,
            }
        }
    }

    impl stats::Host for FdwHost {
        fn inc_stats(&mut self, fdw_name: String, metric: stats::Metric, inc: i64) {
            stats_v1::Host::inc_stats(self, fdw_name, metric.into(), inc);
        }

        fn get_metadata(&mut self, fdw_name: String) -> Option<String> {
            stats_v1::Host::get_metadata(self, fdw_name)
        }

        fn set_metadata(&mut self, fdw_name: String, metadata: Option<String>) {
            stats_v1::Host::set_metadata(self, fdw_name, metadata);
        }
    }
};
