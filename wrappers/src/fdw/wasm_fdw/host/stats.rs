use crate::stats as host_stats;
use pgrx::JsonB;

use super::super::bindings::supabase::wrappers::stats;
use super::FdwHost;

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
