use std::time::{SystemTime, UNIX_EPOCH};
use chrono::DateTime;

use super::super::bindings::supabase::wrappers::time;
use super::FdwHost;

impl time::Host for FdwHost {
    fn epoch_secs(&mut self) -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("SystemTime before UNIX EPOCH!")
            .as_secs() as i64
    }

    fn parse_from_rfc3339(&mut self, s: String) -> time::TimeResult {
        DateTime::parse_from_rfc3339(&s)
            .map(|ts| ts.timestamp_micros())
            .map_err(|e| e.to_string())
    }

    fn parse_from_str(&mut self, s: String, fmt: String) -> time::TimeResult {
        DateTime::parse_from_str(&s, &fmt)
            .map(|ts| ts.timestamp_micros())
            .map_err(|e| e.to_string())
    }

    fn sleep(&mut self, millis: u64) {
        std::thread::sleep(std::time::Duration::from_millis(millis));
    }
}
