use chrono::DateTime;
use std::time::{SystemTime, UNIX_EPOCH};

use super::FdwHost;

const _: () = {
    use super::super::bindings::v1::supabase::wrappers::time;

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

        fn epoch_ms_to_rfc3339(&mut self, msecs: i64) -> Result<String, time::TimeError> {
            DateTime::from_timestamp_micros(msecs)
                .map(|ts| ts.to_rfc3339())
                .ok_or("invalid microseconds since Unix Epoch".to_string())
        }

        fn sleep(&mut self, millis: u64) {
            std::thread::sleep(std::time::Duration::from_millis(millis));
        }
    }
};

const _: () = {
    use super::super::bindings::v1::supabase::wrappers::time as time_v1;
    use super::super::bindings::v2::supabase::wrappers::time;

    impl time::Host for FdwHost {
        fn epoch_secs(&mut self) -> i64 {
            time_v1::Host::epoch_secs(self)
        }

        fn parse_from_rfc3339(&mut self, s: String) -> time::TimeResult {
            time_v1::Host::parse_from_rfc3339(self, s)
        }

        fn parse_from_str(&mut self, s: String, fmt: String) -> time::TimeResult {
            time_v1::Host::parse_from_str(self, s, fmt)
        }

        fn epoch_ms_to_rfc3339(&mut self, msecs: i64) -> Result<String, time::TimeError> {
            time_v1::Host::epoch_ms_to_rfc3339(self, msecs)
        }

        fn sleep(&mut self, millis: u64) {
            time_v1::Host::sleep(self, millis)
        }
    }
};
