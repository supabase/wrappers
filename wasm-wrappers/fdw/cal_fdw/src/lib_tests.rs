use super::{MAX_RETRY_DELAY_SECS, retry_delay_ms};

// A representative Cal.com `x-ratelimit-reset` value: a UTC epoch second.
const RESET_EPOCH: i64 = 1_800_000_000;

// --- epoch values are made relative to now ---

#[test]
fn test_reset_in_the_future_waits_the_remaining_window() {
    let delay = retry_delay_ms(&RESET_EPOCH.to_string(), RESET_EPOCH - 30).unwrap();
    assert_eq!(delay, 30_000);
}

#[test]
fn test_reset_one_second_away_waits_one_second() {
    let delay = retry_delay_ms(&RESET_EPOCH.to_string(), RESET_EPOCH - 1).unwrap();
    assert_eq!(delay, 1_000);
}

// Reading the epoch second as a delay is what this fixes: it used to ask for a
// sleep of roughly 57,000 years, which hangs the scan.
#[test]
fn test_epoch_value_is_not_used_as_the_delay_directly() {
    let delay = retry_delay_ms(&RESET_EPOCH.to_string(), RESET_EPOCH - 30).unwrap();
    assert!(
        delay < RESET_EPOCH as u64,
        "epoch second {RESET_EPOCH} was used as a delay, giving {delay}ms"
    );
}

// --- the wait is bounded ---

#[test]
fn test_reset_far_in_the_future_is_capped() {
    let delay = retry_delay_ms(&RESET_EPOCH.to_string(), 0).unwrap();
    assert_eq!(delay, MAX_RETRY_DELAY_SECS as u64 * 1000);
}

#[test]
fn test_reset_already_passed_still_backs_off() {
    let delay = retry_delay_ms(&RESET_EPOCH.to_string(), RESET_EPOCH + 300).unwrap();
    assert_eq!(delay, 1_000);
}

#[test]
fn test_reset_equal_to_now_still_backs_off() {
    let delay = retry_delay_ms(&RESET_EPOCH.to_string(), RESET_EPOCH).unwrap();
    assert_eq!(delay, 1_000);
}

// --- malformed input is rejected, not turned into a huge sleep ---

#[test]
fn test_surrounding_whitespace_is_tolerated() {
    let delay = retry_delay_ms("  1800000000  ", RESET_EPOCH - 10).unwrap();
    assert_eq!(delay, 10_000);
}

#[test]
fn test_non_numeric_header_is_an_error() {
    assert!(retry_delay_ms("later", RESET_EPOCH).is_err());
}

#[test]
fn test_empty_header_is_an_error() {
    assert!(retry_delay_ms("", RESET_EPOCH).is_err());
}

#[test]
fn test_error_message_names_the_header() {
    let err = retry_delay_ms("later", RESET_EPOCH).unwrap_err();
    assert!(
        err.contains("x-ratelimit-reset"),
        "error should name the header, got {err:?}"
    );
}

// A value far beyond i64 seconds must not wrap into a small or negative delay.
#[test]
fn test_overflowing_header_is_an_error() {
    assert!(retry_delay_ms("99999999999999999999", RESET_EPOCH).is_err());
}

#[test]
fn test_negative_header_still_backs_off() {
    let delay = retry_delay_ms("-1", RESET_EPOCH).unwrap();
    assert_eq!(delay, 1_000);
}
