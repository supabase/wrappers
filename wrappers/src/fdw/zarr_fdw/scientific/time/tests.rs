use super::*;
use serde_json::json;

fn attrs(value: Value) -> Map<String, Value> {
    value.as_object().cloned().unwrap()
}

#[test]
fn parses_supported_units_and_uses_checked_pg_microseconds() {
    for (unit, raw, expected_seconds) in [
        ("seconds", 2.0, 2_i64),
        ("milliseconds", 2_000.0, 2),
        ("microseconds", 2_000_000.0, 2),
        ("nanoseconds", 2_000_000_000.0, 2),
        ("minutes", 2.0, 120),
        ("hours", 2.0, 7_200),
        ("days", 2.0, 172_800),
    ] {
        let spec = TimeSpec::from_cf_attributes(&attrs(json!({
            "units": format!("{unit} since 1970-01-01"),
            "calendar": "proleptic_gregorian"
        })))
        .unwrap();

        assert_eq!(
            spec.raw_to_pg_micros(raw).unwrap(),
            expected_seconds * 1_000_000 - PG_EPOCH_MICROS
        );
    }
}

#[test]
fn accepts_date_datetime_and_rfc3339_origins_as_utc_instants() {
    let origins = [
        "2000-01-01",
        "2000-01-01 00:00:00",
        "2000-01-01T00:00:00",
        "2000-01-01T01:00:00+01:00",
    ];

    for origin in origins {
        let spec = TimeSpec::from_cf_attributes(&attrs(json!({
            "units": format!("seconds since {origin}"),
            "calendar": "proleptic_gregorian"
        })))
        .unwrap();
        assert_eq!(spec.raw_to_pg_micros(0.0).unwrap(), 0);
    }
}

#[test]
fn rejects_missing_malformed_or_unsupported_cf_time_metadata() {
    for value in [
        json!({"calendar": "proleptic_gregorian"}),
        json!({"units": "seconds since 1970-01-01"}),
        json!({
            "units": "months since 1970-01-01",
            "calendar": "proleptic_gregorian"
        }),
        json!({"units": "seconds after 1970-01-01", "calendar": "proleptic_gregorian"}),
        json!({"units": "seconds since 1970-01-01", "calendar": "gregorian"}),
        json!({"units": 1, "calendar": "proleptic_gregorian"}),
        json!({
            "units": "seconds since not-a-date",
            "calendar": "proleptic_gregorian"
        }),
    ] {
        assert!(TimeSpec::from_cf_attributes(&attrs(value)).is_err());
    }
}

#[test]
fn rejects_non_finite_and_overflowing_time_conversions() {
    let spec = TimeSpec::from_cf_attributes(&attrs(json!({
        "units": "days since 1970-01-01",
        "calendar": "proleptic_gregorian"
    })))
    .unwrap();

    assert!(spec.raw_to_pg_micros(f64::NAN).is_err());
    assert!(spec.raw_to_pg_micros(f64::INFINITY).is_err());
    assert!(spec.raw_to_pg_micros(f64::MAX).is_err());
}

#[test]
fn converts_pg_predicate_micros_back_to_raw_with_same_spec() {
    let spec = TimeSpec::from_cf_attributes(&attrs(json!({
        "units": "hours since 2000-01-01 00:00:00",
        "calendar": "proleptic_gregorian"
    })))
    .unwrap();

    assert_eq!(spec.raw_to_pg_micros(6.0).unwrap(), 21_600_000_000);
    let (lo, hi) = spec.pg_micros_to_raw_bounds(21_600_000_000).unwrap();
    assert!(lo < 6.0 && hi > 6.0);
}

#[test]
fn inverse_bounds_cover_nanoseconds_that_round_to_the_same_pg_microsecond() {
    let spec = TimeSpec::from_cf_attributes(&attrs(json!({
        "units": "nanoseconds since 1970-01-01",
        "calendar": "proleptic_gregorian"
    })))
    .unwrap();

    let pg_micros = 1 - PG_EPOCH_MICROS;
    assert_eq!(spec.raw_to_pg_micros(501.0).unwrap(), pg_micros);
    assert_eq!(spec.raw_to_pg_micros(1_499.0).unwrap(), pg_micros);
    let (lo, hi) = spec.pg_micros_to_raw_bounds(pg_micros).unwrap();
    assert!(lo <= 501.0);
    assert!(hi >= 1_499.0);
}
