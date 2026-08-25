use super::*;
use serde_json::json;

fn attributes(value: Value) -> Map<String, Value> {
    value.as_object().cloned().unwrap()
}

#[test]
fn masks_raw_values_before_applying_scale_and_offset() {
    let decoder = ScientificValueDecoder::from_attributes(
        DType::I16,
        &attributes(json!({
            "_FillValue": -32768,
            "missing_value": [-9999, -8888],
            "valid_range": [0, 500],
            "scale_factor": 0.1,
            "add_offset": 273.15
        })),
    )
    .unwrap();

    assert_eq!(decoder.decode(&(-32768_i16).to_le_bytes()).unwrap(), None);
    assert_eq!(decoder.decode(&(-9999_i16).to_le_bytes()).unwrap(), None);
    assert_eq!(decoder.decode(&(501_i16).to_le_bytes()).unwrap(), None);
    let decoded = decoder.decode(&(500_i16).to_le_bytes()).unwrap().unwrap();
    assert!((decoded - 323.15).abs() < 1e-10);
}

#[test]
fn accepts_scalar_missing_and_separate_valid_bounds() {
    let decoder = ScientificValueDecoder::from_attributes(
        DType::F32,
        &attributes(json!({
            "missing_value": -9999.0,
            "valid_min": 0.0,
            "valid_max": 10.0
        })),
    )
    .unwrap();

    assert_eq!(decoder.decode(&(-9999.0_f32).to_le_bytes()).unwrap(), None);
    assert_eq!(decoder.decode(&(-1.0_f32).to_le_bytes()).unwrap(), None);
    assert_eq!(
        decoder.decode(&(10.0_f32).to_le_bytes()).unwrap(),
        Some(10.0)
    );
}

#[test]
fn rejects_conflicting_or_malformed_attributes() {
    for attrs in [
        json!({"valid_range": [0, 1], "valid_min": 0}),
        json!({"valid_range": [1, 0]}),
        json!({"valid_range": [0]}),
        json!({"missing_value": []}),
        json!({"scale_factor": "0.1"}),
        json!({"_FillValue": null}),
    ] {
        assert!(ScientificValueDecoder::from_attributes(DType::I16, &attributes(attrs)).is_err());
    }
}

#[test]
fn identity_semantics_still_promote_to_f64() {
    let decoder = ScientificValueDecoder::from_attributes(DType::I32, &Map::new()).unwrap();
    assert_eq!(decoder.decode(&(42_i32).to_le_bytes()).unwrap(), Some(42.0));
}

#[test]
fn masks_declared_non_finite_sentinels_without_requiring_one_nan_payload() {
    let decoder = ScientificValueDecoder::from_attributes(
        DType::F32,
        &attributes(json!({
            "_FillValue": "NaN",
            "missing_value": ["Infinity", "-Infinity"]
        })),
    )
    .unwrap();

    let alternate_nan = f32::from_bits(0x7fc0_0001);
    assert_eq!(decoder.decode(&alternate_nan.to_le_bytes()).unwrap(), None);
    assert_eq!(decoder.decode(&f32::INFINITY.to_le_bytes()).unwrap(), None);
    assert_eq!(
        decoder.decode(&f32::NEG_INFINITY.to_le_bytes()).unwrap(),
        None
    );
}

#[test]
fn preserves_undeclared_non_finite_float_values() {
    let decoder = ScientificValueDecoder::from_attributes(DType::F64, &Map::new()).unwrap();
    assert!(
        decoder
            .decode(&f64::NAN.to_le_bytes())
            .unwrap()
            .unwrap()
            .is_nan()
    );
    assert_eq!(
        decoder.decode(&f64::INFINITY.to_le_bytes()).unwrap(),
        Some(f64::INFINITY)
    );
}
