use super::super::meta::ArrayMeta;
use super::super::{ZarrFdwError, ZarrFdwResult};
use super::model::{CoordinateRef, Dataset, Dimension, DimensionRole};

const AXIS_TIME: &str = "time";
const AXIS_Y: &str = "y";
const AXIS_X: &str = "x";
const RANK_TWO_PROFILE: [(&str, DimensionRole); 2] = [
    (AXIS_Y, DimensionRole::SpatialY),
    (AXIS_X, DimensionRole::SpatialX),
];
const RANK_THREE_PROFILE: [(&str, DimensionRole); 3] = [
    (AXIS_TIME, DimensionRole::Time),
    (AXIS_Y, DimensionRole::SpatialY),
    (AXIS_X, DimensionRole::SpatialX),
];

/// Adapt the current G0 array profile into the generic dataset model.
///
/// Named-dimension and attribute discovery will replace this adapter as part
/// of G1. Keeping the legacy mapping here prevents its assumptions from
/// leaking further into scan execution in the meantime.
pub(crate) fn legacy_array_dataset(array_path: &str, meta: &ArrayMeta) -> ZarrFdwResult<Dataset> {
    let profile = match meta.shape.len() {
        2 => RANK_TWO_PROFILE.as_slice(),
        3 => RANK_THREE_PROFILE.as_slice(),
        rank => return Err(ZarrFdwError::UnsupportedRank { rank }),
    };
    let coordinate_parent = array_parent_path(array_path);
    let dimensions = profile
        .iter()
        .zip(meta.shape.iter())
        .map(|((name, role), &length)| {
            Dimension::new(
                (*name).to_string(),
                length,
                CoordinateRef::new(coordinate_parent.to_string(), (*name).to_string()),
                *role,
            )
        })
        .collect::<Vec<_>>();
    Ok(Dataset::new(
        dimensions,
        array_path.to_string(),
        meta.dtype.clone(),
    ))
}

fn array_parent_path(array_path: &str) -> &str {
    array_path
        .rsplit_once('/')
        .map(|(parent, _)| parent)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta(shape: Vec<u64>, chunks: Vec<u64>) -> ArrayMeta {
        ArrayMeta {
            zarr_format: 2,
            shape,
            chunks,
            dtype: "<f4".to_string(),
            fill_value: serde_json::json!(-7.5),
            compressor: None,
            dimension_separator: ".".to_string(),
            order: 'C',
            filters: None,
        }
    }

    #[test]
    fn adapts_rank_three_profile_without_leaking_path_rules_to_the_scan() {
        let dataset =
            legacy_array_dataset("nested/temperature", &meta(vec![2, 5, 6], vec![1, 5, 3]))
                .unwrap();

        assert_eq!(
            dataset.variable().dimensions(),
            &["time".to_string(), "y".to_string(), "x".to_string()]
        );
        assert_eq!(dataset.variable().path(), "nested/temperature");
        assert_eq!(dataset.variable().dtype(), "<f4");
        assert_eq!(dataset.dimensions()[0].length(), 2);
        assert_eq!(dataset.dimensions()[0].semantic_role(), DimensionRole::Time);
        assert_eq!(dataset.dimensions()[2].coordinate().parent(), "nested");
        assert_eq!(dataset.dimensions()[2].coordinate().name(), "x");
    }

    #[test]
    fn adapts_rank_two_root_profile() {
        let dataset = legacy_array_dataset("value", &meta(vec![5, 6], vec![5, 3])).unwrap();

        assert_eq!(
            dataset.variable().dimensions(),
            &["y".to_string(), "x".to_string()]
        );
        assert_eq!(dataset.dimensions()[0].coordinate().parent(), "");
        assert_eq!(
            dataset.dimensions()[1].semantic_role(),
            DimensionRole::SpatialX
        );
    }

    #[test]
    fn unsupported_rank_stays_explicit() {
        assert!(matches!(
            legacy_array_dataset("cube", &meta(vec![1, 2, 3, 4], vec![1, 1, 1, 1])),
            Err(ZarrFdwError::UnsupportedRank { rank: 4 })
        ));
    }
}
