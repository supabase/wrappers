//! Strict, bounded exact selectors for named Zarr dimensions.

use std::collections::HashSet;
use std::fmt;

use serde::de::{Error as _, MapAccess, Visitor};
use serde::{Deserialize, Deserializer};
use serde_json::Value as JsonValue;

use super::chunk::IndexBounds;
use super::selection::Selection;
use super::{ZarrFdwError, ZarrFdwResult};

pub(crate) const OPT_DIMENSION_SELECTORS: &str = "dimension_selectors";
const MAX_SELECTOR_DOCUMENT_BYTES: usize = 64 * 1024;
const MAX_SELECTOR_DIMENSIONS: usize = 64;

#[derive(Clone, Copy, Debug, PartialEq)]
enum DimensionSelector {
    Index(usize),
    Value(f64),
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct DimensionSelectors {
    entries: Vec<(String, DimensionSelector)>,
}

impl DimensionSelectors {
    pub(crate) fn parse(raw: Option<&str>) -> ZarrFdwResult<Self> {
        let Some(raw) = raw else {
            return Ok(Self::default());
        };
        if raw.len() > MAX_SELECTOR_DOCUMENT_BYTES {
            return Err(invalid_selector_option(format!(
                "JSON document has {} bytes, exceeding the {MAX_SELECTOR_DOCUMENT_BYTES}-byte limit",
                raw.len()
            )));
        }

        let raw_selectors = serde_json::from_str::<RawDimensionSelectors>(raw)
            .map_err(|error| invalid_selector_option(error.to_string()))?;
        let mut entries = Vec::new();
        entries
            .try_reserve_exact(raw_selectors.0.len())
            .map_err(|_| invalid_selector_option("could not allocate dimension selectors"))?;
        for (dimension, raw_selector) in raw_selectors.0 {
            if raw_selector.0.len() != 1 {
                return Err(invalid_selector_option(format!(
                    "selector for dimension '{dimension}' must contain exactly one of 'index' or 'value'"
                )));
            }
            let (kind, value) = raw_selector.0.into_iter().next().expect("length checked");
            let selector = match kind.as_str() {
                "index" => {
                    let index = value.as_u64().ok_or_else(|| {
                        invalid_selector_option(format!(
                            "selector index for dimension '{dimension}' must be a non-negative integer"
                        ))
                    })?;
                    DimensionSelector::Index(usize::try_from(index).map_err(|_| {
                        invalid_selector_option(format!(
                            "selector index for dimension '{dimension}' exceeds this platform's index capacity"
                        ))
                    })?)
                }
                "value" => {
                    let value = value.as_f64().filter(|value| value.is_finite()).ok_or_else(|| {
                        invalid_selector_option(format!(
                            "selector value for dimension '{dimension}' must be a finite JSON number"
                        ))
                    })?;
                    DimensionSelector::Value(value)
                }
                _ => {
                    return Err(invalid_selector_option(format!(
                        "selector for dimension '{dimension}' must contain exactly one of 'index' or 'value'"
                    )));
                }
            };
            entries.push((dimension, selector));
        }
        Ok(Self { entries })
    }

    pub(crate) fn bind(
        &self,
        axis_names: &[String],
        shape: &[u64],
    ) -> ZarrFdwResult<BoundDimensionSelectors> {
        if shape.len() != axis_names.len() {
            return Err(ZarrFdwError::InvalidMetadata(format!(
                "dimension selector binding rank {} does not match array rank {}",
                axis_names.len(),
                shape.len()
            )));
        }
        let mut by_axis = vec![None; axis_names.len()];
        for (dimension, selector) in &self.entries {
            let axis = axis_names
                .iter()
                .position(|axis_name| axis_name == dimension)
                .ok_or_else(|| {
                    invalid_selector_option(format!(
                        "dimension selector references unknown dimension '{dimension}'"
                    ))
                })?;
            if let DimensionSelector::Index(index) = selector {
                let length = usize::try_from(shape[axis]).map_err(|_| {
                    ZarrFdwError::InvalidMetadata(format!(
                        "dimension '{dimension}' length exceeds this platform's index capacity"
                    ))
                })?;
                if *index >= length {
                    return Err(invalid_selector_option(format!(
                        "dimension selector index {index} is outside dimension '{dimension}' length {length}"
                    )));
                }
            }
            by_axis[axis] = Some(*selector);
        }
        Ok(BoundDimensionSelectors {
            axis_names: axis_names.to_vec(),
            by_axis,
        })
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct BoundDimensionSelectors {
    axis_names: Vec<String>,
    by_axis: Vec<Option<DimensionSelector>>,
}

impl BoundDimensionSelectors {
    pub(crate) fn is_empty(&self) -> bool {
        self.by_axis.iter().all(Option::is_none)
    }

    pub(crate) fn requires_coordinate(&self, axis: usize) -> bool {
        matches!(
            self.by_axis.get(axis),
            Some(Some(DimensionSelector::Value(_)))
        )
    }

    pub(crate) fn selects_axis(&self, axis: usize) -> bool {
        self.by_axis.get(axis).is_some_and(Option::is_some)
    }

    pub(crate) fn resolve(
        &self,
        shape: &[u64],
        coordinate_values: &[Option<Vec<f64>>],
        mut poll_interrupt: impl FnMut() -> ZarrFdwResult<()>,
    ) -> ZarrFdwResult<Selection> {
        let rank = self.by_axis.len();
        if shape.len() != rank || coordinate_values.len() != rank {
            return Err(ZarrFdwError::InvalidMetadata(format!(
                "dimension selector inputs do not match array rank {rank}"
            )));
        }

        let mut bounds = vec![None; rank];
        for (axis, selector) in self.by_axis.iter().enumerate() {
            let Some(selector) = selector else {
                continue;
            };
            let axis_name = &self.axis_names[axis];
            let (first, last) = match selector {
                DimensionSelector::Index(index) => {
                    let length = usize::try_from(shape[axis]).map_err(|_| {
                        ZarrFdwError::InvalidMetadata(format!(
                            "dimension {axis} length exceeds this platform's index capacity"
                        ))
                    })?;
                    debug_assert!(
                        *index < length,
                        "selector indexes are checked while binding"
                    );
                    (*index, *index)
                }
                DimensionSelector::Value(value) => {
                    let coordinates = coordinate_values[axis].as_deref().ok_or_else(|| {
                        ZarrFdwError::InvalidMetadata(format!(
                            "coordinate for dimension '{axis_name}' is required by a value selector but was not loaded"
                        ))
                    })?;
                    let mut first = None;
                    let mut last = None;
                    for (index, coordinate) in coordinates.iter().enumerate() {
                        if index % 1_024 == 0 {
                            poll_interrupt()?;
                        }
                        if *coordinate == *value {
                            first.get_or_insert(index);
                            last = Some(index);
                        }
                    }
                    let Some(first) = first else {
                        return Ok(Selection::empty(rank));
                    };
                    (
                        first,
                        last.expect("a first selector match also sets the last match"),
                    )
                }
            };
            bounds[axis] = Some(IndexBounds {
                start: first,
                end: last,
            });
        }
        Ok(Selection::from_axis_bounds(bounds))
    }

    pub(crate) fn matches_axis_index(
        &self,
        axis: usize,
        index: usize,
        coordinate_values: &[Option<Vec<f64>>],
    ) -> ZarrFdwResult<bool> {
        match self.by_axis.get(axis).copied().flatten() {
            None => Ok(true),
            Some(DimensionSelector::Index(selected)) => Ok(index == selected),
            Some(DimensionSelector::Value(selected)) => {
                let axis_name = self.axis_names.get(axis).map_or("unknown", String::as_str);
                let coordinates = coordinate_values
                    .get(axis)
                    .and_then(Option::as_deref)
                    .ok_or_else(|| {
                        ZarrFdwError::InvalidMetadata(format!(
                            "coordinate for dimension '{axis_name}' is required by a value selector but was not loaded"
                        ))
                    })?;
                Ok(coordinates
                    .get(index)
                    .is_some_and(|value| *value == selected))
            }
        }
    }
}

fn invalid_selector_option(message: impl Into<String>) -> ZarrFdwError {
    ZarrFdwError::InvalidOptionValue {
        option: OPT_DIMENSION_SELECTORS.to_string(),
        message: message.into(),
    }
}

struct RawDimensionSelectors(Vec<(String, RawSelector)>);

impl<'de> Deserialize<'de> for RawDimensionSelectors {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(RawDimensionSelectorsVisitor)
    }
}

struct RawDimensionSelectorsVisitor;

impl<'de> Visitor<'de> for RawDimensionSelectorsVisitor {
    type Value = RawDimensionSelectors;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a JSON object mapping dimension names to selector objects")
    }

    fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
    where
        M: MapAccess<'de>,
    {
        let mut entries = Vec::new();
        let mut names = HashSet::new();
        while let Some((dimension, selector)) = map.next_entry::<String, RawSelector>()? {
            if !names.insert(dimension.clone()) {
                return Err(M::Error::custom(format!(
                    "duplicate dimension selector '{dimension}'"
                )));
            }
            if entries.len() == MAX_SELECTOR_DIMENSIONS {
                return Err(M::Error::custom(format!(
                    "dimension selector object exceeds the {MAX_SELECTOR_DIMENSIONS}-dimension limit"
                )));
            }
            entries.push((dimension, selector));
        }
        Ok(RawDimensionSelectors(entries))
    }
}

struct RawSelector(Vec<(String, JsonValue)>);

impl<'de> Deserialize<'de> for RawSelector {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(RawSelectorVisitor)
    }
}

struct RawSelectorVisitor;

impl<'de> Visitor<'de> for RawSelectorVisitor {
    type Value = RawSelector;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a selector object")
    }

    fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
    where
        M: MapAccess<'de>,
    {
        let mut entries = Vec::new();
        while let Some(entry) = map.next_entry::<String, JsonValue>()? {
            entries.push(entry);
        }
        Ok(RawSelector(entries))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_exact_index_and_value_selectors() {
        let selectors =
            DimensionSelectors::parse(Some(r#"{"band":{"index":3},"level":{"value":850}}"#))
                .unwrap();
        let bound = selectors
            .bind(&["level".to_string(), "band".to_string()], &[3, 4])
            .unwrap();

        assert!(bound.requires_coordinate(0));
        assert!(!bound.requires_coordinate(1));
        let selection = bound
            .resolve(
                &[3, 4],
                &[Some(vec![1000.0, 850.0, 500.0]), None],
                || Ok(()),
            )
            .unwrap();
        assert_eq!(
            selection.axis_bounds(),
            &[
                Some(IndexBounds { start: 1, end: 1 }),
                Some(IndexBounds { start: 3, end: 3 })
            ]
        );
        assert!(
            bound
                .matches_axis_index(0, 1, &[Some(vec![1000.0, 850.0, 500.0]), None])
                .unwrap()
        );
        assert!(
            !bound
                .matches_axis_index(0, 0, &[Some(vec![1000.0, 850.0, 500.0]), None])
                .unwrap()
        );
    }

    #[test]
    fn value_selector_preserves_duplicate_exact_membership() {
        let selectors = DimensionSelectors::parse(Some(r#"{"band":{"value":30}}"#)).unwrap();
        let bound = selectors.bind(&["band".to_string()], &[4]).unwrap();
        let selection = bound
            .resolve(&[4], &[Some(vec![30.0, 10.0, 30.0, 20.0])], || Ok(()))
            .unwrap();

        assert_eq!(
            selection.axis_bounds(),
            &[Some(IndexBounds { start: 0, end: 2 })]
        );
        let coordinates = [Some(vec![30.0, 10.0, 30.0, 20.0])];
        assert!(bound.matches_axis_index(0, 0, &coordinates).unwrap());
        assert!(!bound.matches_axis_index(0, 1, &coordinates).unwrap());
        assert!(bound.matches_axis_index(0, 2, &coordinates).unwrap());
    }

    #[test]
    fn no_matching_value_is_an_empty_selection() {
        let selectors = DimensionSelectors::parse(Some(r#"{"band":{"value":999}}"#)).unwrap();
        let bound = selectors.bind(&["band".to_string()], &[2]).unwrap();
        let selection = bound
            .resolve(&[2], &[Some(vec![10.0, 20.0])], || Ok(()))
            .unwrap();

        assert!(selection.is_empty());
    }

    #[test]
    fn strict_parser_rejects_invalid_and_duplicate_members() {
        for raw in [
            "[]",
            r#"{"band":null}"#,
            r#"{"band":{}}"#,
            r#"{"band":{"index":1,"value":2}}"#,
            r#"{"band":{"index":1,"index":2}}"#,
            r#"{"band":{"unknown":1}}"#,
            r#"{"band":{"index":-1}}"#,
            r#"{"band":{"index":1.5}}"#,
            r#"{"band":{"value":"B04"}}"#,
            r#"{"band":{"value":null}}"#,
            r#"{"band":{"index":1},"band":{"index":2}}"#,
        ] {
            assert!(DimensionSelectors::parse(Some(raw)).is_err(), "{raw}");
        }
    }

    #[test]
    fn parser_bounds_document_and_dimension_counts() {
        let oversized = format!(r#"{{"band":{{"index":0}},"pad":"{}"}}"#, "x".repeat(65_536));
        assert!(DimensionSelectors::parse(Some(&oversized)).is_err());

        let members = (0..65)
            .map(|index| format!(r#""d{index}":{{"index":0}}"#))
            .collect::<Vec<_>>()
            .join(",");
        assert!(DimensionSelectors::parse(Some(&format!("{{{members}}}"))).is_err());
    }

    #[test]
    fn bind_and_index_resolution_fail_clearly() {
        let unknown = DimensionSelectors::parse(Some(r#"{"member":{"index":0}}"#)).unwrap();
        assert!(unknown.bind(&["band".to_string()], &[6]).is_err());

        let outside = DimensionSelectors::parse(Some(r#"{"band":{"index":6}}"#)).unwrap();
        let error = outside.bind(&["band".to_string()], &[6]).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("dimension selector index 6 is outside dimension 'band' length 6")
        );
    }
}
