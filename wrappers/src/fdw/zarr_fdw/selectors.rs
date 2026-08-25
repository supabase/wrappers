//! Strict, bounded exact selectors for named Zarr dimensions.

use std::collections::HashSet;
use std::fmt;

use serde::de::{Error as _, IgnoredAny, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};
use serde_json::{Number as JsonNumber, Value as JsonValue};

use super::chunk::IndexBounds;
use super::selection::Selection;
use super::{ZarrFdwError, ZarrFdwResult};

pub(crate) const OPT_DIMENSION_SELECTORS: &str = "dimension_selectors";
const MAX_SELECTOR_DOCUMENT_BYTES: usize = 64 * 1024;
const MAX_SELECTOR_DIMENSIONS: usize = 64;
const MAX_SELECTOR_MEMBERS: usize = 4_096;

#[derive(Clone, Debug, PartialEq)]
enum DimensionSelector {
    Index(usize),
    Indices(Vec<usize>),
    IndexRange { start: usize, stop: usize },
    Value(f64),
    Values(Vec<f64>),
    ValueRange { min: f64, max: f64 },
}

impl DimensionSelector {
    fn matches_index(&self, index: usize, coordinates: Option<&[f64]>) -> ZarrFdwResult<bool> {
        match self {
            Self::Index(selected) => Ok(index == *selected),
            Self::Indices(selected) => Ok(selected.binary_search(&index).is_ok()),
            Self::IndexRange { start, stop } => Ok(*start <= index && index < *stop),
            Self::Value(selected) => Ok(coordinate_at(coordinates, index)? == *selected),
            Self::Values(selected) => {
                let coordinate = coordinate_at(coordinates, index)?;
                if !coordinate.is_finite() {
                    return Ok(false);
                }
                Ok(selected
                    .binary_search_by(|candidate| {
                        candidate
                            .partial_cmp(&coordinate)
                            .expect("selector and coordinate values are finite")
                    })
                    .is_ok())
            }
            Self::ValueRange { min, max } => {
                let coordinate = coordinate_at(coordinates, index)?;
                Ok(*min <= coordinate && coordinate <= *max)
            }
        }
    }

    fn conservative_bounds(
        &self,
        length: usize,
        coordinates: Option<&[f64]>,
        poll_interrupt: &mut impl FnMut() -> ZarrFdwResult<()>,
    ) -> ZarrFdwResult<Option<IndexBounds>> {
        match self {
            Self::Index(index) => Ok(Some(IndexBounds {
                start: *index,
                end: *index,
            })),
            Self::Indices(indices) => Ok(Some(IndexBounds {
                start: *indices
                    .first()
                    .expect("nonempty list validated while parsing"),
                end: *indices
                    .last()
                    .expect("nonempty list validated while parsing"),
            })),
            Self::IndexRange { start, stop } => Ok(Some(IndexBounds {
                start: *start,
                end: *stop - 1,
            })),
            Self::Value(_) | Self::Values(_) | Self::ValueRange { .. } => {
                let coordinates = coordinates.ok_or_else(|| {
                    ZarrFdwError::InvalidMetadata(
                        "coordinate required by a value selector was not loaded".to_string(),
                    )
                })?;
                if coordinates.len() != length {
                    return Err(ZarrFdwError::InvalidMetadata(format!(
                        "coordinate length {} does not match selected dimension length {length}",
                        coordinates.len()
                    )));
                }
                let mut first = None;
                let mut last = None;
                for index in 0..length {
                    if index % 1_024 == 0 {
                        poll_interrupt()?;
                    }
                    if self.matches_index(index, Some(coordinates))? {
                        first.get_or_insert(index);
                        last = Some(index);
                    }
                }
                Ok(first.map(|start| IndexBounds {
                    start,
                    end: last.expect("a first selector match also sets the last match"),
                }))
            }
        }
    }
}

fn coordinate_at(coordinates: Option<&[f64]>, index: usize) -> ZarrFdwResult<f64> {
    coordinates
        .and_then(|values| values.get(index))
        .copied()
        .ok_or_else(|| {
            ZarrFdwError::InvalidMetadata(format!(
                "coordinate required by a value selector has no value at index {index}"
            ))
        })
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
                    "selector for dimension '{dimension}' must contain exactly one supported selector form"
                )));
            }
            let (kind, value) = raw_selector.0.into_iter().next().expect("length checked");
            let selector = match kind.as_str() {
                "index" => DimensionSelector::Index(parse_index(&dimension, value.into_json()?)?),
                "value" => DimensionSelector::Value(parse_value(&dimension, value.into_json()?)?),
                "indices" => {
                    DimensionSelector::Indices(parse_indices(&dimension, value.into_list()?)?)
                }
                "values" => {
                    DimensionSelector::Values(parse_values(&dimension, value.into_list()?)?)
                }
                "index_range" => {
                    let range = value.into_object()?;
                    let (start, stop) = parse_index_range(&dimension, range)?;
                    DimensionSelector::IndexRange { start, stop }
                }
                "value_range" => {
                    let range = value.into_object()?;
                    let (min, max) = parse_value_range(&dimension, range)?;
                    DimensionSelector::ValueRange { min, max }
                }
                _ => {
                    return Err(invalid_selector_option(format!(
                        "selector for dimension '{dimension}' must contain exactly one supported selector form"
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
            let length = usize::try_from(shape[axis]).map_err(|_| {
                ZarrFdwError::InvalidMetadata(format!(
                    "dimension '{dimension}' length exceeds this platform's index capacity"
                ))
            })?;
            match selector {
                DimensionSelector::Index(index) if *index >= length => {
                    return Err(invalid_selector_option(format!(
                        "dimension selector index {index} is outside dimension '{dimension}' length {length}"
                    )));
                }
                DimensionSelector::Indices(indices) => {
                    if let Some(index) = indices.iter().find(|&&index| index >= length) {
                        return Err(invalid_selector_option(format!(
                            "dimension selector index {index} is outside dimension '{dimension}' length {length}"
                        )));
                    }
                }
                DimensionSelector::IndexRange { stop, .. } if *stop > length => {
                    return Err(invalid_selector_option(format!(
                        "dimension selector index range stop {stop} exceeds dimension '{dimension}' length {length}"
                    )));
                }
                _ => {}
            }
            by_axis[axis] = Some(selector.clone());
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
            Some(Some(
                DimensionSelector::Value(_)
                    | DimensionSelector::Values(_)
                    | DimensionSelector::ValueRange { .. }
            ))
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
            let length = usize::try_from(shape[axis]).map_err(|_| {
                ZarrFdwError::InvalidMetadata(format!(
                    "dimension {axis} length exceeds this platform's index capacity"
                ))
            })?;
            let coordinates = if self.requires_coordinate(axis) {
                Some(coordinate_values[axis].as_deref().ok_or_else(|| {
                    ZarrFdwError::InvalidMetadata(format!(
                        "coordinate for dimension '{axis_name}' is required by a value selector but was not loaded"
                    ))
                })?)
            } else {
                None
            };
            let Some(selector_bounds) =
                selector.conservative_bounds(length, coordinates, &mut poll_interrupt)?
            else {
                return Ok(Selection::empty(rank));
            };
            bounds[axis] = Some(selector_bounds);
        }
        Ok(Selection::from_axis_bounds(bounds))
    }

    pub(crate) fn matches_axis_index(
        &self,
        axis: usize,
        index: usize,
        coordinate_values: &[Option<Vec<f64>>],
    ) -> ZarrFdwResult<bool> {
        match self.by_axis.get(axis).and_then(Option::as_ref) {
            None => Ok(true),
            Some(selector) => {
                let axis_name = self.axis_names.get(axis).map_or("unknown", String::as_str);
                let coordinates = if self.requires_coordinate(axis) {
                    Some(
                        coordinate_values
                            .get(axis)
                            .and_then(Option::as_deref)
                            .ok_or_else(|| {
                                ZarrFdwError::InvalidMetadata(format!(
                                    "coordinate for dimension '{axis_name}' is required by a value selector but was not loaded"
                                ))
                            })?,
                    )
                } else {
                    None
                };
                selector.matches_index(index, coordinates)
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

fn parse_index(dimension: &str, value: JsonValue) -> ZarrFdwResult<usize> {
    let index = value.as_u64().ok_or_else(|| {
        invalid_selector_option(format!(
            "selector index for dimension '{dimension}' must be a non-negative integer"
        ))
    })?;
    usize::try_from(index).map_err(|_| {
        invalid_selector_option(format!(
            "selector index for dimension '{dimension}' exceeds this platform's index capacity"
        ))
    })
}

fn parse_value(dimension: &str, value: JsonValue) -> ZarrFdwResult<f64> {
    let value = value
        .as_f64()
        .filter(|value| value.is_finite())
        .ok_or_else(|| {
            invalid_selector_option(format!(
                "selector value for dimension '{dimension}' must be a finite JSON number"
            ))
        })?;
    Ok(normalize_zero(value))
}

fn parse_indices(dimension: &str, raw: RawList) -> ZarrFdwResult<Vec<usize>> {
    if raw.0.is_empty() {
        return Err(invalid_selector_option(format!(
            "selector indices for dimension '{dimension}' must be a nonempty array"
        )));
    }

    let mut indices = Vec::new();
    indices
        .try_reserve_exact(raw.0.len())
        .map_err(|_| invalid_selector_option("could not allocate selector indices"))?;
    for value in raw.0 {
        let index = value.as_u64().ok_or_else(|| {
            invalid_selector_option(format!(
                "each selector index for dimension '{dimension}' must be a non-negative integer"
            ))
        })?;
        indices.push(usize::try_from(index).map_err(|_| {
            invalid_selector_option(format!(
                "selector index for dimension '{dimension}' exceeds this platform's index capacity"
            ))
        })?);
    }
    indices.sort_unstable();
    if let Some(duplicate) = indices.windows(2).find(|pair| pair[0] == pair[1]) {
        return Err(invalid_selector_option(format!(
            "selector indices for dimension '{dimension}' contain duplicate index {}",
            duplicate[0]
        )));
    }
    Ok(indices)
}

fn parse_values(dimension: &str, raw: RawList) -> ZarrFdwResult<Vec<f64>> {
    if raw.0.is_empty() {
        return Err(invalid_selector_option(format!(
            "selector values for dimension '{dimension}' must be a nonempty array"
        )));
    }

    let mut values = Vec::new();
    values
        .try_reserve_exact(raw.0.len())
        .map_err(|_| invalid_selector_option("could not allocate selector values"))?;
    for value in raw.0 {
        let value = value
            .as_f64()
            .filter(|value| value.is_finite())
            .ok_or_else(|| {
                invalid_selector_option(format!(
                    "each selector value for dimension '{dimension}' must be a finite JSON number"
                ))
            })?;
        values.push(normalize_zero(value));
    }
    values.sort_by(|left, right| left.partial_cmp(right).expect("selector values are finite"));
    if let Some(duplicate) = values.windows(2).find(|pair| pair[0] == pair[1]) {
        return Err(invalid_selector_option(format!(
            "selector values for dimension '{dimension}' contain duplicate value {}",
            duplicate[0]
        )));
    }
    Ok(values)
}

fn parse_index_range(dimension: &str, raw: RawRangeObject) -> ZarrFdwResult<(usize, usize)> {
    let mut start = None;
    let mut stop = None;
    for (member, value) in raw.0 {
        match member.as_str() {
            "start" => start = Some(parse_index_range_member(dimension, "start", value)?),
            "stop" => stop = Some(parse_index_range_member(dimension, "stop", value)?),
            _ => {
                return Err(invalid_selector_option(format!(
                    "index_range for dimension '{dimension}' must contain exactly 'start' and 'stop'"
                )));
            }
        }
    }
    let (Some(start), Some(stop)) = (start, stop) else {
        return Err(invalid_selector_option(format!(
            "index_range for dimension '{dimension}' must contain exactly 'start' and 'stop'"
        )));
    };
    if start >= stop {
        return Err(invalid_selector_option(format!(
            "index_range for dimension '{dimension}' requires start < stop"
        )));
    }
    Ok((start, stop))
}

fn parse_index_range_member(
    dimension: &str,
    member: &str,
    value: JsonValue,
) -> ZarrFdwResult<usize> {
    let index = value.as_u64().ok_or_else(|| {
        invalid_selector_option(format!(
            "index_range member '{member}' for dimension '{dimension}' must be a non-negative integer"
        ))
    })?;
    usize::try_from(index).map_err(|_| {
        invalid_selector_option(format!(
            "index_range member '{member}' for dimension '{dimension}' exceeds this platform's index capacity"
        ))
    })
}

fn parse_value_range(dimension: &str, raw: RawRangeObject) -> ZarrFdwResult<(f64, f64)> {
    let mut min = None;
    let mut max = None;
    for (member, value) in raw.0 {
        match member.as_str() {
            "min" => min = Some(parse_value_range_member(dimension, "min", value)?),
            "max" => max = Some(parse_value_range_member(dimension, "max", value)?),
            _ => {
                return Err(invalid_selector_option(format!(
                    "value_range for dimension '{dimension}' must contain exactly 'min' and 'max'"
                )));
            }
        }
    }
    let (Some(min), Some(max)) = (min, max) else {
        return Err(invalid_selector_option(format!(
            "value_range for dimension '{dimension}' must contain exactly 'min' and 'max'"
        )));
    };
    if min > max {
        return Err(invalid_selector_option(format!(
            "value_range for dimension '{dimension}' requires min <= max"
        )));
    }
    Ok((min, max))
}

fn parse_value_range_member(dimension: &str, member: &str, value: JsonValue) -> ZarrFdwResult<f64> {
    let value = value.as_f64().filter(|value| value.is_finite()).ok_or_else(|| {
        invalid_selector_option(format!(
            "value_range member '{member}' for dimension '{dimension}' must be a finite JSON number"
        ))
    })?;
    Ok(normalize_zero(value))
}

fn normalize_zero(value: f64) -> f64 {
    if value == 0.0 { 0.0 } else { value }
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
        entries
            .try_reserve_exact(MAX_SELECTOR_DIMENSIONS.min(map.size_hint().unwrap_or(0)))
            .map_err(M::Error::custom)?;
        let mut names = HashSet::new();
        names
            .try_reserve(MAX_SELECTOR_DIMENSIONS.min(map.size_hint().unwrap_or(0)))
            .map_err(M::Error::custom)?;
        while let Some(dimension) = map.next_key::<String>()? {
            if entries.len() == MAX_SELECTOR_DIMENSIONS {
                return Err(M::Error::custom(format!(
                    "dimension selector object exceeds the {MAX_SELECTOR_DIMENSIONS}-dimension limit"
                )));
            }
            if !names.insert(dimension.clone()) {
                return Err(M::Error::custom(format!(
                    "duplicate dimension selector '{dimension}'"
                )));
            }
            let selector = map.next_value::<RawSelector>()?;
            if entries.len() == entries.capacity() {
                entries.try_reserve(1).map_err(M::Error::custom)?;
            }
            entries.push((dimension, selector));
        }
        Ok(RawDimensionSelectors(entries))
    }
}

struct RawSelector(Vec<(String, RawSelectorValue)>);

enum RawSelectorValue {
    Json(JsonValue),
    List(RawList),
    Object(RawRangeObject),
}

impl RawSelectorValue {
    fn into_json(self) -> ZarrFdwResult<JsonValue> {
        match self {
            Self::Json(value) => Ok(value),
            Self::List(_) | Self::Object(_) => Err(invalid_selector_option(
                "selector member has an invalid JSON value type",
            )),
        }
    }

    fn into_list(self) -> ZarrFdwResult<RawList> {
        match self {
            Self::List(value) => Ok(value),
            Self::Json(_) | Self::Object(_) => Err(invalid_selector_option(
                "selector list member must be a JSON array",
            )),
        }
    }

    fn into_object(self) -> ZarrFdwResult<RawRangeObject> {
        match self {
            Self::Object(value) => Ok(value),
            Self::Json(_) | Self::List(_) => Err(invalid_selector_option(
                "selector range member must be a JSON object",
            )),
        }
    }
}

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
        entries.try_reserve_exact(1).map_err(M::Error::custom)?;
        while let Some(kind) = map.next_key::<String>()? {
            if let Some((existing, _)) = entries.first() {
                let message = if existing == &kind {
                    format!("duplicate selector member '{kind}'")
                } else {
                    "selector object must contain exactly one supported selector form".to_string()
                };
                return Err(M::Error::custom(message));
            }
            let value = match kind.as_str() {
                "indices" | "values" => RawSelectorValue::List(map.next_value::<RawList>()?),
                "index_range" | "value_range" => {
                    RawSelectorValue::Object(map.next_value::<RawRangeObject>()?)
                }
                _ => RawSelectorValue::Json(map.next_value::<JsonValue>()?),
            };
            entries.push((kind, value));
        }
        Ok(RawSelector(entries))
    }
}

struct RawList(Vec<JsonNumber>);

impl<'de> Deserialize<'de> for RawList {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(RawListVisitor)
    }
}

struct RawListVisitor;

impl<'de> Visitor<'de> for RawListVisitor {
    type Value = RawList;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a JSON array with at most 4096 entries")
    }

    fn visit_seq<S>(self, mut sequence: S) -> Result<Self::Value, S::Error>
    where
        S: SeqAccess<'de>,
    {
        let initial = sequence.size_hint().unwrap_or(0).min(MAX_SELECTOR_MEMBERS);
        let mut values = Vec::new();
        values
            .try_reserve_exact(initial)
            .map_err(S::Error::custom)?;
        while values.len() < MAX_SELECTOR_MEMBERS {
            let Some(value) = sequence.next_element::<JsonNumber>()? else {
                return Ok(RawList(values));
            };
            if values.len() == values.capacity() {
                values.try_reserve(1).map_err(S::Error::custom)?;
            }
            values.push(value);
        }
        if sequence.next_element::<IgnoredAny>()?.is_some() {
            return Err(S::Error::custom(format!(
                "selector list exceeds the {MAX_SELECTOR_MEMBERS}-entry limit"
            )));
        }
        Ok(RawList(values))
    }
}

struct RawRangeObject(Vec<(String, JsonValue)>);

impl<'de> Deserialize<'de> for RawRangeObject {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(RawRangeObjectVisitor)
    }
}

struct RawRangeObjectVisitor;

impl<'de> Visitor<'de> for RawRangeObjectVisitor {
    type Value = RawRangeObject;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a range selector object")
    }

    fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
    where
        M: MapAccess<'de>,
    {
        let mut entries = Vec::new();
        entries.try_reserve_exact(2).map_err(M::Error::custom)?;
        let mut names = HashSet::new();
        names.try_reserve(2).map_err(M::Error::custom)?;
        while let Some(name) = map.next_key::<String>()? {
            if !names.insert(name.clone()) {
                return Err(M::Error::custom(format!(
                    "duplicate range selector member '{name}'"
                )));
            }
            if entries.len() == 2 {
                return Err(M::Error::custom(
                    "range selector object contains more than two members",
                ));
            }
            entries.push((name, map.next_value::<JsonValue>()?));
        }
        Ok(RawRangeObject(entries))
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
    fn index_lists_and_ranges_use_conservative_bounds_and_exact_membership() {
        let selectors = DimensionSelectors::parse(Some(
            r#"{"band":{"indices":[4,1]},"level":{"index_range":{"start":2,"stop":5}}}"#,
        ))
        .unwrap();
        let bound = selectors
            .bind(&["band".to_string(), "level".to_string()], &[6, 5])
            .unwrap();
        let selection = bound.resolve(&[6, 5], &[None, None], || Ok(())).unwrap();

        assert_eq!(
            selection.axis_bounds(),
            &[
                Some(IndexBounds { start: 1, end: 4 }),
                Some(IndexBounds { start: 2, end: 4 })
            ]
        );
        assert!(bound.matches_axis_index(0, 1, &[None, None]).unwrap());
        assert!(!bound.matches_axis_index(0, 2, &[None, None]).unwrap());
        assert!(bound.matches_axis_index(0, 4, &[None, None]).unwrap());
        assert!(!bound.matches_axis_index(1, 1, &[None, None]).unwrap());
        assert!(bound.matches_axis_index(1, 4, &[None, None]).unwrap());
    }

    #[test]
    fn value_lists_and_ranges_preserve_native_coordinate_membership() {
        let selectors = DimensionSelectors::parse(Some(
            r#"{"band":{"values":[40,10]},"level":{"value_range":{"min":15,"max":30}}}"#,
        ))
        .unwrap();
        let bound = selectors
            .bind(&["band".to_string(), "level".to_string()], &[5, 4])
            .unwrap();
        let coordinates = [
            Some(vec![30.0, 10.0, 20.0, 10.0, 40.0]),
            Some(vec![30.0, 10.0, 20.0, 40.0]),
        ];
        let selection = bound.resolve(&[5, 4], &coordinates, || Ok(())).unwrap();

        assert_eq!(
            selection.axis_bounds(),
            &[
                Some(IndexBounds { start: 1, end: 4 }),
                Some(IndexBounds { start: 0, end: 2 })
            ]
        );
        assert!(bound.matches_axis_index(0, 1, &coordinates).unwrap());
        assert!(!bound.matches_axis_index(0, 2, &coordinates).unwrap());
        assert!(bound.matches_axis_index(0, 3, &coordinates).unwrap());
        assert!(bound.matches_axis_index(0, 4, &coordinates).unwrap());
        assert!(bound.matches_axis_index(1, 0, &coordinates).unwrap());
        assert!(!bound.matches_axis_index(1, 1, &coordinates).unwrap());
        assert!(bound.matches_axis_index(1, 2, &coordinates).unwrap());
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
            r#"{"band":{"indices":[]}}"#,
            r#"{"band":{"indices":[1,1]}}"#,
            r#"{"band":{"values":[]}}"#,
            r#"{"band":{"values":[-0.0,0.0]}}"#,
            r#"{"band":{"values":[1,1.0]}}"#,
            r#"{"band":{"index_range":{"start":2,"stop":2}}}"#,
            r#"{"band":{"index_range":{"start":3,"stop":2}}}"#,
            r#"{"band":{"index_range":{"start":0}}}"#,
            r#"{"band":{"index_range":{"start":0,"start":1,"stop":2}}}"#,
            r#"{"band":{"index_range":{"start":0,"stop":2,"step":1}}}"#,
            r#"{"band":{"value_range":{"min":2,"max":1}}}"#,
            r#"{"band":{"value_range":{"min":1}}}"#,
            r#"{"band":{"value_range":{"min":0,"max":1,"step":0.5}}}"#,
        ] {
            assert!(DimensionSelectors::parse(Some(raw)).is_err(), "{raw}");
        }
    }

    #[test]
    fn parser_rejects_the_4097th_list_member_during_visitation() {
        let members = std::iter::repeat_n("0", MAX_SELECTOR_MEMBERS + 1)
            .collect::<Vec<_>>()
            .join(",");
        let raw = format!(r#"{{"band":{{"indices":[{members}]}}}}"#);

        let error = DimensionSelectors::parse(Some(&raw)).unwrap_err();
        assert!(error.to_string().contains("4096-entry limit"));
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

        let list_outside =
            DimensionSelectors::parse(Some(r#"{"band":{"indices":[0,6]}}"#)).unwrap();
        assert!(list_outside.bind(&["band".to_string()], &[6]).is_err());

        let range_outside =
            DimensionSelectors::parse(Some(r#"{"band":{"index_range":{"start":0,"stop":7}}}"#))
                .unwrap();
        assert!(range_outside.bind(&["band".to_string()], &[6]).is_err());

        let range_to_extent =
            DimensionSelectors::parse(Some(r#"{"band":{"index_range":{"start":0,"stop":6}}}"#))
                .unwrap();
        assert!(range_to_extent.bind(&["band".to_string()], &[6]).is_ok());
    }
}
