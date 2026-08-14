/// Dataset-level description consumed by the scan executor.
///
/// The model is deliberately independent of the legacy `x`/`y`/`time`
/// profile. Metadata adapters assign names and semantic roles; the executor
/// works from those descriptors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Dataset {
    dimensions: Vec<Dimension>,
    variable: Variable,
}

impl Dataset {
    pub(super) fn new(dimensions: Vec<Dimension>, variable_path: String, dtype: String) -> Self {
        let variable_dimensions = dimensions
            .iter()
            .map(|dimension| dimension.name().to_string())
            .collect();
        Self {
            dimensions,
            variable: Variable::new(variable_path, dtype, variable_dimensions),
        }
    }

    pub(crate) fn dimensions(&self) -> &[Dimension] {
        &self.dimensions
    }

    pub(crate) fn variable(&self) -> &Variable {
        &self.variable
    }

    pub(crate) fn axis_names(&self) -> Vec<String> {
        self.dimensions
            .iter()
            .map(|dimension| dimension.name().to_string())
            .collect()
    }

    pub(crate) fn is_dimension(&self, name: &str) -> bool {
        self.dimensions
            .iter()
            .any(|dimension| dimension.name() == name)
    }

    pub(crate) fn dimension(&self, name: &str) -> Option<&Dimension> {
        self.dimensions
            .iter()
            .find(|dimension| dimension.name() == name)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Dimension {
    name: String,
    length: u64,
    coordinate: CoordinateRef,
    semantic_role: DimensionRole,
}

impl Dimension {
    pub(super) fn new(
        name: String,
        length: u64,
        coordinate: CoordinateRef,
        semantic_role: DimensionRole,
    ) -> Self {
        Self {
            name,
            length,
            coordinate,
            semantic_role,
        }
    }

    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn length(&self) -> u64 {
        self.length
    }

    pub(crate) fn coordinate(&self) -> &CoordinateRef {
        &self.coordinate
    }

    pub(crate) fn semantic_role(&self) -> DimensionRole {
        self.semantic_role
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CoordinateRef {
    parent: String,
    name: String,
}

impl CoordinateRef {
    pub(super) fn new(parent: String, name: String) -> Self {
        Self { parent, name }
    }

    pub(crate) fn parent(&self) -> &str {
        &self.parent
    }

    pub(crate) fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DimensionRole {
    SpatialX,
    SpatialY,
    Latitude,
    Longitude,
    Vertical,
    Time,
    Band,
    Channel,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Variable {
    path: String,
    dtype: String,
    dimensions: Vec<String>,
}

impl Variable {
    fn new(path: String, dtype: String, dimensions: Vec<String>) -> Self {
        Self {
            path,
            dtype,
            dimensions,
        }
    }

    pub(crate) fn path(&self) -> &str {
        &self.path
    }

    pub(crate) fn dtype(&self) -> &str {
        &self.dtype
    }

    pub(crate) fn dimensions(&self) -> &[String] {
        &self.dimensions
    }
}
