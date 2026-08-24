//! Optional, query-local PostGIS catalog and geometry transformation adapter.
//!
//! The core extension has no PostGIS link-time dependency. This module first
//! proves that the geometry type and every called function are owned by the
//! installed `postgis` extension, then invokes only schema-qualified functions
//! with parameterized values.

use pgrx::{
    Spi,
    pg_sys::{PgTryBuilder, errcodes::PgSqlErrorCode},
    spi::quote_identifier,
};

use super::super::{ZarrFdwError, ZarrFdwResult};

const MAX_EWKB_BYTES: usize = 8 * 1024 * 1024;
pub(crate) const MAX_COVERAGE_CANDIDATES: usize = 65_536;

const DISCOVER_POSTGIS_SQL: &str = r#"
    SELECT extension.oid::bigint AS extension_oid,
           namespace.nspname::text AS schema_name
      FROM pg_catalog.pg_extension AS extension
      JOIN pg_catalog.pg_namespace AS namespace
        ON namespace.oid = extension.extnamespace
     WHERE extension.extname = 'postgis'
"#;

// Verify extension ownership as well as names/signatures. A same-name object in
// the PostGIS schema is not accepted unless pg_depend records it as an extension
// member. Built-in argument/result OIDs are stable PostgreSQL catalog OIDs.
const VERIFY_POSTGIS_MEMBERS_SQL: &str = r#"
    WITH owned_geometry AS (
        SELECT type.oid AS geometry_oid
          FROM pg_catalog.pg_type AS type
          JOIN pg_catalog.pg_extension AS extension
            ON extension.oid = $1::bigint::oid
           AND type.typnamespace = extension.extnamespace
          JOIN pg_catalog.pg_depend AS dependency
            ON dependency.classid = 'pg_catalog.pg_type'::pg_catalog.regclass
           AND dependency.objid = type.oid
           AND dependency.refclassid = 'pg_catalog.pg_extension'::pg_catalog.regclass
           AND dependency.refobjid = extension.oid
           AND dependency.deptype = 'e'
         WHERE type.typname = 'geometry'
    ),
    owned_box3d AS (
        SELECT type.oid AS box3d_oid
          FROM pg_catalog.pg_type AS type
          JOIN pg_catalog.pg_extension AS extension
            ON extension.oid = $1::bigint::oid
           AND type.typnamespace = extension.extnamespace
          JOIN pg_catalog.pg_depend AS dependency
            ON dependency.classid = 'pg_catalog.pg_type'::pg_catalog.regclass
           AND dependency.objid = type.oid
           AND dependency.refclassid = 'pg_catalog.pg_extension'::pg_catalog.regclass
           AND dependency.refobjid = extension.oid
           AND dependency.deptype = 'e'
         WHERE type.typname = 'box3d'
    ),
    owned_functions AS (
        SELECT procedure.proname,
               procedure.pronargs,
               procedure.proargtypes,
               procedure.prorettype,
               geometry.geometry_oid,
               box3d.box3d_oid
          FROM pg_catalog.pg_proc AS procedure
          JOIN pg_catalog.pg_extension AS extension
            ON extension.oid = $1::bigint::oid
           AND procedure.pronamespace = extension.extnamespace
          CROSS JOIN owned_geometry AS geometry
          CROSS JOIN owned_box3d AS box3d
          JOIN pg_catalog.pg_depend AS dependency
            ON dependency.classid = 'pg_catalog.pg_proc'::pg_catalog.regclass
           AND dependency.objid = procedure.oid
           AND dependency.refclassid = 'pg_catalog.pg_extension'::pg_catalog.regclass
           AND dependency.refobjid = extension.oid
           AND dependency.deptype = 'e'
    ),
    owned_spatial_ref_sys AS (
        SELECT relation.oid
          FROM pg_catalog.pg_class AS relation
          JOIN pg_catalog.pg_extension AS extension
            ON extension.oid = $1::bigint::oid
           AND relation.relnamespace = extension.extnamespace
          JOIN pg_catalog.pg_depend AS dependency
            ON dependency.classid = 'pg_catalog.pg_class'::pg_catalog.regclass
           AND dependency.objid = relation.oid
           AND dependency.refclassid = 'pg_catalog.pg_extension'::pg_catalog.regclass
           AND dependency.refobjid = extension.oid
           AND dependency.deptype = 'e'
         WHERE relation.relname = 'spatial_ref_sys'
    )
    SELECT EXISTS (SELECT 1 FROM owned_geometry)
       AND EXISTS (SELECT 1 FROM owned_box3d)
       AND EXISTS (SELECT 1 FROM owned_spatial_ref_sys)
       AND EXISTS (
            SELECT 1 FROM owned_functions
             WHERE proname = 'st_geomfromewkb' AND pronargs = 1
               AND proargtypes[0] = 17 AND prorettype = geometry_oid
       )
       AND EXISTS (
            SELECT 1 FROM owned_functions
             WHERE proname = 'st_srid' AND pronargs = 1
               AND proargtypes[0] = geometry_oid AND prorettype = 23
       )
       AND EXISTS (
            SELECT 1 FROM owned_functions
             WHERE proname = 'st_geometrytype' AND pronargs = 1
               AND proargtypes[0] = geometry_oid AND prorettype = 25
       )
       AND EXISTS (
            SELECT 1 FROM owned_functions
             WHERE proname = 'st_isempty' AND pronargs = 1
               AND proargtypes[0] = geometry_oid AND prorettype = 16
       )
       AND EXISTS (
            SELECT 1 FROM owned_functions
             WHERE proname = 'st_isvalid' AND pronargs = 1
               AND proargtypes[0] = geometry_oid AND prorettype = 16
       )
       AND EXISTS (
            SELECT 1 FROM owned_functions
             WHERE proname = 'st_ndims' AND pronargs = 1
               AND proargtypes[0] = geometry_oid AND prorettype = 21
       )
       AND EXISTS (
            SELECT 1 FROM owned_functions
             WHERE proname = 'st_transform' AND pronargs = 2
               AND proargtypes[0] = geometry_oid AND proargtypes[1] = 23
               AND prorettype = geometry_oid
       )
       AND EXISTS (
            SELECT 1 FROM owned_functions
             WHERE proname = 'st_x' AND pronargs = 1
               AND proargtypes[0] = geometry_oid AND prorettype = 701
       )
       AND EXISTS (
            SELECT 1 FROM owned_functions
             WHERE proname = 'st_y' AND pronargs = 1
               AND proargtypes[0] = geometry_oid AND prorettype = 701
       )
       AND EXISTS (
            SELECT 1 FROM owned_functions
             WHERE proname = 'box3d' AND pronargs = 1
               AND proargtypes[0] = geometry_oid AND prorettype = box3d_oid
       )
       AND EXISTS (
            SELECT 1 FROM owned_functions
             WHERE proname = 'st_xmin' AND pronargs = 1
               AND proargtypes[0] = box3d_oid AND prorettype = 701
       )
       AND EXISTS (
            SELECT 1 FROM owned_functions
             WHERE proname = 'st_ymin' AND pronargs = 1
               AND proargtypes[0] = box3d_oid AND prorettype = 701
       )
       AND EXISTS (
            SELECT 1 FROM owned_functions
             WHERE proname = 'st_xmax' AND pronargs = 1
               AND proargtypes[0] = box3d_oid AND prorettype = 701
       )
       AND EXISTS (
            SELECT 1 FROM owned_functions
             WHERE proname = 'st_ymax' AND pronargs = 1
               AND proargtypes[0] = box3d_oid AND prorettype = 701
       )
       AND EXISTS (
            SELECT 1 FROM owned_functions
             WHERE proname = 'st_makepoint' AND pronargs = 2
               AND proargtypes[0] = 701 AND proargtypes[1] = 701
               AND prorettype = geometry_oid
       )
       AND EXISTS (
            SELECT 1 FROM owned_functions
             WHERE proname = 'st_setsrid' AND pronargs = 2
               AND proargtypes[0] = geometry_oid AND proargtypes[1] = 23
               AND prorettype = geometry_oid
       )
       AND EXISTS (
            SELECT 1 FROM owned_functions
             WHERE proname = 'st_covers' AND pronargs = 2
               AND proargtypes[0] = geometry_oid AND proargtypes[1] = geometry_oid
               AND prorettype = 16
       ) AS valid
"#;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct TransformedPoint {
    pub(crate) x: f64,
    pub(crate) y: f64,
    pub(crate) source_srid: i32,
    pub(crate) target_srid: i32,
}

#[derive(Debug, Clone, Copy, PartialEq)]
#[allow(dead_code)]
pub(crate) struct GeometryEnvelope {
    pub(crate) xmin: f64,
    pub(crate) ymin: f64,
    pub(crate) xmax: f64,
    pub(crate) ymax: f64,
    pub(crate) source_srid: i32,
    pub(crate) target_srid: i32,
}

/// A validated, query-local reference to the installed PostGIS extension.
#[derive(Debug, Clone)]
pub(crate) struct PostgisCatalog {
    schema: String,
}

#[derive(Debug)]
struct PointDescription {
    source_srid: i32,
    geometry_type: String,
    dimensions: i32,
    empty: bool,
    x: Option<f64>,
    y: Option<f64>,
    transformed_srid: Option<i32>,
}

#[derive(Debug)]
struct PolygonDescription {
    source_srid: i32,
    source_srid_known: bool,
    geometry_type: String,
    dimensions: i32,
    empty: bool,
    valid: bool,
    transformed_srid: Option<i32>,
}

#[derive(Debug)]
struct EnvelopeDescription {
    polygon: PolygonDescription,
    xmin: Option<f64>,
    ymin: Option<f64>,
    xmax: Option<f64>,
    ymax: Option<f64>,
}

impl PostgisCatalog {
    /// Discover and authenticate the installed PostGIS catalog. `None` means
    /// the extension is not installed; a present but incomplete or shadowed
    /// installation is rejected.
    pub(crate) fn discover() -> ZarrFdwResult<Option<Self>> {
        let discovered = Spi::connect(|client| {
            let mut rows = client.select(DISCOVER_POSTGIS_SQL, None, &[])?;
            let Some(row) = rows.next() else {
                return Ok::<_, pgrx::spi::Error>(None);
            };
            Ok::<_, pgrx::spi::Error>(Some((
                row.get_by_name::<i64, _>("extension_oid")?
                    .expect("pg_extension.oid is not null"),
                row.get_by_name::<String, _>("schema_name")?
                    .expect("pg_namespace.nspname is not null"),
            )))
        })?;
        let Some((extension_oid, schema)) = discovered else {
            return Ok(None);
        };

        let members_valid =
            Spi::get_one_with_args::<bool>(VERIFY_POSTGIS_MEMBERS_SQL, &[extension_oid.into()])?
                .unwrap_or(false);
        if !members_valid {
            return Err(ZarrFdwError::PostgisUnavailable(
                "the installed extension does not own the required spatial types, spatial_ref_sys catalog, and geometry functions"
                    .to_string(),
            ));
        }
        Ok(Some(Self { schema }))
    }

    pub(crate) fn require() -> ZarrFdwResult<Self> {
        Self::discover()?.ok_or_else(|| {
            ZarrFdwError::PostgisUnavailable("the postgis extension is not installed".to_string())
        })
    }

    /// Parse one EWKB point, transform it to `target_epsg`, and return only
    /// built-in numeric values. Geometry datums never cross the Rust boundary.
    pub(crate) fn transform_ewkb_point(
        &self,
        ewkb: &[u8],
        array_path: &str,
        target_epsg: i32,
    ) -> ZarrFdwResult<TransformedPoint> {
        validate_ewkb_size(ewkb)?;
        if target_epsg <= 0 {
            return Err(invalid_crs(
                array_path,
                format!("target EPSG code must be positive, got {target_epsg}"),
            ));
        }
        self.require_known_srid(array_path, target_epsg)?;

        let sql = point_transform_sql(&self.schema);
        // PostGIS 3.x is not relocatable and some transformation internals
        // resolve its extension-owned spatial_ref_sys through search_path. Set
        // the catalog-discovered schema only for this transaction-local call,
        // then restore the caller's setting on every ordinary Result path.
        let prior_search_path =
            Spi::get_one::<String>("SELECT pg_catalog.current_setting('search_path')")?
                .unwrap_or_default();
        let operation_search_path = format!("{}, pg_catalog", quote_identifier(&self.schema));
        Spi::get_one_with_args::<String>(
            "SELECT pg_catalog.set_config('search_path', $1, true)",
            &[operation_search_path.into()],
        )?;
        let description = Spi::connect(|client| {
            let mut rows = client
                .select(&sql, Some(1), &[ewkb.to_vec().into(), target_epsg.into()])
                .map_err(|_| {
                    ZarrFdwError::InvalidGeometry(
                        "PostGIS could not parse or transform the supplied EWKB".to_string(),
                    )
                })?;
            let row = rows.next().ok_or_else(|| {
                ZarrFdwError::InvalidGeometry(
                    "PostGIS returned no description for the supplied EWKB".to_string(),
                )
            })?;
            Ok::<_, ZarrFdwError>(PointDescription {
                source_srid: row
                    .get_by_name::<i32, _>("source_srid")?
                    .expect("ST_SRID result is not null"),
                geometry_type: row
                    .get_by_name::<String, _>("geometry_type")?
                    .expect("ST_GeometryType result is not null"),
                dimensions: row
                    .get_by_name::<i32, _>("dimensions")?
                    .expect("ST_NDims result is not null"),
                empty: row
                    .get_by_name::<bool, _>("is_empty")?
                    .expect("ST_IsEmpty result is not null"),
                x: row.get_by_name::<f64, _>("x")?,
                y: row.get_by_name::<f64, _>("y")?,
                transformed_srid: row.get_by_name::<i32, _>("transformed_srid")?,
            })
        });
        let restore = Spi::get_one_with_args::<String>(
            "SELECT pg_catalog.set_config('search_path', $1, true)",
            &[prior_search_path.into()],
        );
        match (description, restore) {
            (Ok(description), Ok(_)) => validate_point_description(description, target_epsg),
            (Err(error), _) => Err(error),
            (Ok(_), Err(error)) => Err(error.into()),
        }
    }

    /// Validate and transform a polygonal EWKB geometry, then return its
    /// finite target-CRS envelope using only built-in Rust scalar types.
    #[allow(dead_code)]
    pub(crate) fn transform_ewkb_geometry_envelope(
        &self,
        ewkb: &[u8],
        array_path: &str,
        target_epsg: i32,
    ) -> ZarrFdwResult<GeometryEnvelope> {
        validate_ewkb_size(ewkb)?;
        if target_epsg <= 0 {
            return Err(invalid_crs(
                array_path,
                format!("target EPSG code must be positive, got {target_epsg}"),
            ));
        }
        self.require_known_srid(array_path, target_epsg)?;

        let sql = polygon_envelope_sql(&self.schema);
        let description = self.with_extension_search_path(|| {
            PgTryBuilder::new(|| {
                Spi::connect(|client| {
                    let mut rows = client.select(
                        &sql,
                        Some(1),
                        &[ewkb.to_vec().into(), target_epsg.into()],
                    )?;
                    let row = rows.next().ok_or_else(|| {
                        ZarrFdwError::InvalidGeometry(
                            "PostGIS returned no description for the supplied polygon".to_string(),
                        )
                    })?;
                    Ok::<_, ZarrFdwError>(EnvelopeDescription {
                        polygon: PolygonDescription {
                            source_srid: row
                                .get_by_name::<i32, _>("source_srid")?
                                .expect("ST_SRID result is not null"),
                            source_srid_known: row
                                .get_by_name::<bool, _>("source_srid_known")?
                                .expect("source SRID catalog check is not null"),
                            geometry_type: row
                                .get_by_name::<String, _>("geometry_type")?
                                .expect("ST_GeometryType result is not null"),
                            dimensions: row
                                .get_by_name::<i32, _>("dimensions")?
                                .expect("ST_NDims result is not null"),
                            empty: row
                                .get_by_name::<bool, _>("is_empty")?
                                .expect("ST_IsEmpty result is not null"),
                            valid: row
                                .get_by_name::<bool, _>("is_valid")?
                                .expect("ST_IsValid result is not null"),
                            transformed_srid: row.get_by_name::<i32, _>("transformed_srid")?,
                        },
                        xmin: row.get_by_name::<f64, _>("xmin")?,
                        ymin: row.get_by_name::<f64, _>("ymin")?,
                        xmax: row.get_by_name::<f64, _>("xmax")?,
                        ymax: row.get_by_name::<f64, _>("ymax")?,
                    })
                })
            })
            // PostGIS reports malformed EWKB and invalid transforms as XX000.
            // Catch only that code so cancellation, permissions, and all other
            // PostgreSQL errors retain their native behavior.
            .catch_when(PgSqlErrorCode::ERRCODE_INTERNAL_ERROR, |_| {
                Err(ZarrFdwError::InvalidGeometry(
                    "PostGIS could not parse or transform the supplied polygon".to_string(),
                ))
            })
            .execute()
        })?;
        validate_envelope_description(description, array_path, target_epsg)
    }

    /// Test a bounded batch of target-CRS cell centers against one polygonal
    /// EWKB geometry. One SPI query evaluates the whole batch in input order.
    #[allow(dead_code)]
    pub(crate) fn covers_ewkb_geometry_points(
        &self,
        ewkb: &[u8],
        array_path: &str,
        target_epsg: i32,
        candidates: &[(f64, f64)],
    ) -> ZarrFdwResult<Vec<bool>> {
        validate_ewkb_size(ewkb)?;
        validate_coverage_candidates(candidates)?;
        if target_epsg <= 0 {
            return Err(invalid_crs(
                array_path,
                format!("target EPSG code must be positive, got {target_epsg}"),
            ));
        }
        self.require_known_srid(array_path, target_epsg)?;
        let candidate_json = serde_json::to_string(candidates).map_err(|_| {
            ZarrFdwError::InvalidGeometry(
                "could not encode polygon coverage candidates".to_string(),
            )
        })?;

        let sql = polygon_coverage_sql(&self.schema);
        let (description, coverage_json) = self.with_extension_search_path(|| {
            PgTryBuilder::new(|| {
                Spi::connect(|client| {
                    let mut rows = client.select(
                        &sql,
                        Some(1),
                        &[
                            ewkb.to_vec().into(),
                            target_epsg.into(),
                            candidate_json.into(),
                        ],
                    )?;
                    let row = rows.next().ok_or_else(|| {
                        ZarrFdwError::InvalidGeometry(
                            "PostGIS returned no coverage result for the supplied polygon"
                                .to_string(),
                        )
                    })?;
                    let description = PolygonDescription {
                        source_srid: row
                            .get_by_name::<i32, _>("source_srid")?
                            .expect("ST_SRID result is not null"),
                        source_srid_known: row
                            .get_by_name::<bool, _>("source_srid_known")?
                            .expect("source SRID catalog check is not null"),
                        geometry_type: row
                            .get_by_name::<String, _>("geometry_type")?
                            .expect("ST_GeometryType result is not null"),
                        dimensions: row
                            .get_by_name::<i32, _>("dimensions")?
                            .expect("ST_NDims result is not null"),
                        empty: row
                            .get_by_name::<bool, _>("is_empty")?
                            .expect("ST_IsEmpty result is not null"),
                        valid: row
                            .get_by_name::<bool, _>("is_valid")?
                            .expect("ST_IsValid result is not null"),
                        transformed_srid: row.get_by_name::<i32, _>("transformed_srid")?,
                    };
                    let coverage_json = row
                        .get_by_name::<String, _>("coverage_json")?
                        .expect("coverage JSON result is not null");
                    Ok::<_, ZarrFdwError>((description, coverage_json))
                })
            })
            .catch_when(PgSqlErrorCode::ERRCODE_INTERNAL_ERROR, |_| {
                Err(ZarrFdwError::InvalidGeometry(
                    "PostGIS could not parse, transform, or mask the supplied polygon".to_string(),
                ))
            })
            .execute()
        })?;
        validate_polygon_description(&description, array_path, target_epsg)?;
        parse_coverage_json(&coverage_json, candidates.len())
    }

    fn with_extension_search_path<T>(
        &self,
        operation: impl FnOnce() -> ZarrFdwResult<T>,
    ) -> ZarrFdwResult<T> {
        let prior_search_path =
            Spi::get_one::<String>("SELECT pg_catalog.current_setting('search_path')")?
                .unwrap_or_default();
        let operation_search_path = format!("{}, pg_catalog", quote_identifier(&self.schema));
        Spi::get_one_with_args::<String>(
            "SELECT pg_catalog.set_config('search_path', $1, true)",
            &[operation_search_path.into()],
        )?;
        let result = operation();
        let restore = Spi::get_one_with_args::<String>(
            "SELECT pg_catalog.set_config('search_path', $1, true)",
            &[prior_search_path.into()],
        );
        match (result, restore) {
            (Ok(value), Ok(_)) => Ok(value),
            (Err(error), _) => Err(error),
            (Ok(_), Err(error)) => Err(error.into()),
        }
    }

    fn require_known_srid(&self, array_path: &str, epsg: i32) -> ZarrFdwResult<()> {
        let schema = quote_identifier(&self.schema);
        let sql = format!("SELECT EXISTS (SELECT 1 FROM {schema}.spatial_ref_sys WHERE srid = $1)");
        let exists = Spi::get_one_with_args::<bool>(&sql, &[epsg.into()])?.unwrap_or(false);
        if !exists {
            return Err(invalid_crs(
                array_path,
                format!("EPSG:{epsg} is not present in the installed PostGIS spatial_ref_sys"),
            ));
        }
        Ok(())
    }
}

fn polygon_transform_ctes(schema: &str) -> String {
    let schema = quote_identifier(schema);
    format!(
        r#"
        WITH parsed AS MATERIALIZED (
            SELECT {schema}.ST_GeomFromEWKB($1) AS geometry
        ),
        described AS MATERIALIZED (
            SELECT geometry,
                   {schema}.ST_SRID(geometry) AS source_srid,
                   EXISTS (
                       SELECT 1
                         FROM {schema}.spatial_ref_sys AS source_crs
                        WHERE source_crs.srid = {schema}.ST_SRID(geometry)
                   ) AS source_srid_known,
                   {schema}.ST_GeometryType(geometry) AS geometry_type,
                   {schema}.ST_NDims(geometry)::integer AS dimensions,
                   {schema}.ST_IsEmpty(geometry) AS is_empty,
                   {schema}.ST_IsValid(geometry) AS is_valid
              FROM parsed
        ),
        transformed AS MATERIALIZED (
            SELECT source_srid, source_srid_known, geometry_type, dimensions, is_empty, is_valid,
                   CASE
                     WHEN source_srid > 0
                      AND source_srid_known
                      AND geometry_type IN ('ST_Polygon', 'ST_MultiPolygon')
                      AND dimensions = 2
                      AND NOT is_empty
                      AND is_valid
                     THEN {schema}.ST_Transform(geometry, $2)
                   END AS geometry
              FROM described
        )
        "#
    )
}

fn polygon_envelope_sql(schema: &str) -> String {
    let schema_identifier = quote_identifier(schema);
    let transform = polygon_transform_ctes(schema);
    format!(
        r#"
        {transform},
        bounded AS (
            SELECT source_srid, source_srid_known, geometry_type, dimensions, is_empty, is_valid, geometry,
                   CASE WHEN geometry IS NOT NULL
                        THEN {schema_identifier}.Box3D(geometry)
                   END AS bounds
              FROM transformed
        )
        SELECT source_srid, source_srid_known, geometry_type, dimensions, is_empty, is_valid,
               CASE WHEN geometry IS NOT NULL
                    THEN {schema_identifier}.ST_SRID(geometry)
               END AS transformed_srid,
               CASE WHEN bounds IS NOT NULL
                    THEN {schema_identifier}.ST_XMin(bounds)
               END AS xmin,
               CASE WHEN bounds IS NOT NULL
                    THEN {schema_identifier}.ST_YMin(bounds)
               END AS ymin,
               CASE WHEN bounds IS NOT NULL
                    THEN {schema_identifier}.ST_XMax(bounds)
               END AS xmax,
               CASE WHEN bounds IS NOT NULL
                    THEN {schema_identifier}.ST_YMax(bounds)
               END AS ymax
          FROM bounded
        "#
    )
}

fn polygon_coverage_sql(schema: &str) -> String {
    let schema_identifier = quote_identifier(schema);
    let transform = polygon_transform_ctes(schema);
    format!(
        r#"
        {transform},
        candidate_points AS (
            SELECT candidate.ordinality,
                   pg_catalog.jsonb_extract_path_text(candidate.value, '0')::pg_catalog.float8 AS x,
                   pg_catalog.jsonb_extract_path_text(candidate.value, '1')::pg_catalog.float8 AS y
              FROM pg_catalog.jsonb_array_elements($3::pg_catalog.jsonb)
                   WITH ORDINALITY AS candidate(value, ordinality)
        ),
        coverage AS (
            SELECT candidate.ordinality,
                   CASE WHEN transformed.geometry IS NOT NULL THEN
                       {schema_identifier}.ST_Covers(
                           transformed.geometry,
                           {schema_identifier}.ST_SetSRID(
                               {schema_identifier}.ST_MakePoint(candidate.x, candidate.y),
                               $2
                           )
                       )
                   END AS is_covered
              FROM transformed
              CROSS JOIN candidate_points AS candidate
        )
        SELECT transformed.source_srid,
               transformed.source_srid_known,
               transformed.geometry_type,
               transformed.dimensions,
               transformed.is_empty,
               transformed.is_valid,
               CASE WHEN transformed.geometry IS NOT NULL
                    THEN {schema_identifier}.ST_SRID(transformed.geometry)
               END AS transformed_srid,
               COALESCE(
                   (
                       SELECT pg_catalog.jsonb_agg(
                                  coverage.is_covered ORDER BY coverage.ordinality
                              )
                         FROM coverage
                   ),
                   '[]'::pg_catalog.jsonb
               )::pg_catalog.text AS coverage_json
          FROM transformed
        "#
    )
}

fn point_transform_sql(schema: &str) -> String {
    let schema = quote_identifier(schema);
    format!(
        r#"
        WITH parsed AS (
            SELECT {schema}.ST_GeomFromEWKB($1) AS geometry
        ),
        described AS (
            SELECT geometry,
                   {schema}.ST_SRID(geometry) AS source_srid,
                   {schema}.ST_GeometryType(geometry) AS geometry_type,
                   {schema}.ST_NDims(geometry)::integer AS dimensions,
                   {schema}.ST_IsEmpty(geometry) AS is_empty
              FROM parsed
        ),
        transformed AS (
            SELECT source_srid, geometry_type, dimensions, is_empty,
                   CASE
                     WHEN source_srid > 0
                      AND geometry_type = 'ST_Point'
                      AND dimensions = 2
                      AND NOT is_empty
                     THEN {schema}.ST_Transform(geometry, $2)
                   END AS geometry
              FROM described
        )
        SELECT source_srid, geometry_type, dimensions, is_empty,
               CASE WHEN geometry IS NOT NULL THEN {schema}.ST_X(geometry) END AS x,
               CASE WHEN geometry IS NOT NULL THEN {schema}.ST_Y(geometry) END AS y,
               CASE WHEN geometry IS NOT NULL THEN {schema}.ST_SRID(geometry) END AS transformed_srid
          FROM transformed
        "#
    )
}

fn validate_point_description(
    point: PointDescription,
    target_srid: i32,
) -> ZarrFdwResult<TransformedPoint> {
    if point.source_srid <= 0 {
        return Err(ZarrFdwError::InvalidGeometry(
            "EWKB point must declare a positive SRID".to_string(),
        ));
    }
    if point.geometry_type != "ST_Point" {
        return Err(ZarrFdwError::InvalidGeometry(format!(
            "expected a Point, got {}",
            point.geometry_type
        )));
    }
    if point.dimensions != 2 {
        return Err(ZarrFdwError::InvalidGeometry(format!(
            "expected a two-dimensional Point, got {} dimensions",
            point.dimensions
        )));
    }
    if point.empty {
        return Err(ZarrFdwError::InvalidGeometry(
            "point must not be empty".to_string(),
        ));
    }
    let (Some(x), Some(y), Some(transformed_srid)) = (point.x, point.y, point.transformed_srid)
    else {
        return Err(ZarrFdwError::InvalidGeometry(
            "PostGIS did not return transformed point coordinates".to_string(),
        ));
    };
    if !x.is_finite() || !y.is_finite() {
        return Err(ZarrFdwError::InvalidGeometry(
            "transformed point coordinates must be finite".to_string(),
        ));
    }
    if transformed_srid != target_srid {
        return Err(ZarrFdwError::InvalidGeometry(format!(
            "PostGIS returned SRID {transformed_srid}, expected {target_srid}"
        )));
    }
    Ok(TransformedPoint {
        x,
        y,
        source_srid: point.source_srid,
        target_srid,
    })
}

fn validate_ewkb_size(ewkb: &[u8]) -> ZarrFdwResult<()> {
    if ewkb.len() > MAX_EWKB_BYTES {
        return Err(ZarrFdwError::InvalidGeometry(format!(
            "EWKB input is {} bytes, exceeding the {MAX_EWKB_BYTES}-byte limit",
            ewkb.len()
        )));
    }
    Ok(())
}

fn validate_coverage_candidates(candidates: &[(f64, f64)]) -> ZarrFdwResult<()> {
    if candidates.len() > MAX_COVERAGE_CANDIDATES {
        return Err(ZarrFdwError::InvalidGeometry(format!(
            "polygon coverage batch contains {} candidates, exceeding the {MAX_COVERAGE_CANDIDATES}-candidate limit",
            candidates.len()
        )));
    }
    if let Some((index, (x, y))) = candidates
        .iter()
        .enumerate()
        .find(|(_, (x, y))| !x.is_finite() || !y.is_finite())
    {
        return Err(ZarrFdwError::InvalidGeometry(format!(
            "polygon coverage candidate {index} must contain finite coordinates, got ({x}, {y})"
        )));
    }
    Ok(())
}

fn validate_polygon_description(
    polygon: &PolygonDescription,
    array_path: &str,
    target_srid: i32,
) -> ZarrFdwResult<()> {
    if polygon.source_srid <= 0 {
        return Err(ZarrFdwError::InvalidGeometry(
            "EWKB polygon must declare a positive SRID".to_string(),
        ));
    }
    if !polygon.source_srid_known {
        return Err(invalid_crs(
            array_path,
            format!(
                "EPSG:{} is not present in the installed PostGIS spatial_ref_sys",
                polygon.source_srid
            ),
        ));
    }
    if !matches!(
        polygon.geometry_type.as_str(),
        "ST_Polygon" | "ST_MultiPolygon"
    ) {
        return Err(ZarrFdwError::InvalidGeometry(format!(
            "expected a Polygon or MultiPolygon, got {}",
            polygon.geometry_type
        )));
    }
    if polygon.dimensions != 2 {
        return Err(ZarrFdwError::InvalidGeometry(format!(
            "expected a two-dimensional polygon, got {} dimensions",
            polygon.dimensions
        )));
    }
    if polygon.empty {
        return Err(ZarrFdwError::InvalidGeometry(
            "polygon must not be empty".to_string(),
        ));
    }
    if !polygon.valid {
        return Err(ZarrFdwError::InvalidGeometry(
            "polygon must be valid according to PostGIS ST_IsValid".to_string(),
        ));
    }
    let Some(transformed_srid) = polygon.transformed_srid else {
        return Err(ZarrFdwError::InvalidGeometry(
            "PostGIS did not return a transformed polygon".to_string(),
        ));
    };
    if transformed_srid != target_srid {
        return Err(ZarrFdwError::InvalidGeometry(format!(
            "PostGIS returned SRID {transformed_srid}, expected {target_srid}"
        )));
    }
    Ok(())
}

fn validate_envelope_description(
    description: EnvelopeDescription,
    array_path: &str,
    target_srid: i32,
) -> ZarrFdwResult<GeometryEnvelope> {
    validate_polygon_description(&description.polygon, array_path, target_srid)?;
    let (Some(xmin), Some(ymin), Some(xmax), Some(ymax)) = (
        description.xmin,
        description.ymin,
        description.xmax,
        description.ymax,
    ) else {
        return Err(ZarrFdwError::InvalidGeometry(
            "PostGIS did not return a polygon envelope".to_string(),
        ));
    };
    if [xmin, ymin, xmax, ymax]
        .iter()
        .any(|value| !value.is_finite())
    {
        return Err(ZarrFdwError::InvalidGeometry(
            "transformed polygon envelope must contain only finite coordinates".to_string(),
        ));
    }
    if xmin > xmax || ymin > ymax {
        return Err(ZarrFdwError::InvalidGeometry(format!(
            "PostGIS returned an invalid polygon envelope ({xmin}, {ymin}, {xmax}, {ymax})"
        )));
    }
    Ok(GeometryEnvelope {
        xmin,
        ymin,
        xmax,
        ymax,
        source_srid: description.polygon.source_srid,
        target_srid,
    })
}

fn parse_coverage_json(value: &str, expected_len: usize) -> ZarrFdwResult<Vec<bool>> {
    let coverage = serde_json::from_str::<Vec<bool>>(value).map_err(|_| {
        ZarrFdwError::InvalidGeometry(
            "PostGIS returned malformed polygon coverage results".to_string(),
        )
    })?;
    if coverage.len() != expected_len {
        return Err(ZarrFdwError::InvalidGeometry(format!(
            "PostGIS returned {} polygon coverage results for {expected_len} candidates",
            coverage.len()
        )));
    }
    Ok(coverage)
}

fn invalid_crs(array: &str, message: impl Into<String>) -> ZarrFdwError {
    ZarrFdwError::InvalidCrs {
        array: array.to_string(),
        message: message.into(),
    }
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    fn point() -> PointDescription {
        PointDescription {
            source_srid: 4326,
            geometry_type: "ST_Point".to_string(),
            dimensions: 2,
            empty: false,
            x: Some(100.0),
            y: Some(20.0),
            transformed_srid: Some(3857),
        }
    }

    fn polygon() -> PolygonDescription {
        PolygonDescription {
            source_srid: 4326,
            source_srid_known: true,
            geometry_type: "ST_Polygon".to_string(),
            dimensions: 2,
            empty: false,
            valid: true,
            transformed_srid: Some(3857),
        }
    }

    fn envelope() -> EnvelopeDescription {
        EnvelopeDescription {
            polygon: polygon(),
            xmin: Some(10.0),
            ymin: Some(20.0),
            xmax: Some(30.0),
            ymax: Some(40.0),
        }
    }

    #[test]
    fn validates_transformed_point_description() {
        assert_eq!(
            validate_point_description(point(), 3857).unwrap(),
            TransformedPoint {
                x: 100.0,
                y: 20.0,
                source_srid: 4326,
                target_srid: 3857,
            }
        );

        let mut invalid = point();
        invalid.source_srid = 0;
        assert!(validate_point_description(invalid, 3857).is_err());

        let mut invalid = point();
        invalid.geometry_type = "ST_LineString".to_string();
        assert!(validate_point_description(invalid, 3857).is_err());

        let mut invalid = point();
        invalid.dimensions = 3;
        assert!(validate_point_description(invalid, 3857).is_err());

        let mut invalid = point();
        invalid.empty = true;
        assert!(validate_point_description(invalid, 3857).is_err());

        let mut invalid = point();
        invalid.x = Some(f64::NAN);
        assert!(validate_point_description(invalid, 3857).is_err());

        let mut invalid = point();
        invalid.transformed_srid = Some(4326);
        assert!(validate_point_description(invalid, 3857).is_err());
    }

    #[test]
    fn validates_polygon_contract_and_finite_envelope() {
        assert_eq!(
            validate_envelope_description(envelope(), "nested/spatial2d", 3857).unwrap(),
            GeometryEnvelope {
                xmin: 10.0,
                ymin: 20.0,
                xmax: 30.0,
                ymax: 40.0,
                source_srid: 4326,
                target_srid: 3857,
            }
        );

        let mut invalid = polygon();
        invalid.source_srid = 0;
        assert!(validate_polygon_description(&invalid, "array", 3857).is_err());

        let mut invalid = polygon();
        invalid.source_srid_known = false;
        assert!(matches!(
            validate_polygon_description(&invalid, "array", 3857),
            Err(ZarrFdwError::InvalidCrs { .. })
        ));

        let mut invalid = polygon();
        invalid.geometry_type = "ST_LineString".to_string();
        assert!(validate_polygon_description(&invalid, "array", 3857).is_err());

        let mut multipolygon = polygon();
        multipolygon.geometry_type = "ST_MultiPolygon".to_string();
        assert!(validate_polygon_description(&multipolygon, "array", 3857).is_ok());

        let mut invalid = polygon();
        invalid.dimensions = 3;
        assert!(validate_polygon_description(&invalid, "array", 3857).is_err());

        let mut invalid = polygon();
        invalid.empty = true;
        assert!(validate_polygon_description(&invalid, "array", 3857).is_err());

        let mut invalid = polygon();
        invalid.valid = false;
        assert!(validate_polygon_description(&invalid, "array", 3857).is_err());

        let mut invalid = polygon();
        invalid.transformed_srid = Some(4326);
        assert!(validate_polygon_description(&invalid, "array", 3857).is_err());

        let mut invalid = envelope();
        invalid.xmin = Some(f64::NAN);
        assert!(validate_envelope_description(invalid, "array", 3857).is_err());

        let mut invalid = envelope();
        invalid.xmin = Some(31.0);
        assert!(validate_envelope_description(invalid, "array", 3857).is_err());
    }

    #[test]
    fn bounds_and_validates_coverage_candidates_and_results() {
        assert!(validate_coverage_candidates(&[]).is_ok());
        assert!(validate_coverage_candidates(&[(1.0, 2.0)]).is_ok());
        assert!(validate_coverage_candidates(&[(f64::INFINITY, 2.0)]).is_err());
        assert!(
            validate_coverage_candidates(&vec![(0.0, 0.0); MAX_COVERAGE_CANDIDATES + 1]).is_err()
        );

        assert_eq!(
            parse_coverage_json("[true,false,true]", 3).unwrap(),
            vec![true, false, true]
        );
        assert!(parse_coverage_json("[true]", 2).is_err());
        assert!(parse_coverage_json("[true,null]", 2).is_err());
    }
}

// SQL construction quotes identifiers with PostgreSQL's own routine, so these
// checks run in a backend rather than initializing pgrx from a Rust test thread.
#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use super::*;
    use pgrx::pg_test;

    #[pg_test]
    fn point_sql_is_qualified_and_parameterized() {
        let sql = point_transform_sql("post\"gis");
        assert!(sql.contains("\"post\"\"gis\".ST_GeomFromEWKB($1)"));
        assert!(sql.contains("\"post\"\"gis\".ST_Transform(geometry, $2)"));
        assert!(!sql.contains("4326"));
        assert!(!sql.contains("3857"));
    }

    #[pg_test]
    fn polygon_sql_is_qualified_parameterized_and_batched() {
        let envelope_sql = polygon_envelope_sql("post\"gis");
        assert!(envelope_sql.contains("\"post\"\"gis\".ST_IsValid(geometry)"));
        assert!(envelope_sql.contains("\"post\"\"gis\".Box3D(geometry)"));
        assert!(envelope_sql.contains("\"post\"\"gis\".ST_XMin(bounds)"));
        assert!(envelope_sql.contains("ST_Transform(geometry, $2)"));

        let coverage_sql = polygon_coverage_sql("post\"gis");
        assert!(coverage_sql.contains("jsonb_array_elements($3::pg_catalog.jsonb)"));
        assert!(coverage_sql.contains("\"post\"\"gis\".ST_Covers("));
        assert!(coverage_sql.contains("\"post\"\"gis\".ST_MakePoint("));
        assert!(coverage_sql.contains("ORDER BY coverage.ordinality"));
        assert!(!coverage_sql.contains("4326"));
        assert!(!coverage_sql.contains("3857"));

        for function in [
            "st_isvalid",
            "box3d",
            "st_xmin",
            "st_ymin",
            "st_xmax",
            "st_ymax",
            "st_makepoint",
            "st_setsrid",
            "st_covers",
        ] {
            assert!(VERIFY_POSTGIS_MEMBERS_SQL.contains(&format!("proname = '{function}'")));
        }
    }
}
