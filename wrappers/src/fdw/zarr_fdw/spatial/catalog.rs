//! Security-invoker catalog loading for spatial operations over a Zarr table.

use std::collections::HashMap;

use pgrx::{pg_sys, prelude::*};
use supabase_wrappers::prelude::{Column, ForeignServer};

use super::super::{ZarrFdwError, ZarrFdwResult};

#[derive(Debug)]
pub(crate) struct SpatialForeignTable {
    pub(crate) server: ForeignServer,
    pub(crate) options: HashMap<String, String>,
    pub(crate) columns: Vec<Column>,
}

/// Resolve a caller-visible relation and load only the catalog state needed to
/// construct a fresh Zarr executor. The privilege predicates deliberately make
/// missing and inaccessible relations indistinguishable.
pub(crate) fn load_zarr_foreign_table(relation_name: &str) -> ZarrFdwResult<SpatialForeignTable> {
    let (table_oid, server_oid, server_name, server_type, server_version) = Spi::connect(
        |client| {
            let mut rows = client.select(
                "SELECT c.oid::bigint AS table_oid,
                        s.oid::bigint AS server_oid,
                        s.srvname::text AS server_name,
                        s.srvtype::text AS server_type,
                        s.srvversion::text AS server_version
                   FROM pg_catalog.pg_class AS c
                   JOIN pg_catalog.pg_foreign_table AS ft ON ft.ftrelid = c.oid
                   JOIN pg_catalog.pg_foreign_server AS s ON s.oid = ft.ftserver
                   JOIN pg_catalog.pg_foreign_data_wrapper AS w ON w.oid = s.srvfdw
                   JOIN pg_catalog.pg_proc AS handler ON handler.oid = w.fdwhandler
                  WHERE c.oid = pg_catalog.to_regclass($1)
                    AND pg_catalog.has_table_privilege(c.oid, 'SELECT')
                    AND pg_catalog.has_server_privilege(s.oid, 'USAGE')
                    AND handler.proname = 'zarr_fdw_handler'
                    AND EXISTS (
                          SELECT 1
                            FROM pg_catalog.pg_depend AS dependency
                            JOIN pg_catalog.pg_extension AS extension
                              ON extension.oid = dependency.refobjid
                           WHERE dependency.classid = 'pg_catalog.pg_proc'::pg_catalog.regclass
                             AND dependency.objid = handler.oid
                             AND dependency.refclassid = 'pg_catalog.pg_extension'::pg_catalog.regclass
                             AND dependency.deptype = 'e'
                             AND extension.extname = 'wrappers'
                        )",
                Some(1),
                &[relation_name.into()],
            )?;
            let Some(row) = rows.next() else {
                return Ok::<_, pgrx::spi::Error>((None, None, None, None, None));
            };
            Ok((
                row.get_by_name::<i64, _>("table_oid")?,
                row.get_by_name::<i64, _>("server_oid")?,
                row.get_by_name::<String, _>("server_name")?,
                row.get_by_name::<String, _>("server_type")?,
                row.get_by_name::<String, _>("server_version")?,
            ))
        },
    )?;

    let (table_oid, server_oid, server_name) = match (table_oid, server_oid, server_name) {
        (Some(table_oid), Some(server_oid), Some(server_name)) => {
            (table_oid, server_oid, server_name)
        }
        _ => {
            return Err(ZarrFdwError::InvalidMetadata(format!(
                "foreign table '{relation_name}' does not exist or is not accessible"
            )));
        }
    };

    let table_options = load_options(
        "SELECT option.option_name::text, option.option_value::text
           FROM pg_catalog.pg_foreign_table AS table_catalog
           CROSS JOIN LATERAL pg_catalog.pg_options_to_table(table_catalog.ftoptions) AS option
          WHERE table_catalog.ftrelid = $1::bigint::pg_catalog.oid",
        table_oid,
    )?;
    let server_options = load_options(
        "SELECT option.option_name::text, option.option_value::text
           FROM pg_catalog.pg_foreign_server AS server_catalog
           CROSS JOIN LATERAL pg_catalog.pg_options_to_table(server_catalog.srvoptions) AS option
          WHERE server_catalog.oid = $1::bigint::pg_catalog.oid",
        server_oid,
    )?;
    let columns = load_columns(table_oid)?;
    if columns.is_empty() {
        return Err(ZarrFdwError::InvalidMetadata(format!(
            "foreign table '{relation_name}' has no readable columns"
        )));
    }

    let server_oid = u32::try_from(server_oid).map_err(|_| {
        ZarrFdwError::InvalidMetadata("foreign server OID is out of range".to_string())
    })?;
    Ok(SpatialForeignTable {
        server: ForeignServer {
            server_oid: pg_sys::Oid::from_u32(server_oid),
            server_name,
            server_type,
            server_version,
            options: server_options,
        },
        options: table_options,
        columns,
    })
}

fn load_options(sql: &str, owner_oid: i64) -> ZarrFdwResult<HashMap<String, String>> {
    Spi::connect(|client| {
        let rows = client.select(sql, None, &[owner_oid.into()])?;
        let mut options = HashMap::new();
        for row in rows {
            if let (Some(name), Some(value)) = (row.get::<String>(1)?, row.get::<String>(2)?) {
                options.insert(name, value);
            }
        }
        Ok::<_, pgrx::spi::Error>(options)
    })
    .map_err(Into::into)
}

fn load_columns(table_oid: i64) -> ZarrFdwResult<Vec<Column>> {
    Spi::connect(|client| {
        let rows = client.select(
            "SELECT attribute.attname::text AS name,
                    attribute.attnum::integer AS number,
                    attribute.atttypid::bigint AS type_oid
               FROM pg_catalog.pg_attribute AS attribute
              WHERE attribute.attrelid = $1::bigint::pg_catalog.oid
                AND attribute.attnum > 0
                AND NOT attribute.attisdropped
              ORDER BY attribute.attnum",
            None,
            &[table_oid.into()],
        )?;
        let mut columns = Vec::new();
        for row in rows {
            let Some(name) = row.get_by_name::<String, _>("name")? else {
                continue;
            };
            let number = row
                .get_by_name::<i32, _>("number")?
                .ok_or(pgrx::spi::Error::InvalidPosition)?;
            let type_oid = row
                .get_by_name::<i64, _>("type_oid")?
                .ok_or(pgrx::spi::Error::InvalidPosition)?;
            let number = usize::try_from(number).map_err(|_| pgrx::spi::Error::InvalidPosition)?;
            let type_oid =
                u32::try_from(type_oid).map_err(|_| pgrx::spi::Error::InvalidPosition)?;
            columns.push(Column {
                name,
                num: number,
                type_oid: pg_sys::Oid::from_u32(type_oid),
            });
        }
        Ok::<_, pgrx::spi::Error>(columns)
    })
    .map_err(Into::into)
}
