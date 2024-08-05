use pg_sys::AsPgCStr;
use pgrx::pg_sys::panic::ErrorReport;
use pgrx::{debug2, prelude::*, PgList};

use crate::options::options_to_hashmap;
use crate::prelude::ForeignDataWrapper;

#[repr(u32)]
#[derive(Debug, Clone)]
pub enum ListType {
    FdwImportSchemaAll = pgrx::pg_sys::ImportForeignSchemaType_FDW_IMPORT_SCHEMA_ALL,
    FdwImportSchemaLimitTo = pgrx::pg_sys::ImportForeignSchemaType_FDW_IMPORT_SCHEMA_LIMIT_TO,
    FdwImportSchemaExcept = pgrx::pg_sys::ImportForeignSchemaType_FDW_IMPORT_SCHEMA_EXCEPT,
}

#[derive(Debug, Clone)]
pub struct ImportForeignSchemaStmt {
    pub server_name: String,
    pub remote_schema: String,
    pub local_schema: String,
    pub list_type: ListType,
    pub table_list: Vec<String>,
    pub options: std::collections::HashMap<String, String>,
}

#[pg_guard]
pub(super) extern "C" fn import_foreign_schema<E: Into<ErrorReport>, W: ForeignDataWrapper<E>>(
    stmt: *mut pg_sys::ImportForeignSchemaStmt,
    server_oid: pg_sys::Oid,
) -> *mut pg_sys::List {
    debug2!("---> import_foreign_schema");

    let import_foreign_schema_stmt: ImportForeignSchemaStmt;

    unsafe {
        import_foreign_schema_stmt = ImportForeignSchemaStmt {
            server_name: std::ffi::CStr::from_ptr((*stmt).server_name)
                .to_str()
                .unwrap()
                .to_string(),
            remote_schema: std::ffi::CStr::from_ptr((*stmt).remote_schema)
                .to_str()
                .unwrap()
                .to_string(),
            local_schema: std::ffi::CStr::from_ptr((*stmt).local_schema)
                .to_str()
                .unwrap()
                .to_string(),

            list_type: match (*stmt).list_type {
                pgrx::pg_sys::ImportForeignSchemaType_FDW_IMPORT_SCHEMA_ALL => {
                    ListType::FdwImportSchemaAll
                }
                pgrx::pg_sys::ImportForeignSchemaType_FDW_IMPORT_SCHEMA_LIMIT_TO => {
                    ListType::FdwImportSchemaLimitTo
                }
                pgrx::pg_sys::ImportForeignSchemaType_FDW_IMPORT_SCHEMA_EXCEPT => {
                    ListType::FdwImportSchemaExcept
                }
                // This should not happen, it's okay to default to FdwImportSchemaAll
                // because PostgreSQL will filter the list anyway.
                _ => ListType::FdwImportSchemaAll,
            },

            table_list: {
                let tables: PgList<pg_sys::RangeVar> = PgList::from_pg((*stmt).table_list);
                tables
                    .iter_ptr()
                    .map(|item| {
                        std::ffi::CStr::from_ptr(item.as_mut().unwrap().relname)
                            .to_str()
                            .unwrap()
                            .to_string()
                    })
                    .collect()
            },

            options: options_to_hashmap((*stmt).options).unwrap(),
        }
    }

    let mut ret: PgList<i8> = PgList::new();
    for command in W::import_foreign_schema(import_foreign_schema_stmt, server_oid) {
        ret.push(command.as_pg_cstr() as *mut std::os::raw::c_char);
    }

    ret.into_pg()
}
