use crate::stats;
use bson::Document;
use mongodb::{Client, Cursor};
use pgrx::pg_sys;
use std::collections::HashMap;

use supabase_wrappers::prelude::*;

use super::{MongodbFdwError, MongodbFdwResult};

/// Cached state from begin_scan so re_scan can replay without re-deparsing.
#[derive(Clone, Default)]
struct ScanState {
    database: String,
    collection: String,
    filter: Document,
    sort: Option<Document>,
    limit: Option<i64>,
    projection: Option<Document>,
}

#[wrappers_fdw(
    version = "0.1.0",
    author = "Supabase",
    website = "https://github.com/supabase/wrappers/tree/main/wrappers/src/fdw/mongodb_fdw",
    error_type = "MongodbFdwError"
)]
pub(crate) struct MongodbFdw {
    rt: Runtime,
    client: Option<Client>,
    tgt_cols: Vec<Column>,
    rowid_col: String,
    scan_state: Option<ScanState>,
    cursor: Option<Cursor<Document>>,
    scanned_row_cnt: usize,
}

impl MongodbFdw {
    const FDW_NAME: &'static str = "MongodbFdw";
}

impl ForeignDataWrapper<MongodbFdwError> for MongodbFdw {
    fn new(server: ForeignServer) -> MongodbFdwResult<Self> {
        let rt = create_async_runtime()?;
        let conn_str = match server.options.get("conn_string") {
            Some(s) => s.to_owned(),
            None => {
                let id = require_option("conn_string_id", &server.options)?;
                get_vault_secret(id)
                    .ok_or_else(|| MongodbFdwError::VaultSecretNotFound(id.to_string()))?
            }
        };

        let client = rt.block_on(async { Client::with_uri_str(&conn_str).await })?;

        stats::inc_stats(Self::FDW_NAME, stats::Metric::CreateTimes, 1);

        Ok(MongodbFdw {
            rt,
            client: Some(client),
            tgt_cols: Vec::new(),
            rowid_col: String::default(),
            scan_state: None,
            cursor: None,
            scanned_row_cnt: 0,
        })
    }

    fn begin_scan(
        &mut self,
        _quals: &[Qual],
        _columns: &[Column],
        _sorts: &[Sort],
        _limit: &Option<Limit>,
        _options: &HashMap<String, String>,
    ) -> MongodbFdwResult<()> {
        // Filled in by Task 6.
        Ok(())
    }

    fn iter_scan(&mut self, _row: &mut Row) -> MongodbFdwResult<Option<()>> {
        // Filled in by Task 7.
        Ok(None)
    }

    fn end_scan(&mut self) -> MongodbFdwResult<()> {
        self.cursor = None;
        self.scan_state = None;
        Ok(())
    }

    fn validator(
        options: Vec<Option<String>>,
        catalog: Option<pg_sys::Oid>,
    ) -> MongodbFdwResult<()> {
        if let Some(oid) = catalog
            && oid == FOREIGN_TABLE_RELATION_ID
        {
            check_options_contain(&options, "database")?;
            check_options_contain(&options, "collection")?;
        }
        Ok(())
    }
}
