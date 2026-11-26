#[allow(warnings)]
mod bindings;

use bindings::{
    exports::supabase::wrappers::routines::Guest,
    supabase::wrappers::{
        http, stats,
        types::{
            Cell, Column, Context, FdwError, FdwResult, ImportForeignSchemaStmt, OptionsType, Row,
            TypeOid,
        },
        utils,
    },
};

use serde_json::Value as JsonValue;

#[derive(Debug, Default)]
struct GsheetsFdw {
    // Google API key, from the server options or Vault
    api_key: String,
    // spreadsheet and sheet (tab) of the foreign table being scanned
    spreadsheet_id: String,
    sheet: String,
    // rows requested per batch, 0 means the whole sheet is fetched at once
    batch_size: usize,
    // number of sheet rows read so far, header included, where the next batch starts
    rows_read: usize,
    // foreign table column position to column index in the sheet
    col_idx: Vec<usize>,
    // rows of the current batch, or of the whole sheet if batching is not used
    src_rows: Vec<JsonValue>,
    // index of the next row to return in `src_rows`
    src_idx: usize,
}

const API: &str = "https://sheets.googleapis.com/v4/spreadsheets";

// Sheet serial number to microseconds since Unix epoch
fn serial_to_unix_us(serial: f64) -> i64 {
    // Sheets stores dates as days since 1899-12-30
    const UNIX_EPOCH_SERIAL: f64 = 25569.0;
    const MICROSEC_PER_DAY: f64 = 86_400.0 * 1_000_000.0;
    ((serial - UNIX_EPOCH_SERIAL) * MICROSEC_PER_DAY).round() as i64
}

static mut INSTANCE: *mut GsheetsFdw = std::ptr::null_mut::<GsheetsFdw>();
static FDW_NAME: &str = "GsheetsFdw";

impl GsheetsFdw {
    fn init() {
        let instance = Self::default();
        unsafe {
            INSTANCE = Box::leak(Box::new(instance));
        }
    }

    fn this_mut() -> Result<&'static mut Self, FdwError> {
        unsafe {
            if INSTANCE.is_null() {
                return Err("GsheetsFdw is not initialized".to_string());
            }
            Ok(&mut (*INSTANCE))
        }
    }

    fn make_request(&self, req: &http::Request) -> Result<JsonValue, FdwError> {
        let resp = http::get(req)?;
        http::error_for_status(&resp).map_err(|err| format!("{}: {}", err, resp.body))?;
        stats::inc_stats(FDW_NAME, stats::Metric::BytesIn, resp.body.len() as i64);
        serde_json::from_str(&resp.body).map_err(|e| e.to_string())
    }

    fn normalize_column_name(s: &str) -> String {
        s.trim()
            .to_lowercase()
            .chars()
            .map(|c| if c.is_alphanumeric() { c } else { '_' })
            .collect()
    }

    // We expect the first row of the sheet to contain column names
    fn map_columns(&mut self, ctx: &Context) -> FdwResult {
        let sheet_columns_row: Option<&Vec<JsonValue>> =
            self.src_rows.first().and_then(|v| v.as_array());
        let mut header: Vec<String> = Vec::new();
        if let Some(sheet_columns_row) = sheet_columns_row {
            for c in sheet_columns_row {
                let name = match c {
                    JsonValue::String(s) => Self::normalize_column_name(s),
                    other => Self::normalize_column_name(&other.to_string()),
                };
                header.push(name);
            }
        }

        if header.is_empty() {
            return Err(format!("sheet '{}' has no header row", self.sheet));
        }

        // Map table columns to index in sheet
        self.col_idx = ctx
            .get_columns()
            .iter()
            .map(|col| {
                let name = col.name();
                let normalized = Self::normalize_column_name(&name);
                header.iter().position(|h| *h == normalized).ok_or_else(|| {
                    format!(
                        "column '{}' not found in sheet header, available columns: {}",
                        name,
                        header.join(", ")
                    )
                })
            })
            .collect::<Result<_, _>>()?;

        Ok(())
    }

    fn is_batched(&self) -> bool {
        self.batch_size != 0
    }

    fn fetch_source_data(&mut self) -> FdwResult {
        self.src_rows.clear();
        self.src_idx = 0;

        let range = match self.is_batched() {
            false => {
                urlencoding::encode(&format!("'{}'", self.sheet.replace('\'', "''"))).into_owned()
            }
            true => urlencoding::encode(&format!(
                "'{}'!{}:{}",
                self.sheet.replace('\'', "''"),
                self.rows_read + 1,
                self.rows_read + self.batch_size
            ))
            .into_owned(),
        };

        let url = format!(
            "{}/{}/values/{}?valueRenderOption=UNFORMATTED_VALUE",
            API, self.spreadsheet_id, range
        );

        let req = http::Request {
            method: http::Method::Get,
            url,
            headers: vec![("x-goog-api-key".to_owned(), self.api_key.clone())],
            body: String::default(),
        };

        let json = self.make_request(&req)?;

        self.src_rows = json
            .get("values")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();

        Ok(())
    }

    fn start_scan(&mut self) -> FdwResult {
        self.rows_read = 0;
        self.fetch_source_data()?;
        // We expect that first row is column definition and we need to skip it.
        self.src_idx = 1;
        self.rows_read = 1;
        Ok(())
    }

    fn src_to_cell(src: &JsonValue, tgt_col: &Column) -> Result<Option<Cell>, FdwError> {
        if src.is_null() || src.as_str().is_some_and(|s| s.is_empty()) {
            return Ok(None);
        }

        let tgt_col_name = tgt_col.name();

        // column type mapping
        let cell = match tgt_col.type_oid() {
            TypeOid::Bool => src.as_bool().map(Cell::Bool),
            TypeOid::I32 => src
                .as_i64()
                .and_then(|v| i32::try_from(v).ok())
                .map(Cell::I32),
            TypeOid::I64 => src.as_i64().map(Cell::I64),
            TypeOid::F64 => src.as_f64().map(Cell::F64),
            TypeOid::Numeric => src.as_f64().map(Cell::Numeric),
            TypeOid::String => Some(Cell::String(match src {
                JsonValue::String(s) => s.clone(),
                other => other.to_string(),
            })),
            TypeOid::Date => src
                .as_f64()
                .map(|d| Cell::Date(serial_to_unix_us(d).div_euclid(1_000_000))),
            TypeOid::Timestamp => src.as_f64().map(|d| Cell::Timestamp(serial_to_unix_us(d))),
            TypeOid::Timestamptz => src
                .as_f64()
                .map(|d| Cell::Timestamptz(serial_to_unix_us(d))),
            _ => {
                return Err(format!(
                    "target column '{tgt_col_name}' type is not supported"
                ));
            }
        };

        cell.map(Some)
            .ok_or_else(|| format!("column '{tgt_col_name}': invalid value {src}"))
    }
}

impl Guest for GsheetsFdw {
    fn host_version_requirement() -> String {
        // semver ref: https://docs.rs/semver/latest/semver/enum.Op.html
        "^0.1.0".to_string()
    }

    fn init(ctx: &Context) -> FdwResult {
        Self::init();
        let this = Self::this_mut()?;
        let opts = ctx.get_options(&OptionsType::Server);
        this.api_key = match opts.get("api_key") {
            Some(key) => key,
            None => {
                let key_id = opts.require("api_key_id")?;
                utils::get_vault_secret(&key_id)
                    .ok_or_else(|| format!("cannot read vault secret '{key_id}' for api_key_id"))?
            }
        };
        stats::inc_stats(FDW_NAME, stats::Metric::CreateTimes, 1);
        Ok(())
    }

    fn begin_scan(ctx: &Context) -> FdwResult {
        let this = Self::this_mut()?;
        let opts = ctx.get_options(&OptionsType::Table);
        this.spreadsheet_id = opts.require("spreadsheet_id")?;
        this.sheet = opts.require("sheet")?;
        this.batch_size = match opts.get("batch_size") {
            None => 0,
            Some(v) => v
                .parse::<usize>()
                .map_err(|_| format!("invalid batch_size '{v}'"))?,
        };
        this.start_scan()?;
        this.map_columns(ctx)?;
        Ok(())
    }

    fn iter_scan(ctx: &Context, row: &Row) -> Result<Option<u32>, FdwError> {
        let this = Self::this_mut()?;

        if this.src_idx >= this.src_rows.len() {
            // If batching is not used or last batch has fewer than batch_size rows
            // we can exit scan.
            if !this.is_batched() || this.src_rows.len() < this.batch_size {
                return Ok(None);
            }
            // Request next batch.
            this.fetch_source_data()?;
            if this.src_rows.is_empty() {
                return Ok(None);
            }
        }

        let src_row = this.src_rows[this.src_idx]
            .as_array()
            .cloned()
            .unwrap_or_default();

        this.src_idx += 1;
        this.rows_read += 1;

        for (tgt_col, &idx) in ctx.get_columns().iter().zip(this.col_idx.iter()) {
            // trailing empty cells are omitted by the API, treat them as NULL
            let cell = match src_row.get(idx) {
                Some(src) => Self::src_to_cell(src, tgt_col)
                    .map_err(|e| format!("sheet row {0}, {1}", this.rows_read, e))?,
                None => None,
            };
            row.push(cell.as_ref());
        }

        Ok(Some(0))
    }

    fn re_scan(_ctx: &Context) -> FdwResult {
        let this = Self::this_mut()?;
        // With batching only the last batch is in memory, so the scan has to start over.
        // Otherwise the whole sheet is already fetched, so just reset the counters.
        if this.is_batched() {
            this.start_scan()
        } else {
            this.src_idx = 1;
            this.rows_read = 1;
            Ok(())
        }
    }

    fn end_scan(_ctx: &Context) -> FdwResult {
        let this = Self::this_mut()?;
        this.src_rows.clear();
        Ok(())
    }

    fn begin_modify(_ctx: &Context) -> FdwResult {
        Err("modify on foreign table is not supported".to_owned())
    }

    fn insert(_ctx: &Context, _row: &Row) -> FdwResult {
        Ok(())
    }

    fn update(_ctx: &Context, _rowid: Cell, _row: &Row) -> FdwResult {
        Ok(())
    }

    fn delete(_ctx: &Context, _rowid: Cell) -> FdwResult {
        Ok(())
    }

    fn end_modify(_ctx: &Context) -> FdwResult {
        Ok(())
    }

    fn import_foreign_schema(
        _ctx: &Context,
        _stmt: ImportForeignSchemaStmt,
    ) -> Result<Vec<String>, FdwError> {
        Err("import foreign schema is not supported".to_owned())
    }
}

bindings::export!(GsheetsFdw with_types_in bindings);
