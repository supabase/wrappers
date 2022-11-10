use gcp_bigquery_client::{
    client_builder::ClientBuilder,
    model::{
        field_type::FieldType, query_request::QueryRequest, query_response::ResultSet,
        table::Table, table_data_insert_all_request::TableDataInsertAllRequest,
        table_field_schema::TableFieldSchema,
    },
    Client,
};
use pgx::log::PgSqlErrorCode;
use pgx::prelude::{Date, Timestamp};
use serde_json::json;
use std::collections::HashMap;
use time::{format_description::well_known::Iso8601, OffsetDateTime, PrimitiveDateTime};

use supabase_wrappers::{
    create_async_runtime, report_error, require_option, wrappers_meta, Cell, ForeignDataWrapper,
    Limit, Qual, Row, Runtime, Sort,
};

macro_rules! field_type_error {
    ($field:ident, $err:ident) => {{
        report_error(
            PgSqlErrorCode::ERRCODE_FDW_INVALID_DATA_TYPE,
            &format!("get field {} failed: {}", &$field.name, $err),
        );
        None
    }};
}

// convert BigQuery field to Cell
fn field_to_cell(rs: &ResultSet, field: &TableFieldSchema) -> Option<Cell> {
    match field.r#type {
        FieldType::Boolean => rs
            .get_bool_by_name(&field.name)
            .unwrap_or_else(|err| field_type_error!(field, err))
            .map(|v| Cell::Bool(v)),
        FieldType::Int64 | FieldType::Integer => rs
            .get_i64_by_name(&field.name)
            .unwrap_or_else(|err| field_type_error!(field, err))
            .map(|v| Cell::I64(v.try_into().unwrap())),
        FieldType::String => rs
            .get_string_by_name(&field.name)
            .unwrap_or_else(|err| field_type_error!(field, err))
            .map(|v| Cell::String(v)),
        FieldType::Date => rs
            .get_string_by_name(&field.name)
            .unwrap_or_else(|err| field_type_error!(field, err))
            .map(|v| {
                let pg_epoch = time::Date::parse("2000-01-01", &Iso8601::DEFAULT).unwrap();
                let dt = time::Date::parse(&v, &Iso8601::DEFAULT).unwrap();
                let days = (dt - pg_epoch).whole_days();
                let dt = Date::from_pg_epoch_days(days.try_into().unwrap());
                Cell::Date(dt)
            }),
        FieldType::Datetime => rs
            .get_string_by_name(&field.name)
            .unwrap_or_else(|err| field_type_error!(field, err))
            .map(|v| {
                let dt = PrimitiveDateTime::parse(&v, &Iso8601::DEFAULT).unwrap();
                let ts = Timestamp::try_from(dt).unwrap();
                Cell::Timestamp(ts)
            }),
        FieldType::Timestamp => rs
            .get_f64_by_name(&field.name)
            .unwrap_or_else(|err| field_type_error!(field, err))
            .map(|v| {
                let dt = OffsetDateTime::from_unix_timestamp_nanos((v * 1e9) as i128).unwrap();
                let ts = Timestamp::try_from(dt).unwrap();
                Cell::Timestamp(ts)
            }),
        _ => {
            report_error(
                PgSqlErrorCode::ERRCODE_FDW_ERROR,
                &format!("field type {:?} not supported", field.r#type),
            );
            None
        }
    }
}

#[wrappers_meta(
    version = "0.1.0",
    author = "Supabase",
    website = "https://github.com/supabase/wrappers/tree/main/wrappers/src/fdw/bigquery_fdw"
)]
pub(crate) struct BigQueryFdw {
    rt: Runtime,
    client: Option<Client>,
    project_id: String,
    dataset_id: String,
    table: String,
    rowid_col: String,
    tgt_cols: Vec<String>,
    scan_result: Option<(Table, ResultSet)>,
}

impl BigQueryFdw {
    pub fn new(options: &HashMap<String, String>) -> Self {
        let mut ret = BigQueryFdw {
            rt: create_async_runtime(),
            client: None,
            project_id: "".to_string(),
            dataset_id: "".to_string(),
            table: "".to_string(),
            rowid_col: "".to_string(),
            tgt_cols: Vec::new(),
            scan_result: None,
        };

        //let sa_key_file = require_option("sa_key_file", options);
        let project_id = require_option("project_id", options);
        let dataset_id = require_option("dataset_id", options);

        //if sa_key_file.is_none() || project_id.is_none() || dataset_id.is_none() {
        if project_id.is_none() || dataset_id.is_none() {
            return ret;
        }

        let sa_key_file = sa_key_file.unwrap();
        ret.project_id = project_id.unwrap();
        ret.dataset_id = dataset_id.unwrap();

        let mut bq_client_builder = ClientBuilder::new();

        let bq_client = match require_option("api_endpoint", options) {
            Some(api_endpoint) => bq_client_builder
                .with_v2_base_url(api_endpoint)
                .build_from_service_account_key_file(&sa_key_file),
            None => bq_client_builder.build_from_service_account_key_file(&sa_key_file),
        };

        ret.client = match ret.rt.block_on(bq_client) {
            Ok(client) => Some(client),
            Err(err) => {
                report_error(
                    PgSqlErrorCode::ERRCODE_FDW_ERROR,
                    &format!("create client failed: {}", err),
                );
                None
            }
        };

        ret
    }

    fn deparse(&self, quals: &Vec<Qual>, columns: &Vec<String>) -> String {
        let tgts = if columns.is_empty() {
            "*".to_string()
        } else {
            columns.join(", ")
        };
        let table = format!("{}.{}.{}", self.project_id, self.dataset_id, self.table,);
        let sql = if quals.is_empty() {
            format!("select {} from {}", tgts, table)
        } else {
            let cond = quals
                .iter()
                .map(|q| q.deparse())
                .collect::<Vec<String>>()
                .join(" and ");
            format!("select {} from {} where {}", tgts, table, cond)
        };
        sql
    }
}

impl ForeignDataWrapper for BigQueryFdw {
    fn get_rel_size(
        &mut self,
        _quals: &Vec<Qual>,
        _columns: &Vec<String>,
        _sorts: &Vec<Sort>,
        _limit: &Option<Limit>,
        _options: &HashMap<String, String>,
    ) -> (i64, i32) {
        (0, 0)
    }

    fn begin_scan(
        &mut self,
        quals: &Vec<Qual>,
        columns: &Vec<String>,
        _sorts: &Vec<Sort>,
        _limit: &Option<Limit>,
        options: &HashMap<String, String>,
    ) {
        let table = require_option("table", options);
        if table.is_none() {
            return;
        }
        self.table = table.unwrap();
        self.tgt_cols = columns.clone();

        let location = options
            .get("location")
            .map(|t| t.to_owned())
            .unwrap_or("US".to_string());

        if let Some(client) = &self.client {
            // get table metadata
            let selected_fields = columns.iter().map(|c| c.as_str()).collect::<Vec<&str>>();
            let tbl = match self.rt.block_on(client.table().get(
                &self.project_id,
                &self.dataset_id,
                &self.table,
                Some(selected_fields),
            )) {
                Ok(tbl) => tbl,
                Err(err) => {
                    report_error(
                        PgSqlErrorCode::ERRCODE_FDW_ERROR,
                        &format!("get table metadata failed: {}", err),
                    );
                    return;
                }
            };

            let sql = self.deparse(quals, columns);
            let mut req = QueryRequest::new(sql);
            req.location = Some(location);

            // execute query on BigQuery
            match self.rt.block_on(client.job().query(&self.project_id, req)) {
                Ok(rs) => {
                    self.scan_result = Some((tbl, rs));
                }
                Err(err) => {
                    self.scan_result = None;
                    report_error(
                        PgSqlErrorCode::ERRCODE_FDW_ERROR,
                        &format!("query failed: {}", err),
                    );
                }
            }
        }
    }

    fn iter_scan(&mut self) -> Option<Row> {
        if let Some((ref tbl, ref mut rs)) = self.scan_result {
            if rs.next_row() {
                if let Some(fields) = &tbl.schema.fields {
                    let mut ret = Row::new();
                    for tgt_col in &self.tgt_cols {
                        let field = fields.iter().find(|&f| &f.name == tgt_col).unwrap();
                        let cell = field_to_cell(rs, field);
                        if cell.is_none() {
                            return None;
                        }
                        ret.push(&field.name, cell);
                    }
                    return Some(ret);
                }
            }
        }
        None
    }

    fn end_scan(&mut self) {
        self.scan_result.take();
    }

    fn begin_modify(&mut self, options: &HashMap<String, String>) {
        let table = require_option("table", options);
        let rowid_col = require_option("rowid_column", options);
        if table.is_none() || rowid_col.is_none() {
            return;
        }
        self.table = table.unwrap();
        self.rowid_col = rowid_col.unwrap();
    }

    fn insert(&mut self, src: &Row) {
        if let Some(ref mut client) = self.client {
            let mut insert_request = TableDataInsertAllRequest::new();
            let mut row_json = json!({});

            for (col_name, cell) in src.iter() {
                if let Some(cell) = cell {
                    match cell {
                        Cell::Bool(v) => row_json[col_name] = json!(v),
                        Cell::I8(v) => row_json[col_name] = json!(v),
                        Cell::I16(v) => row_json[col_name] = json!(v),
                        Cell::I32(v) => row_json[col_name] = json!(v),
                        Cell::I64(v) => row_json[col_name] = json!(v),
                        Cell::F32(v) => row_json[col_name] = json!(v),
                        Cell::F64(v) => row_json[col_name] = json!(v),
                        Cell::String(v) => row_json[col_name] = json!(v),
                        Cell::Date(v) => row_json[col_name] = json!(v),
                        Cell::Timestamp(v) => row_json[col_name] = json!(v),
                        Cell::Json(v) => row_json[col_name] = json!(v),
                    }
                }
            }

            insert_request.add_row(None, row_json).unwrap();

            // execute insert job on BigQuery
            if let Err(err) = self.rt.block_on(client.tabledata().insert_all(
                &self.project_id,
                &self.dataset_id,
                &self.table,
                insert_request,
            )) {
                report_error(
                    PgSqlErrorCode::ERRCODE_FDW_ERROR,
                    &format!("insert failed: {}", err),
                );
            }
        }
    }

    fn update(&mut self, rowid: &Cell, new_row: &Row) {
        if let Some(ref mut client) = self.client {
            let mut sets = Vec::new();
            for (col, cell) in new_row.iter() {
                if col == &self.rowid_col {
                    continue;
                }
                if let Some(cell) = cell {
                    sets.push(format!("{} = {}", col, cell));
                } else {
                    sets.push(format!("{} = null", col));
                }
            }
            let sql = format!(
                "update `{}.{}.{}` set {} where {} = {}",
                self.project_id,
                self.dataset_id,
                self.table,
                sets.join(", "),
                self.rowid_col,
                rowid
            );

            let query_job = client
                .job()
                .query(&self.project_id, QueryRequest::new(&sql));

            // execute update on BigQuery
            if let Err(err) = self.rt.block_on(query_job) {
                report_error(
                    PgSqlErrorCode::ERRCODE_FDW_ERROR,
                    &format!("update failed: {}", err),
                );
            }
        }
    }

    fn delete(&mut self, rowid: &Cell) {
        if let Some(ref mut client) = self.client {
            let sql = format!(
                "delete from `{}.{}.{}` where {} = {}",
                self.project_id, self.dataset_id, self.table, self.rowid_col, rowid
            );

            let query_job = client
                .job()
                .query(&self.project_id, QueryRequest::new(&sql));

            // execute delete on BigQuery
            if let Err(err) = self.rt.block_on(query_job) {
                report_error(
                    PgSqlErrorCode::ERRCODE_FDW_ERROR,
                    &format!("update failed: {}", err),
                );
            }
        }
    }

    fn end_modify(&mut self) {}
}
