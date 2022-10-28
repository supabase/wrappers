use gcp_bigquery_client::{
    model::{
        field_type::FieldType, query_request::QueryRequest, query_response::ResultSet,
        table::Table, table_field_schema::TableFieldSchema,
    },
    Client,
};
use pgx::log::PgSqlErrorCode;
use pgx::prelude::{Date, Timestamp};
use std::collections::HashMap;
use time::{format_description::well_known::Iso8601, OffsetDateTime, PrimitiveDateTime};

use supabase_wrappers::{
    create_async_runtime, report_error, Cell, ForeignDataWrapper, Limit, Qual, Row, Runtime, Sort,
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

pub struct BigQueryFdw {
    rt: Runtime,
    client: Client,
    project_id: String,
    dataset_id: String,
    table: String,
    scan_result: Option<(Table, ResultSet)>,
}

impl BigQueryFdw {
    pub fn new(options: &HashMap<String, String>) -> Self {
        let sa_key_file = options.get("sa_key_file").map(|t| t.to_owned()).unwrap();
        let project_id = options.get("project_id").map(|t| t.to_owned()).unwrap();
        let dataset_id = options.get("dataset_id").map(|t| t.to_owned()).unwrap();

        let rt = create_async_runtime();
        let client = rt.block_on(Client::from_service_account_key_file(&sa_key_file));
        BigQueryFdw {
            rt,
            client,
            project_id,
            dataset_id,
            table: "".to_string(),
            scan_result: None,
        }
    }

    fn deparse(
        &self,
        quals: &Vec<Qual>,
        columns: &Vec<String>,
        options: &HashMap<String, String>,
    ) -> String {
        let tgts = columns.join(", ");
        let table = format!(
            "{}.{}.{}",
            self.project_id,
            self.dataset_id,
            options.get("table").unwrap()
        );
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
        self.table = options.get("table").map(|t| t.to_owned()).unwrap();
        let location = options
            .get("location")
            .map(|t| t.to_owned())
            .unwrap_or("US".to_string());

        // get table metadata
        let selected_fields = columns.iter().map(|c| c.as_str()).collect::<Vec<&str>>();
        let tbl = match self.rt.block_on(self.client.table().get(
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

        let sql = self.deparse(quals, columns, options);
        let mut req = QueryRequest::new(sql);
        req.location = Some(location);

        // execute query on BigQuery
        match self
            .rt
            .block_on(self.client.job().query(&self.project_id, req))
        {
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

    fn iter_scan(&mut self) -> Option<Row> {
        if let Some((ref tbl, ref mut rs)) = self.scan_result {
            if rs.next_row() {
                if let Some(fields) = &tbl.schema.fields {
                    let mut ret = Row::new();
                    for field in fields {
                        let cell = field_to_cell(rs, field);
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
}
