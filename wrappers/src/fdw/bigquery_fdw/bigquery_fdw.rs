use crate::stats;
use futures::executor;
use gcp_bigquery_client::{
    client_builder::ClientBuilder,
    model::{
        field_type::FieldType, get_query_results_parameters::GetQueryResultsParameters,
        query_request::QueryRequest, query_response::QueryResponse, query_response::ResultSet,
        table_data_insert_all_request::TableDataInsertAllRequest,
        table_field_schema::TableFieldSchema,
    },
    Client,
};
use pgrx::prelude::PgSqlErrorCode;
use pgrx::prelude::{AnyNumeric, Date, Timestamp};
use serde_json::json;
use std::collections::HashMap;
use std::str::FromStr;

use supabase_wrappers::prelude::*;

// convert BigQuery field to Cell
fn field_to_cell(rs: &ResultSet, field: &TableFieldSchema) -> BigQueryFdwResult<Option<Cell>> {
    Ok(match field.r#type {
        FieldType::Boolean => rs.get_bool_by_name(&field.name)?.map(Cell::Bool),
        FieldType::Int64 | FieldType::Integer => rs.get_i64_by_name(&field.name)?.map(Cell::I64),
        FieldType::Float64 | FieldType::Float => rs.get_f64_by_name(&field.name)?.map(Cell::F64),
        FieldType::Numeric => match rs.get_f64_by_name(&field.name)? {
            Some(v) => Some(Cell::Numeric(AnyNumeric::try_from(v)?)),
            None => None,
        },
        FieldType::String => rs.get_string_by_name(&field.name)?.map(Cell::String),
        FieldType::Date => match rs.get_string_by_name(&field.name)? {
            Some(v) => Some(Cell::Date(Date::from_str(&v)?)),
            None => None,
        },
        FieldType::Datetime => match rs.get_string_by_name(&field.name)? {
            Some(v) => Some(Cell::Timestamp(Timestamp::from_str(&v)?)),
            None => None,
        },
        FieldType::Timestamp => rs.get_f64_by_name(&field.name)?.map(|v| {
            let ts = pgrx::to_timestamp(v);
            Cell::Timestamp(ts.to_utc())
        }),
        _ => {
            return Err(BigQueryFdwError::UnsupportedFieldType(field.r#type.clone()));
        }
    })
}

#[wrappers_fdw(
    version = "0.1.4",
    author = "Supabase",
    website = "https://github.com/supabase/wrappers/tree/main/wrappers/src/fdw/bigquery_fdw",
    error_type = "BigQueryFdwError"
)]
pub(crate) struct BigQueryFdw {
    rt: Runtime,
    client: Option<Client>,
    project_id: String,
    dataset_id: String,
    table: String,
    rowid_col: String,
    tgt_cols: Vec<Column>,
    scan_result: Option<ResultSet>,
    auth_mock: Option<GoogleAuthMock>,
}

impl BigQueryFdw {
    const FDW_NAME: &'static str = "BigQueryFdw";

    fn deparse(
        &self,
        quals: &[Qual],
        columns: &[Column],
        sorts: &[Sort],
        limit: &Option<Limit>,
    ) -> String {
        let tgts = if columns.is_empty() {
            "*".to_string()
        } else {
            columns
                .iter()
                .map(|c| c.name.clone())
                .collect::<Vec<String>>()
                .join(", ")
        };
        let table = if self.table.starts_with('(') {
            self.table.clone()
        } else {
            format!("`{}.{}.{}`", self.project_id, self.dataset_id, self.table,)
        };

        let mut sql = if quals.is_empty() {
            format!("select {} from {}", tgts, table)
        } else {
            let cond = quals
                .iter()
                .map(|q| q.deparse())
                .collect::<Vec<String>>()
                .join(" and ");
            format!("select {} from {} where {}", tgts, table, cond)
        };

        // push down sorts
        if !sorts.is_empty() {
            let order_by = sorts
                .iter()
                .map(|sort| sort.deparse())
                .collect::<Vec<String>>()
                .join(", ");
            sql.push_str(&format!(" order by {}", order_by));
        }

        // push down limits
        // Note: Postgres will take limit and offset locally after reading rows
        // from remote, so we calculate the real limit and only use it without
        // pushing down offset.
        if let Some(limit) = limit {
            let real_limit = limit.offset + limit.count;
            sql.push_str(&format!(" limit {}", real_limit));
        }

        sql
    }

    fn extract_row(
        tgt_cols: &[Column],
        row: &mut Row,
        rs: &mut ResultSet,
    ) -> BigQueryFdwResult<bool> {
        if rs.next_row() {
            if let Some(schema) = &rs.query_response().schema {
                if let Some(fields) = &schema.fields {
                    for tgt_col in tgt_cols {
                        if let Some(field) = fields.iter().find(|&f| f.name == tgt_col.name) {
                            let cell = field_to_cell(rs, field)?;
                            row.push(&field.name, cell);
                        }
                    }
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }
}

impl ForeignDataWrapper<BigQueryFdwError> for BigQueryFdw {
    fn new(options: &HashMap<String, String>) -> Result<Self, BigQueryFdwError> {
        let mut ret = BigQueryFdw {
            rt: create_async_runtime()?,
            client: None,
            project_id: require_option("project_id", options)?.to_string(),
            dataset_id: require_option("dataset_id", options)?.to_string(),
            table: "".to_string(),
            rowid_col: "".to_string(),
            tgt_cols: Vec::new(),
            scan_result: None,
            auth_mock: None,
        };

        // Is authentication mocked
        let mock_auth: bool = options
            .get("mock_auth")
            .map(|t| t.to_owned())
            .unwrap_or_else(|| "false".to_string())
            == *"true";

        let api_endpoint = options
            .get("api_endpoint")
            .map(|t| t.to_owned())
            .unwrap_or_else(|| "https://bigquery.googleapis.com/bigquery/v2".to_string());

        let sa_key_json = match mock_auth {
            true => {
                // Key file is not required if we're mocking auth
                let auth_mock = executor::block_on(GoogleAuthMock::start());
                executor::block_on(auth_mock.mock_token(1));
                let auth_mock_uri = auth_mock.uri();
                let dummy_auth_config = dummy_configuration(&auth_mock_uri);
                ret.auth_mock = Some(auth_mock);
                serde_json::to_string_pretty(&dummy_auth_config)
                    .expect("dummy auth config should not fail to serialize")
            }
            false => match options.get("sa_key") {
                Some(sa_key) => sa_key.to_owned(),
                None => {
                    let sa_key_id = require_option("sa_key_id", options)?;
                    match get_vault_secret(sa_key_id) {
                        Some(sa_key) => sa_key,
                        None => return Ok(ret),
                    }
                }
            },
        };

        let sa_key = match yup_oauth2::parse_service_account_key(sa_key_json.as_bytes()) {
            Ok(sa_key) => sa_key,
            Err(err) => {
                report_error(
                    PgSqlErrorCode::ERRCODE_FDW_ERROR,
                    &format!("parse service account key JSON failed: {}", err),
                );
                return Ok(ret);
            }
        };

        ret.client = match ret.rt.block_on(
            ClientBuilder::new()
                .with_v2_base_url(api_endpoint)
                .build_from_service_account_key(sa_key, false),
        ) {
            Ok(client) => Some(client),
            Err(err) => {
                report_error(
                    PgSqlErrorCode::ERRCODE_FDW_ERROR,
                    &format!("create client failed: {}", err),
                );
                None
            }
        };

        stats::inc_stats(Self::FDW_NAME, stats::Metric::CreateTimes, 1);

        Ok(ret)
    }

    fn get_rel_size(
        &mut self,
        _quals: &[Qual],
        _columns: &[Column],
        _sorts: &[Sort],
        _limit: &Option<Limit>,
        _options: &HashMap<String, String>,
    ) -> Result<(i64, i32), BigQueryFdwError> {
        Ok((0, 0))
    }

    fn begin_scan(
        &mut self,
        quals: &[Qual],
        columns: &[Column],
        sorts: &[Sort],
        limit: &Option<Limit>,
        options: &HashMap<String, String>,
    ) -> Result<(), BigQueryFdwError> {
        self.table = require_option("table", options)?.to_string();
        self.tgt_cols = columns.to_vec();

        let location = options
            .get("location")
            .map(|t| t.to_owned())
            .unwrap_or_else(|| "US".to_string());

        let mut timeout: i32 = 30_000;
        if let Some(timeout_str) = options.get("timeout") {
            match timeout_str.parse::<i32>() {
                Ok(t) => timeout = t,
                Err(_) => report_error(
                    PgSqlErrorCode::ERRCODE_FDW_ERROR,
                    &format!("invalid timeout value: {}", timeout_str),
                ),
            }
        }

        if let Some(client) = &self.client {
            let sql = self.deparse(quals, columns, sorts, limit);
            let mut req = QueryRequest::new(sql);
            req.location = Some(location);
            req.timeout_ms = Some(timeout);

            // execute query on BigQuery
            match self.rt.block_on(client.job().query(&self.project_id, req)) {
                Ok(rs) => {
                    let resp = rs.query_response();
                    if resp.job_complete == Some(false) {
                        report_error(
                            PgSqlErrorCode::ERRCODE_FDW_ERROR,
                            &format!("query timeout {}ms expired", timeout),
                        );
                    } else {
                        stats::inc_stats(
                            Self::FDW_NAME,
                            stats::Metric::RowsIn,
                            resp.total_rows
                                .as_ref()
                                .and_then(|v| v.parse::<i64>().ok())
                                .unwrap_or(0i64),
                        );
                        stats::inc_stats(
                            Self::FDW_NAME,
                            stats::Metric::RowsOut,
                            resp.total_rows
                                .as_ref()
                                .and_then(|v| v.parse::<i64>().ok())
                                .unwrap_or(0i64),
                        );
                        stats::inc_stats(
                            Self::FDW_NAME,
                            stats::Metric::BytesIn,
                            resp.total_bytes_processed
                                .as_ref()
                                .and_then(|v| v.parse::<i64>().ok())
                                .unwrap_or(0i64),
                        );
                        self.scan_result = Some(rs);
                    }
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

        Ok(())
    }

    fn iter_scan(&mut self, row: &mut Row) -> Result<Option<()>, BigQueryFdwError> {
        if let Some(client) = &self.client {
            if let Some(ref mut rs) = self.scan_result {
                if Self::extract_row(&self.tgt_cols, row, rs)? {
                    return Ok(Some(()));
                }

                // deal with pagination
                if rs.query_response().page_token.is_some() {
                    if let Some(job_ref) = &rs.query_response().job_reference {
                        if let Some(job_id) = &job_ref.job_id {
                            match self.rt.block_on(client.job().get_query_results(
                                &self.project_id,
                                job_id,
                                GetQueryResultsParameters {
                                    location: job_ref.location.clone(),
                                    page_token: rs.query_response().page_token.clone(),
                                    ..Default::default()
                                },
                            )) {
                                Ok(resp) => {
                                    // replace result set with data from the new page
                                    *rs = ResultSet::new(QueryResponse::from(resp));
                                    if Self::extract_row(&self.tgt_cols, row, rs)? {
                                        return Ok(Some(()));
                                    }
                                }
                                Err(err) => {
                                    self.scan_result = None;
                                    report_error(
                                        PgSqlErrorCode::ERRCODE_FDW_ERROR,
                                        &format!("fetch query result failed: {}", err),
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }
        Ok(None)
    }

    fn end_scan(&mut self) -> Result<(), BigQueryFdwError> {
        self.scan_result.take();
        Ok(())
    }

    fn begin_modify(&mut self, options: &HashMap<String, String>) -> Result<(), BigQueryFdwError> {
        self.table = require_option("table", options)?.to_string();
        self.rowid_col = require_option("rowid_column", options)?.to_string();

        Ok(())
    }

    fn insert(&mut self, src: &Row) -> Result<(), BigQueryFdwError> {
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
                        Cell::Numeric(v) => row_json[col_name] = json!(v),
                        Cell::String(v) => row_json[col_name] = json!(v),
                        Cell::Date(v) => row_json[col_name] = json!(v),
                        Cell::Timestamp(v) => row_json[col_name] = json!(v),
                        Cell::Timestamptz(v) => row_json[col_name] = json!(v),
                        Cell::Json(v) => row_json[col_name] = json!(v),
                    }
                }
            }

            insert_request.add_row(None, row_json)?;

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

        Ok(())
    }

    fn update(&mut self, rowid: &Cell, new_row: &Row) -> Result<(), BigQueryFdwError> {
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

            let query_job = client.job().query(&self.project_id, QueryRequest::new(sql));

            // execute update on BigQuery
            if let Err(err) = self.rt.block_on(query_job) {
                report_error(
                    PgSqlErrorCode::ERRCODE_FDW_ERROR,
                    &format!("update failed: {}", err),
                );
            }
        }
        Ok(())
    }

    fn delete(&mut self, rowid: &Cell) -> Result<(), BigQueryFdwError> {
        if let Some(ref mut client) = self.client {
            let sql = format!(
                "delete from `{}.{}.{}` where {} = {}",
                self.project_id, self.dataset_id, self.table, self.rowid_col, rowid
            );

            let query_job = client.job().query(&self.project_id, QueryRequest::new(sql));

            // execute delete on BigQuery
            if let Err(err) = self.rt.block_on(query_job) {
                report_error(
                    PgSqlErrorCode::ERRCODE_FDW_ERROR,
                    &format!("update failed: {}", err),
                );
            }
        }
        Ok(())
    }
}

use crate::fdw::bigquery_fdw::{BigQueryFdwError, BigQueryFdwResult};
use auth_mock::GoogleAuthMock;

mod auth_mock {
    use serde::Serialize;
    use std::ops::Deref;
    use wiremock::{
        matchers::{method, path},
        Mock, MockServer, ResponseTemplate, Times,
    };

    pub const AUTH_TOKEN_ENDPOINT: &str = "/:o/oauth2/token";

    pub struct GoogleAuthMock {
        server: MockServer,
    }

    impl Deref for GoogleAuthMock {
        type Target = MockServer;

        fn deref(&self) -> &Self::Target {
            &self.server
        }
    }

    impl GoogleAuthMock {
        pub async fn start() -> Self {
            Self {
                server: MockServer::start().await,
            }
        }
    }

    #[derive(Eq, PartialEq, Serialize, Debug, Clone)]
    pub struct Token {
        access_token: String,
        token_type: String,
        expires_in: u32,
    }

    impl Token {
        fn fake() -> Self {
            Self {
                access_token: "aaaa".to_string(),
                token_type: "bearer".to_string(),
                expires_in: 9999999,
            }
        }
    }

    impl GoogleAuthMock {
        /// Mock token, given how many times the endpoint will be called.
        pub async fn mock_token<T: Into<Times>>(&self, n_times: T) {
            let response = ResponseTemplate::new(200).set_body_json(Token::fake());
            Mock::given(method("POST"))
                .and(path(AUTH_TOKEN_ENDPOINT))
                .respond_with(response)
                .named("mock token")
                .expect(n_times)
                .mount(self)
                .await;
        }
    }
}

pub fn dummy_configuration(oauth_server: &str) -> serde_json::Value {
    let oauth_endpoint = format!("{}/:o/oauth2", oauth_server);
    serde_json::json!({
      "type": "service_account",
      "project_id": "dummy",
      "private_key_id": "dummy",
      "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQDNk6cKkWP/4NMu\nWb3s24YHfM639IXzPtTev06PUVVQnyHmT1bZgQ/XB6BvIRaReqAqnQd61PAGtX3e\n8XocTw+u/ZfiPJOf+jrXMkRBpiBh9mbyEIqBy8BC20OmsUc+O/YYh/qRccvRfPI7\n3XMabQ8eFWhI6z/t35oRpvEVFJnSIgyV4JR/L/cjtoKnxaFwjBzEnxPiwtdy4olU\nKO/1maklXexvlO7onC7CNmPAjuEZKzdMLzFszikCDnoKJC8k6+2GZh0/JDMAcAF4\nwxlKNQ89MpHVRXZ566uKZg0MqZqkq5RXPn6u7yvNHwZ0oahHT+8ixPPrAEjuPEKM\nUPzVRz71AgMBAAECggEAfdbVWLW5Befkvam3hea2+5xdmeN3n3elrJhkiXxbAhf3\nE1kbq9bCEHmdrokNnI34vz0SWBFCwIiWfUNJ4UxQKGkZcSZto270V8hwWdNMXUsM\npz6S2nMTxJkdp0s7dhAUS93o9uE2x4x5Z0XecJ2ztFGcXY6Lupu2XvnW93V9109h\nkY3uICLdbovJq7wS/fO/AL97QStfEVRWW2agIXGvoQG5jOwfPh86GZZRYP9b8VNw\ntkAUJe4qpzNbWs9AItXOzL+50/wsFkD/iWMGWFuU8DY5ZwsL434N+uzFlaD13wtZ\n63D+tNAxCSRBfZGQbd7WxJVFfZe/2vgjykKWsdyNAQKBgQDnEBgSI836HGSRk0Ub\nDwiEtdfh2TosV+z6xtyU7j/NwjugTOJEGj1VO/TMlZCEfpkYPLZt3ek2LdNL66n8\nDyxwzTT5Q3D/D0n5yE3mmxy13Qyya6qBYvqqyeWNwyotGM7hNNOix1v9lEMtH5Rd\nUT0gkThvJhtrV663bcAWCALmtQKBgQDjw2rYlMUp2TUIa2/E7904WOnSEG85d+nc\norhzthX8EWmPgw1Bbfo6NzH4HhebTw03j3NjZdW2a8TG/uEmZFWhK4eDvkx+rxAa\n6EwamS6cmQ4+vdep2Ac4QCSaTZj02YjHb06Be3gptvpFaFrotH2jnpXxggdiv8ul\n6x+ooCffQQKBgQCR3ykzGoOI6K/c75prELyR+7MEk/0TzZaAY1cSdq61GXBHLQKT\nd/VMgAN1vN51pu7DzGBnT/dRCvEgNvEjffjSZdqRmrAVdfN/y6LSeQ5RCfJgGXSV\nJoWVmMxhCNrxiX3h01Xgp/c9SYJ3VD54AzeR/dwg32/j/oEAsDraLciXGQKBgQDF\nMNc8k/DvfmJv27R06Ma6liA6AoiJVMxgfXD8nVUDW3/tBCVh1HmkFU1p54PArvxe\nchAQqoYQ3dUMBHeh6ZRJaYp2ATfxJlfnM99P1/eHFOxEXdBt996oUMBf53bZ5cyJ\n/lAVwnQSiZy8otCyUDHGivJ+mXkTgcIq8BoEwERFAQKBgQDmImBaFqoMSVihqHIf\nDa4WZqwM7ODqOx0JnBKrKO8UOc51J5e1vpwP/qRpNhUipoILvIWJzu4efZY7GN5C\nImF9sN3PP6Sy044fkVPyw4SYEisxbvp9tfw8Xmpj/pbmugkB2ut6lz5frmEBoJSN\n3osZlZTgx+pM3sO6ITV6U4ID2Q==\n-----END PRIVATE KEY-----\n",
      "client_email": "dummy@developer.gserviceaccount.com",
      "client_id": "dummy",
      "auth_uri": format!("{}/auth", oauth_endpoint),
      "token_uri": format!("{}{}", oauth_server, auth_mock::AUTH_TOKEN_ENDPOINT),
      "auth_provider_x509_cert_url": format!("{}/v1/certs", oauth_endpoint),
      "client_x509_cert_url": format!("{}/robot/v1/metadata/x509/457015483506-compute%40developer.gserviceaccount.com", oauth_server)
    })
}
