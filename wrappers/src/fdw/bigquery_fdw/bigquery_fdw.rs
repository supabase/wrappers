use futures::executor;
use gcp_bigquery_client::{
    client_builder::ClientBuilder,
    model::{
        field_type::FieldType, query_request::QueryRequest, query_response::ResultSet,
        table::Table, table_data_insert_all_request::TableDataInsertAllRequest,
        table_field_schema::TableFieldSchema,
    },
    Client,
};
use pgx::prelude::PgSqlErrorCode;
use pgx::prelude::{Date, Timestamp};
use serde_json::json;
use std::collections::HashMap;
use time::{format_description::well_known::Iso8601, OffsetDateTime, PrimitiveDateTime};

use supabase_wrappers::prelude::*;

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

pub(crate) struct BigQueryFdw {
    rt: Runtime,
    client: Option<Client>,
    project_id: String,
    dataset_id: String,
    table: String,
    rowid_col: String,
    tgt_cols: Vec<String>,
    scan_result: Option<(Table, ResultSet)>,
    auth_mock: Option<GoogleAuthMock>,
}

impl BigQueryFdw {
    fn deparse(&self, quals: &Vec<Qual>, columns: &Vec<String>) -> String {
        let tgts = if columns.is_empty() {
            "*".to_string()
        } else {
            columns.join(", ")
        };
        let table = format!("`{}.{}.{}`", self.project_id, self.dataset_id, self.table,);
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
    fn new(options: &HashMap<String, String>) -> Self {
        let mut ret = BigQueryFdw {
            rt: create_async_runtime(),
            client: None,
            project_id: "".to_string(),
            dataset_id: "".to_string(),
            table: "".to_string(),
            rowid_col: "".to_string(),
            tgt_cols: Vec::new(),
            scan_result: None,
            auth_mock: None,
        };

        let project_id = require_option("project_id", options);
        let dataset_id = require_option("dataset_id", options);

        if project_id.is_none() || dataset_id.is_none() {
            return ret;
        }
        ret.project_id = project_id.unwrap();
        ret.dataset_id = dataset_id.unwrap();

        // Is authentication mocked
        let mock_auth: bool = options
            .get("mock_auth")
            .map(|t| t.to_owned())
            .unwrap_or("false".to_string())
            == "true".to_string();

        let api_endpoint = options
            .get("api_endpoint")
            .map(|t| t.to_owned())
            .unwrap_or("https://bigquery.googleapis.com/bigquery/v2".to_string());

        let (auth_endpoint, sa_key_json) = match mock_auth {
            true => {
                // Key file is not required if we're mocking auth
                let auth_mock = executor::block_on(GoogleAuthMock::start());
                executor::block_on(auth_mock.mock_token(1));
                let auth_mock_uri = auth_mock.uri();
                let dummy_auth_config = dummy_configuration(&auth_mock_uri);
                ret.auth_mock = Some(auth_mock);
                let sa_key_json = serde_json::to_string_pretty(&dummy_auth_config).unwrap();
                (auth_mock_uri.to_string(), sa_key_json)
            }
            false => {
                let uri = "https://www.googleapis.com/auth/bigquery".to_string();
                match options.get("sa_key") {
                    Some(sa_key) => (uri, sa_key.to_owned()),
                    None => {
                        let sa_key_id = match require_option("sa_key_id", options) {
                            Some(sa_key_id) => sa_key_id,
                            None => return ret,
                        };
                        let sa_key_json = match get_vault_secret(&sa_key_id) {
                            Some(sa_key) => sa_key,
                            None => return ret,
                        };
                        (uri, sa_key_json)
                    }
                }
            }
        };

        let sa_key = match yup_oauth2::parse_service_account_key(sa_key_json.as_bytes()) {
            Ok(sa_key) => sa_key,
            Err(err) => {
                report_error(
                    PgSqlErrorCode::ERRCODE_FDW_ERROR,
                    &format!("parse service account key JSON failed: {}", err),
                );
                return ret;
            }
        };

        ret.client = match ret.rt.block_on(
            ClientBuilder::new()
                .with_auth_base_url(auth_endpoint)
                // Url of the BigQuery emulator docker image.
                .with_v2_base_url(api_endpoint)
                .build_from_service_account_key(sa_key, true),
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

        ret
    }

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
