use pgx::log::PgSqlErrorCode;
use reqwest::{self, header};
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry::{policies::ExponentialBackoff, RetryTransientMiddleware};
use serde_json::Value;
use std::collections::HashMap;

use serde::de::{self, MapAccess, Visitor};
use serde::{Deserialize, Deserializer};
use std::fmt;
use std::marker::PhantomData;
use std::str::FromStr;

use supabase_wrappers::{
    create_async_runtime, report_error, require_option, Cell, ForeignDataWrapper, Limit, Qual, Row,
    Runtime, Sort,
};

pub(crate) struct AirtableFdw {
    rt: Runtime,
    base_url: String,
    client: Option<ClientWithMiddleware>,
    scan_result: Option<Vec<Row>>,
}

#[derive(Deserialize)]
struct AirtableResponse {
    records: Vec<AirtableRecord>,
}

struct AirtableFields(HashMap<String, Value>);

struct AirtableFieldsVisitor {
    marker: PhantomData<fn() -> AirtableFields>,
}

impl AirtableFieldsVisitor {
    fn new() -> Self {
        AirtableFieldsVisitor {
            marker: PhantomData,
        }
    }
}

// This is the trait that Deserializers are going to be driving. There
// is one method for each type of data that our type knows how to
// deserialize from. There are many other methods that are not
// implemented here, for example deserializing from integers or strings.
// By default those methods will return an error, which makes sense
// because we cannot deserialize a AirtableFields from an integer or string.
impl<'de> Visitor<'de> for AirtableFieldsVisitor {
    // The type that our Visitor is going to produce.
    type Value = AirtableFields;

    // Format a message stating what data this Visitor expects to receive.
    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("map")
    }

    // Deserialize AirtableFields from an abstract "map" provided by the
    // Deserializer. The MapAccess input is a callback provided by
    // the Deserializer to let us see each entry in the map.
    fn visit_map<M>(self, mut access: M) -> Result<Self::Value, M::Error>
    where
        M: MapAccess<'de>,
    {
        let mut map = AirtableFields(HashMap::with_capacity(access.size_hint().unwrap_or(0)));

        // While there are entries remaining in the input, add them
        // into our map.
        while let Some((key, value)) = access.next_entry::<String, Value>()? {
            map.0.insert(key.to_lowercase(), value);
        }

        Ok(map)
    }
}

// This is the trait that informs Serde how to deserialize AirtableFields.
impl<'de> Deserialize<'de> for AirtableFields {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // Instantiate our Visitor and ask the Deserializer to drive
        // it over the input data, resulting in an instance of AirtableFields.
        deserializer.deserialize_map(AirtableFieldsVisitor::new())
    }
}

#[derive(Deserialize)]
struct AirtableRecord {
    id: String,
    fields: AirtableFields,
    // TODO Incorporate the createdTime field? We'll need to deserialize as a timestamp
}

impl AirtableRecord {
    fn value_to_cell(value: &Value) -> Option<Cell> {
        use serde_json::Value::*;
        match value {
            Null => None,
            Bool(v) => Some(Cell::Bool(*v)),
            Number(n) => n.as_i64().map_or_else(
                || n.as_f64().map_or(None, |v| Some(Cell::F64(v))),
                |v| Some(Cell::I64(v)),
            ),
            String(v) => Some(Cell::String(v.clone())),
            // XXX Handle timestamps somehow...
            // XXX Fix (probably map to JsonB)
            _ => panic!("Unsupported: Array/Object"),
        }
    }

    fn to_row(&self, columns: &Vec<String>) -> Row {
        let mut row = Row::new();
        for col in columns.iter() {
            if col == "id" {
                row.push("id", Some(Cell::String(self.id.clone())));
            } else {
                row.push(
                    col,
                    match self.fields.0.get(col) {
                        Some(val) => AirtableRecord::value_to_cell(val),
                        None => None,
                    },
                );
            }
        }
        row
    }
}

impl AirtableFdw {
    pub fn new(options: &HashMap<String, String>) -> Self {
        let base_url = options
            .get("api_url")
            .map(|t| t.to_owned())
            .unwrap_or("https://api.airtable.com/v0/app4PDEzNrArJdQ5k".to_string())
            .trim_end_matches('/')
            .to_owned();

        // TODO: Support a cache
        let client = require_option("api_key", options).map(|api_key| {
            let mut headers = header::HeaderMap::new();
            let value = format!("Bearer {}", api_key);
            let mut auth_value = header::HeaderValue::from_str(&value).unwrap();
            auth_value.set_sensitive(true);
            headers.insert(header::AUTHORIZATION, auth_value);
            let client = reqwest::Client::builder()
                .default_headers(headers)
                .build()
                .unwrap();
            let retry_policy = ExponentialBackoff::builder().build_with_max_retries(3);
            let client = ClientBuilder::new(client)
                .with(RetryTransientMiddleware::new_with_policy(retry_policy))
                .build();
            client
        });

        AirtableFdw {
            rt: create_async_runtime(),
            base_url,
            client,
            scan_result: None,
        }
    }

    #[inline]
    fn build_url(&self, base_id: &str, table_name: &str) -> String {
        // XXX: Test with table that has a space in the name (do we need to encode here?)
        format!("{}/{}/{}", &self.base_url, base_id, table_name)
    }

    // convert response body text to rows
    fn resp_to_rows(&self, resp_body: &str, columns: &Vec<String>) -> Vec<Row> {
        let response: AirtableResponse = serde_json::from_str(resp_body).unwrap();
        let mut result = Vec::new();

        for record in response.records.iter() {
            result.push(record.to_row(columns));
        }

        result
    }
}

macro_rules! report_fetch_error {
    ($url:ident, $err:ident) => {
        report_error(
            PgSqlErrorCode::ERRCODE_FDW_ERROR,
            &format!("fetch {} failed: {}", $url, $err),
        )
    };
}

// XXX Add support for INSERT
impl ForeignDataWrapper for AirtableFdw {
    fn begin_scan(
        &mut self,
        // TODO: We should be able to propagate some of these through to airtable
        _quals: &Vec<Qual>,
        columns: &Vec<String>,
        _sorts: &Vec<Sort>,
        _limit: &Option<Limit>, // TODO: maxRecords
        options: &HashMap<String, String>,
    ) {
        // TODO: Support specifying other options (view, maxRecords)
        let url = if let Some(url) = require_option("base_id", options).and_then(|base_id| {
            require_option("table", options).map(|table| self.build_url(&base_id, &table))
        }) {
            url
        } else {
            // XXX Should we report an error here? It doesn't seem like the Stripe one does
            // if object is empty
            return;
        };

        // XXX Implement pagination
        if let Some(client) = &self.client {
            match self.rt.block_on(client.get(&url).send()) {
                Ok(resp) => match resp.error_for_status() {
                    Ok(resp) => {
                        let body = self.rt.block_on(resp.text()).unwrap();
                        let result = self.resp_to_rows(&body, columns);
                        self.scan_result = Some(result);
                    }
                    Err(err) => report_fetch_error!(url, err),
                },
                Err(err) => report_fetch_error!(url, err),
            }
        }
    }

    fn iter_scan(&mut self) -> Option<Row> {
        if let Some(ref mut result) = self.scan_result {
            if !result.is_empty() {
                return result.drain(0..1).last();
            }
        }
        None
    }

    fn end_scan(&mut self) {
        self.scan_result.take();
    }
}
