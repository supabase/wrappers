use super::{Auth0FdwError, Auth0FdwResult};
use pgrx::pg_sys::panic::ErrorReport;
use pgrx::PgSqlErrorCode;
use reqwest::{self, header};
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry::{policies::ExponentialBackoff, RetryTransientMiddleware};
use std::collections::HashMap;
use supabase_wrappers::prelude::*;
// A simple demo FDW
#[wrappers_fdw(
    version = "0.1.1",
    author = "Joel",
    website = "https://github.com/supabase/wrappers/tree/main/wrappers/src/fdw/auth0_fdw",
    error_type = "Auth0FdwError"
)]
pub(crate) struct Auth0Fdw {
    // row counter
    row_cnt: i64,
    client: Option<ClientWithMiddleware>,
    base_url: String,

    // target column list
    tgt_cols: Vec<Column>,
}

fn create_client(api_key: &str) -> Result<ClientWithMiddleware, Auth0FdwError> {
    let mut headers = header::HeaderMap::new();
    let client = reqwest::Client::builder()
        .default_headers(headers)
        .build()?;
    let retry_policy = ExponentialBackoff::builder().build_with_max_retries(3);
    Ok(ClientBuilder::new(client)
        .with(RetryTransientMiddleware::new_with_policy(retry_policy))
        .build())
}

impl Auth0Fdw {
    const FDW_NAME: &str = "Auth0Fdw";
}

impl ForeignDataWrapper<Auth0FdwError> for Auth0Fdw {
    // 'options' is the key-value pairs defined in `CREATE SERVER` SQL, for example,
    //
    // create server my_auth0_server
    //   foreign data wrapper wrappers_auth0
    //   options (
    //     foo 'bar'
    // );
    //
    // 'options' passed here will be a hashmap { 'foo' -> 'bar' }.
    //
    // You can do any initalization in this new() function, like saving connection
    // info or API url in an variable, but don't do any heavy works like making a
    // database connection or API call.

    fn new(_options: &HashMap<String, String>) -> Auth0FdwResult<Self> {
        // let base_url = options
        //     .get("api_url")
        //     .map(|t| t.to_owned())
        //     // TODO: Find a way to pass through tenant
        //     .unwrap_or_else(|| "https://@@TENANT@@.auth0.com/api/v2/users".to_string());

        // let client = match options.get("api_key") {
        //     Some(api_key) => Some(create_client(api_key)?),
        //     None => {
        //         let key_id = require_option("api_key_id", options)?;
        //         if let Some(api_key) = get_vault_secret(key_id) {
        //             Some(create_client(&api_key)?)
        //         } else {
        //             None
        //         }
        //     }
        // };
        Ok(Self {
            base_url: "".to_string(),
            row_cnt: 5,
            client: None,
            tgt_cols: Vec::new(),
        })
    }

    fn begin_scan(
        &mut self,
        _quals: &[Qual],
        columns: &[Column],
        _sorts: &[Sort],
        _limit: &Option<Limit>,
        _options: &HashMap<String, String>,
    ) -> Auth0FdwResult<()> {
        // reset row counter
        self.row_cnt = 0;

        // save a copy of target columns
        self.tgt_cols = columns.to_vec();

        Ok(())
    }

    fn iter_scan(&mut self, row: &mut Row) -> Auth0FdwResult<Option<()>> {
        // this is called on each row and we only return one row here
        if self.row_cnt < 1 {
            // add values to row if they are in target column list
            for tgt_col in &self.tgt_cols {
                match tgt_col.name.as_str() {
                    "id" => row.push("id", Some(Cell::I64(self.row_cnt))),
                    "col" => row.push("col", Some(Cell::String("Hello world".to_string()))),
                    _ => {}
                }
            }

            self.row_cnt += 1;

            // return Some(()) to Postgres and continue data scan
            return Ok(Some(()));
        }

        // return 'None' to stop data scan
        Ok(None)
    }

    fn end_scan(&mut self) -> Auth0FdwResult<()> {
        // we do nothing here, but you can do things like resource cleanup and etc.
        Ok(())
    }
}
