use super::{Auth0FdwError, Auth0FdwResult};
use crate::fdw::auth0_fdw::auth0_client::Auth0Client;
use crate::fdw::auth0_fdw::result::Auth0Record;
use crate::stats;
use std::collections::HashMap;
use supabase_wrappers::prelude::*;
use url::Url;

// A simple demo FDW
#[wrappers_fdw(
    version = "0.1.1",
    author = "Joel",
    website = "https://github.com/supabase/wrappers/tree/main/wrappers/src/fdw/auth0_fdw",
    error_type = "Auth0FdwError"
)]
pub(crate) struct Auth0Fdw {
    // row counter
    url: String,
    api_key: String,
    rt: Runtime,
    scan_result: Option<Vec<Row>>,
    client: Option<Auth0Client>,
}

impl Auth0Fdw {
    const FDW_NAME: &str = "Auth0Fdw";

    fn set_limit_offset(
        &self,
        url: &str,
        page_size: Option<usize>,
        offset: Option<&str>,
    ) -> Auth0FdwResult<String> {
        let mut params = Vec::new();
        if let Some(page_size) = page_size {
            params.push(("pageSize", format!("{}", page_size)));
        }

        Ok(Url::parse_with_params(url, &params).map(|x| x.into())?)
    }
    // convert response text to rows
    fn parse_resp(
        &self,
        resp_body: &str,
        columns: &[Column],
    ) -> Auth0FdwResult<(Vec<Row>, Option<String>)> {
        let response: Vec<Auth0Record> = serde_json::from_str(resp_body)?;
        let mut result = Vec::new();

        for record in response.iter() {
            result.push(record.to_row(columns)?);
        }

        Ok((result, None))
    }
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

    fn new(options: &HashMap<String, String>) -> Result<Self, Auth0FdwError> {
        let url = require_option("url", options)?.to_string();
        let api_key = if let Some(api_key) = options.get("api_key") {
            api_key.clone()
        } else {
            let api_key_id = options
                .get("api_key_id")
                .expect("`api_key_id` must be set if `api_key` is not");
            get_vault_secret(api_key_id).ok_or(Auth0FdwError::SecretNotFound(api_key_id.clone()))?
        };

        // TODO: Properly create the error
        let auth0_client = Auth0Client::new(&url, &api_key)?;

        stats::inc_stats(Self::FDW_NAME, stats::Metric::CreateTimes, 1);
        Ok(Self {
            url,
            api_key,
            client: Some(auth0_client),
            rt: create_async_runtime()?,
            scan_result: None,
        })
    }

    fn begin_scan(
        &mut self,
        _quals: &[Qual],
        columns: &[Column],
        _sorts: &[Sort],
        _limit: &Option<Limit>,
        options: &HashMap<String, String>,
    ) -> Auth0FdwResult<()> {
        // save a copy of target columns
        let mut rows = Vec::new();
        if let Some(client) = &self.client {
            let mut offset: Option<String> = None;
            loop {
                // Fetch all of the rows upfront. Arguably, this could be done in batches (and invoked each
                // time iter_scan() runs out of rows) to pipeline the I/O, but we'd have to manage more
                // state so starting with the simpler solution.
                //let url = self.set_limit_offset(&url, None, offset.as_deref())?;

                let body = self
                    .rt
                    .block_on(client.get_client().get(&self.url).send())
                    .and_then(|resp| {
                        resp.error_for_status()
                            .and_then(|resp| self.rt.block_on(resp.text()))
                            .map_err(reqwest_middleware::Error::from)
                    })?;

                let (new_rows, new_offset) = self.parse_resp(&body, columns)?;
                rows.extend(new_rows);

                stats::inc_stats(Self::FDW_NAME, stats::Metric::BytesIn, body.len() as i64);

                if let Some(new_offset) = new_offset {
                    offset = Some(new_offset);
                } else {
                    break;
                }
            }
        }
        stats::inc_stats(Self::FDW_NAME, stats::Metric::RowsIn, rows.len() as i64);
        stats::inc_stats(Self::FDW_NAME, stats::Metric::RowsOut, rows.len() as i64);

        self.scan_result = Some(rows);
        Ok(())
    }

    fn iter_scan(&mut self, row: &mut Row) -> Auth0FdwResult<Option<()>> {
        // this is called on each row and we only return one row here
        if let Some(ref mut result) = self.scan_result {
            if !result.is_empty() {
                return Ok(result
                    .drain(0..1)
                    .last()
                    .map(|src_row| row.replace_with(src_row)));
            }
        }
        Ok(None)
    }

    fn end_scan(&mut self) -> Auth0FdwResult<()> {
        Ok(())
    }
}
