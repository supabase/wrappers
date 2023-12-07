use super::{Auth0FdwError, Auth0FdwResult};
use crate::fdw::auth0_fdw::auth0_client::rows_iterator::RowsIterator;
use crate::fdw::auth0_fdw::auth0_client::Auth0Client;

use crate::fdw::auth0_fdw::result::Auth0Record;
use crate::stats;
use pgrx::pg_sys;
use std::collections::HashMap;
use supabase_wrappers::prelude::*;

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
    rows_iterator: Option<RowsIterator>,
}

impl Auth0Fdw {
    const FDW_NAME: &'static str = "Auth0Fdw";

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

        stats::inc_stats(Self::FDW_NAME, stats::Metric::CreateTimes, 1);
        Ok(Self {
            url,
            api_key,
            rows_iterator: None,
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
        let auth0_client = Auth0Client::new(&self.api_key)?;
        self.rows_iterator = Some(RowsIterator::new(columns.to_vec(), 1000, auth0_client));

        Ok(())
    }

    fn iter_scan(&mut self, row: &mut Row) -> Result<Option<()>, Auth0FdwError> {
        let rows_iterator = self
            .rows_iterator
            .as_mut()
            .expect("Can't be None as rows_iterator is initialized in begin_scan");
        if let Some(new_row_result) = rows_iterator.next() {
            let new_row = new_row_result?;
            row.replace_with(new_row);
            Ok(Some(()))
        } else {
            Ok(None)
        }
    }

    fn end_scan(&mut self) -> Auth0FdwResult<()> {
        Ok(())
    }

    fn validator(options: Vec<Option<String>>, catalog: Option<pg_sys::Oid>) -> Auth0FdwResult<()> {
        if let Some(oid) = catalog {
            if oid == FOREIGN_SERVER_RELATION_ID {
                let api_key_exists = check_options_contain(&options, "api_key").is_ok();
                let api_key_id_exists = check_options_contain(&options, "api_key_id").is_ok();
                let url_exists = check_options_contain(&options, "url").is_ok();
                if (api_key_exists && api_key_id_exists) || (!api_key_exists && !api_key_id_exists)
                {
                    return Err(Auth0FdwError::SetOneOfApiKeyAndApiKeyIdSet);
                }
                if !url_exists {
                    return Err(Auth0FdwError::URLOptionMissing);
                }
            } else if oid == FOREIGN_TABLE_RELATION_ID {
                check_options_contain(&options, "object")?;
            }
        }

        Ok(())
    }
}
