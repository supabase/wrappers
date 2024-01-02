use crate::fdw::auth0_fdw::auth0_client::rows_iterator::RowsIterator;
use crate::fdw::auth0_fdw::auth0_client::CognitoClient;

use crate::stats;
use pgrx::pg_sys;
use std::collections::HashMap;
use supabase_wrappers::prelude::*;

use crate::fdw::cognito_fdw::cognito_client::CognitoClientError;
use pgrx::pg_sys::panic::ErrorReport;
use pgrx::PgSqlErrorCode;
use thiserror::Error;

#[wrappers_fdw(
    version = "0.1.1",
    author = "Joel",
    website = "https://github.com/supabase/wrappers/tree/main/wrappers/src/fdw/cognito_fdw",
    error_type = "CognitoFdwError"
)]
pub(crate) struct CognitoFdw {
    // row counter
    url: String,
    api_key: String,
    rows_iterator: Option<RowsIterator>,
}

#[derive(Error, Debug)]
enum CognitoFdwError {
    #[error("{0}")]
    CognitoClientError(#[from] CognitoClientError),

    #[error("{0}")]
    CreateRuntimeError(#[from] CreateRuntimeError),

    #[error("parse url failed: {0}")]
    UrlParseError(#[from] url::ParseError),

    #[error("request failed: {0}")]
    RequestError(#[from] reqwest::Error),

    #[error("request middleware failed: {0}")]
    RequestMiddlewareError(#[from] reqwest_middleware::Error),

    #[error("invalid json response: {0}")]
    SerdeError(#[from] serde_json::Error),

    #[error("{0}")]
    OptionsError(#[from] OptionsError),

    #[error("{0}")]
    NumericConversionError(#[from] pgrx::numeric::Error),
    #[error("no secret found in vault with id {0}")]
    SecretNotFound(String),

    #[error("`url` option must be set")]
    URLOptionMissing,

    #[error("exactly one of `api_key` or `api_key_id` options must be set")]
    SetOneOfApiKeyAndApiKeyIdSet,
}

impl From<CognitoFdwError> for ErrorReport {
    fn from(value: CognitoFdwError) -> Self {
        match value {
            CognitoFdwError::CreateRuntimeError(e) => e.into(),
            CognitoFdwError::OptionsError(e) => e.into(),
            CognitoFdwError::CognitoClientError(e) => e.into(),
            CognitoFdwError::SecretNotFound(_) => {
                ErrorReport::new(PgSqlErrorCode::ERRCODE_FDW_ERROR, format!("{value}"), "")
            }
            _ => ErrorReport::new(PgSqlErrorCode::ERRCODE_FDW_ERROR, format!("{value}"), ""),
        }
    }
}

type CognitoFdwResult<T> = Result<T, CognitoFdwError>;

impl CognitoFdw {
    const FDW_NAME: &'static str = "CognitoFdw";
}

impl ForeignDataWrapper<CognitoFdwError> for CognitoFdw {
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

    fn new(options: &HashMap<String, String>) -> Result<Self, CognitoFdwError> {
        let url = require_option("url", options)?.to_string();
        let api_key = if let Some(api_key) = options.get("api_key") {
            api_key.clone()
        } else {
            let api_key_id = options
                .get("api_key_id")
                .expect("`api_key_id` must be set if `api_key` is not");
            get_vault_secret(api_key_id).ok_or(CognitoFdwError::SecretNotFound(api_key_id.clone()))?
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
        _options: &HashMap<String, String>,
    ) -> CognitoFdwResult<()> {
        let auth0_client = CognitoClient::new(&self.url, &self.api_key)?;
        self.rows_iterator = Some(RowsIterator::new(columns.to_vec(), 1000, auth0_client));

        Ok(())
    }

    fn iter_scan(&mut self, row: &mut Row) -> Result<Option<()>, CognitoFdwError> {
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

    fn end_scan(&mut self) -> CognitoFdwResult<()> {
        Ok(())
    }

    fn validator(options: Vec<Option<String>>, catalog: Option<pg_sys::Oid>) -> CognitoFdwResult<()> {
        if let Some(oid) = catalog {
            if oid == FOREIGN_SERVER_RELATION_ID {
                let api_key_exists = check_options_contain(&options, "api_key").is_ok();
                let api_key_id_exists = check_options_contain(&options, "api_key_id").is_ok();
                let url_exists = check_options_contain(&options, "url").is_ok();
                if (api_key_exists && api_key_id_exists) || (!api_key_exists && !api_key_id_exists)
                {
                    return Err(CognitoFdwError::SetOneOfApiKeyAndApiKeyIdSet);
                }
                if !url_exists {
                    return Err(CognitoFdwError::URLOptionMissing);
                }
            } else if oid == FOREIGN_TABLE_RELATION_ID {
                check_options_contain(&options, "object")?;
            }
        }

        Ok(())
    }
}
