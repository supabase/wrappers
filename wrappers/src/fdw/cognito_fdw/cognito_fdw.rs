use crate::fdw::cognito_fdw::cognito_client::rows_iterator::RowsIterator;
use crate::fdw::cognito_fdw::cognito_client::CognitoClientError;

use std::env;

use aws_sdk_cognitoidentityprovider::config::BehaviorVersion;
use aws_sdk_cognitoidentityprovider::Client;

use crate::stats;
use pgrx::pg_sys;
use std::collections::HashMap;
use supabase_wrappers::prelude::*;

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
    client: aws_sdk_cognitoidentityprovider::Client,
    user_pool_id: String,
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

    #[error("both `api_key` and `api_secret_key` options must be set")]
    ApiKeyAndSecretKeySet,

    #[error("exactly one of `aws_secret_access_key` or `api_key_id` options must be set")]
    SetOneOfSecretKeyAndApiKeyIdSet,
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
    // create server my_cognito_server
    //   foreign data wrapper wrappers_cognito
    //   options (
    //     foo 'bar'
    // );
    // 'options' passed here will be a hashmap { 'foo' -> 'bar' }.
    //
    // You can do any initalization in this new() function, like saving connection
    // info or API url in an variable, but don't do any heavy works like making a
    // database connection or API call.

    fn new(options: &HashMap<String, String>) -> Result<Self, CognitoFdwError> {
        let user_pool_id = require_option("user_pool_id", options)?.to_string();
        let aws_region = require_option("region", options)?.to_string();

        let aws_access_key_id = require_option("aws_access_key_id", options)?.to_string();
        let aws_secret_access_key =
            if let Some(aws_secret_access_key) = options.get("aws_secret_access_key") {
                aws_secret_access_key.clone()
            } else {
                let aws_secret_access_key = options
                    .get("api_key_id")
                    .expect("`api_key_id` must be set if `aws_secret_access_key` is not");
                get_vault_secret(aws_secret_access_key).ok_or(CognitoFdwError::SecretNotFound(
                    aws_secret_access_key.clone(),
                ))?
            };

        let rt = create_async_runtime()?;
        let client = rt.block_on(async {
            env::set_var("AWS_ACCESS_KEY_ID", aws_access_key_id);
            env::set_var("AWS_SECRET_ACCESS_KEY", aws_secret_access_key);
            env::set_var("AWS_REGION", aws_region);
            let config = aws_config::load_defaults(BehaviorVersion::latest()).await;

            let mut builder = config.to_builder();
            if let Some(endpoint_url) = options.get("endpoint_url") {
                if !endpoint_url.is_empty() {
                    builder.set_endpoint_url(Some(endpoint_url.clone()));
                }
            }
            let final_config = builder.build();

            Client::new(&final_config)
        });

        stats::inc_stats(Self::FDW_NAME, stats::Metric::CreateTimes, 1);
        Ok(Self {
            client,
            user_pool_id,
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
        let cognito_client = &self.client;
        let user_pool_id = self.user_pool_id.to_string();
        self.rows_iterator = Some(RowsIterator::new(
            columns.to_vec(),
            user_pool_id,
            cognito_client.clone(),
        ));

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

    fn validator(
        options: Vec<Option<String>>,
        catalog: Option<pg_sys::Oid>,
    ) -> CognitoFdwResult<()> {
        if let Some(oid) = catalog {
            if oid == FOREIGN_SERVER_RELATION_ID {
                let access_key_exists =
                    check_options_contain(&options, "aws_access_key_id").is_ok();
                let secret_access_key_exists =
                    check_options_contain(&options, "aws_secret_access_key").is_ok();
                let api_key_id_exists = check_options_contain(&options, "api_key_id").is_ok();
                if !access_key_exists && !secret_access_key_exists {
                    return Err(CognitoFdwError::ApiKeyAndSecretKeySet);
                }
                if (api_key_id_exists && secret_access_key_exists)
                    || (!api_key_id_exists && !secret_access_key_exists)
                {
                    return Err(CognitoFdwError::SetOneOfSecretKeyAndApiKeyIdSet);
                }
            } else if oid == FOREIGN_TABLE_RELATION_ID {
                check_options_contain(&options, "object")?;
            }
        }

        Ok(())
    }
}
