use crate::fdw::cognito_fdw::cognito_client::rows_iterator::RowsIterator;
use crate::fdw::cognito_fdw::cognito_client::CognitoClient;
use crate::fdw::cognito_fdw::cognito_client::CognitoClientError;

use pgrx::notice;
use std::env;

use aws_sdk_cognitoidentityprovider::{config::Region, Client};

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
    // create server my_cognito_server
    //   foreign data wrapper wrappers_cognito
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
        // let url = require_option("url", options)?.to_string();
        //     let user_pool_id = require_option("user_pool_id", options)?.to_string();
        //     let aws_region = require_option("region", options)?.to_string();
        notice!("we here");

        let creds = {
            // if using credentials directly specified
            let aws_access_key_id = require_option("aws_access_key_id", options)?.to_string();
            notice!("{:?}", aws_access_key_id);
            let aws_secret_access_key =
                require_option("aws_secret_access_key", options)?.to_string();
            Some((aws_access_key_id, aws_secret_access_key))
        };

        let Some(creds) = creds else {
            panic!("this shouldn't happen");
        };
        // set AWS environment variables and create shared config from them

        let rt = tokio::runtime::Runtime::new()
            .map_err(CreateRuntimeError::FailedToCreateAsyncRuntime)?;
        let client = rt.block_on(async {
            let region = "ap-southeast-2".to_string();

            env::set_var("AWS_ACCESS_KEY_ID", creds.0);
            env::set_var("AWS_SECRET_ACCESS_KEY", creds.1);
            env::set_var("AWS_REGION", region);
            let config = aws_config::from_env().load().await;

            return Client::new(&config);
        });

        rt.block_on(async {
            match client
                .list_users()
                .set_user_pool_id(Some("ap-southeast-2_xuUGae0Bl".to_string()))
                .send()
                .await
            {
                Ok(response) => {
                    // handling response
                    notice!("some response");
                    if let Some(users) = response.users {
                        for user in users {
                            // Here you can print or process each user
                            notice!("{:?}", user);
                        }
                    } else {
                        notice!("No users found.");
                    }
                    if let Some(token) = response.pagination_token {
                        notice!("Pagination token for next request: {}", token);
                    }
                }
                Err(e) => {
                    notice!("Error: {:?}", e);
                }
            }
        });
        notice!("we finished");

        stats::inc_stats(Self::FDW_NAME, stats::Metric::CreateTimes, 1);
        Ok(Self {
            client: client,
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
        self.rows_iterator = Some(RowsIterator::new(
            columns.to_vec(),
            1000,
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
        // TODO: Update fields that we validate on
        // if let Some(oid) = catalog {
        //     if oid == FOREIGN_SERVER_RELATION_ID {
        //        // let api_key_exists = check_options_contain(&options, "api_key").is_ok();
        //         // let url_exists = check_options_contain(&options, "url").is_ok();
        //         if (api_key_exists && api_key_id_exists) || (!api_key_exists && !api_key_id_exists)
        //         {
        //             return Err(CognitoFdwError::SetOneOfApiKeyAndApiKeyIdSet);
        //         }
        //         if !url_exists {
        //             return Err(CognitoFdwError::URLOptionMissing);
        //         }
        //     } else if oid == FOREIGN_TABLE_RELATION_ID {
        //         check_options_contain(&options, "object")?;
        //     }
        // }

        Ok(())
    }
}
