use std::env;
use std::sync::Arc;

use aws_sdk_cognitoidentityprovider::{config::BehaviorVersion, Client};

use crate::stats;
use pgrx::pg_sys;
use std::collections::HashMap;
use supabase_wrappers::prelude::*;

use super::cognito_client::rows_iterator::RowsIterator;
use super::{CognitoFdwError, CognitoFdwResult};

#[wrappers_fdw(
    version = "0.1.4",
    author = "Joel",
    website = "https://github.com/supabase/wrappers/tree/main/wrappers/src/fdw/cognito_fdw",
    error_type = "CognitoFdwError"
)]
pub(crate) struct CognitoFdw {
    rt: Arc<Runtime>,
    client: aws_sdk_cognitoidentityprovider::Client,
    user_pool_id: String,
    rows_iterator: Option<RowsIterator>,
}

impl CognitoFdw {
    const FDW_NAME: &'static str = "CognitoFdw";
}

impl ForeignDataWrapper<CognitoFdwError> for CognitoFdw {
    fn new(server: ForeignServer) -> Result<Self, CognitoFdwError> {
        let user_pool_id = require_option("user_pool_id", &server.options)?.to_string();
        let aws_region = require_option("region", &server.options)?.to_string();

        let aws_access_key_id = require_option("aws_access_key_id", &server.options)?.to_string();
        let aws_secret_access_key =
            if let Some(aws_secret_access_key) = server.options.get("aws_secret_access_key") {
                aws_secret_access_key.clone()
            } else {
                let aws_secret_access_key = server
                    .options
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
            if let Some(endpoint_url) = server.options.get("endpoint_url") {
                if !endpoint_url.is_empty() {
                    builder.set_endpoint_url(Some(endpoint_url.clone()));
                }
            }
            let final_config = builder.build();

            Client::new(&final_config)
        });

        stats::inc_stats(Self::FDW_NAME, stats::Metric::CreateTimes, 1);

        Ok(Self {
            rt: Arc::new(rt),
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
        self.rows_iterator = Some(RowsIterator::new(
            self.rt.clone(),
            columns.to_vec(),
            self.user_pool_id.clone(),
            self.client.clone(),
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

    fn import_foreign_schema(
        &mut self,
        stmt: ImportForeignSchemaStmt,
    ) -> CognitoFdwResult<Vec<String>> {
        Ok(vec![format!(
            r#"create foreign table if not exists users (
                username text,
                email text,
                status text,
                enabled boolean,
                created_at timestamp,
                updated_at timestamp,
                attributes jsonb
            )
            server {} options (object 'users')"#,
            stmt.server_name
        )])
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
