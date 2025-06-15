use std::collections::HashMap;

use supabase_wrappers::prelude::*;

use super::{DuckdbFdwError, DuckdbFdwResult};

type ServerOptions = HashMap<String, String>;

pub(super) enum ServerType {
    // S3 compatible storage
    S3,
    R2,

    // generic Iceberg services
    Iceberg,

    // specific Iceberg services
    S3Tables,
    R2Catalog,
    Polaris,
    Lakekeeper,
}

impl ServerType {
    pub(super) fn new(svr_opts: &ServerOptions) -> DuckdbFdwResult<Self> {
        let svr_type = require_option("type", svr_opts)?;
        let ret = match svr_type {
            "s3" => Self::S3,
            "r2" => Self::R2,
            "iceberg" => Self::Iceberg,
            "s3_tables" => Self::S3Tables,
            "r2_catalog" => Self::R2Catalog,
            "polaris" => Self::Polaris,
            "lakekeeper" => Self::Lakekeeper,
            _ => return Err(DuckdbFdwError::InvalidServerType(svr_type.to_owned())),
        };
        Ok(ret)
    }

    pub(super) fn as_str(&self) -> &'static str {
        match self {
            Self::S3 => "s3",
            Self::R2 => "r2",
            Self::Iceberg => "iceberg",
            Self::S3Tables => "s3_tables",
            Self::R2Catalog => "r2_catalog",
            Self::Polaris => "polaris",
            Self::Lakekeeper => "lakekeeper",
        }
    }

    pub(super) fn is_iceberg(&self) -> bool {
        matches!(
            self,
            Self::Iceberg | Self::S3Tables | Self::R2Catalog | Self::Polaris | Self::Lakekeeper
        )
    }

    fn allowed_secret_params(&self) -> Vec<&'static str> {
        match self {
            // ref: https://duckdb.org/docs/stable/core_extensions/httpfs/s3api.html#overview-of-s3-secret-parameters
            Self::S3 | Self::S3Tables => vec![
                "endpoint",
                "key_id",
                "region",
                "secret",
                "session_token",
                "url_compatibility_mode",
                "url_style",
                "use_ssl",
                "account_id",
                "kms_key_id",
            ],
            // ref: https://duckdb.org/docs/stable/guides/network_cloud_storage/cloudflare_r2_import.html
            Self::R2 => vec!["key_id", "secret", "account_id"],
            // ref: https://duckdb.org/docs/stable/core_extensions/iceberg/iceberg_rest_catalogs
            Self::Iceberg | Self::R2Catalog | Self::Polaris | Self::Lakekeeper => vec![
                "client_id",
                "client_secret",
                "token",
                "oauth2_scope",
                "oauth2_server_uri",
            ],
        }
    }

    // pick up selected parameters from server options,
    // and form them into a comma-separated string
    fn format_options(&self, svr_opts: &ServerOptions, param_list: &[&str]) -> String {
        svr_opts
            .iter()
            .map(|(k, v)| {
                // get decrypted text from options with 'vault_' prefix
                let value = if k.starts_with("vault_") {
                    get_vault_secret(v).unwrap_or_default()
                } else {
                    v.clone()
                };
                let key = k.strip_prefix("vault_").unwrap_or(k).to_string();
                (key, value)
            })
            .filter(|(k, _)| param_list.contains(&k.as_str()))
            .map(|(k, v)| format!("{} '{}'", k, v.replace("'", "''")))
            .collect::<Vec<_>>()
            .join(",")
    }

    // make 'create secret' sql for DuckDB from server options
    pub(super) fn get_create_secret_sql(&self, svr_opts: &ServerOptions) -> String {
        let secrets: Vec<(&str, Vec<&str>)> = match self {
            Self::S3 | Self::S3Tables => vec![("s3", self.allowed_secret_params())],
            Self::R2 => vec![("r2", self.allowed_secret_params())],

            // note: for generic Iceberg, we only support S3 compatible storage for now,
            // so we need to create 2 secrets: one for S3 and one for Iceberg
            Self::Iceberg => {
                vec![
                    ("s3", Self::S3.allowed_secret_params()),
                    ("iceberg", self.allowed_secret_params()),
                ]
            }

            Self::R2Catalog | Self::Polaris | Self::Lakekeeper => {
                vec![("iceberg", self.allowed_secret_params())]
            }
        };

        let mut ret = String::default();
        for (typ, params) in secrets {
            let opts = self.format_options(svr_opts, &params);
            ret.push_str(&format!("create or replace secret (type {typ}, {opts});"));
        }

        ret
    }

    pub(super) fn get_attach_sql(&self, svr_opts: &ServerOptions) -> DuckdbFdwResult<String> {
        let ret = match self {
            Self::S3Tables => {
                let arn = require_option("s3_tables_arn", svr_opts)?;
                let db_name = self.as_str();
                format!(
                    "
                    attach '{arn}' as {db_name} (
                        type iceberg,
                        endpoint_type s3_tables  
                    );"
                )
            }
            Self::Iceberg | Self::R2Catalog | Self::Polaris | Self::Lakekeeper => {
                // ref: https://duckdb.org/docs/stable/core_extensions/iceberg/iceberg_rest_catalogs#specific-catalog-examples
                let warehouse = require_option("warehouse", svr_opts)?;
                let catalog_uri = require_option("catalog_uri", svr_opts)?;
                let db_name = self.as_str();
                format!(
                    "
                    attach '{warehouse}' as {db_name} (
                        type iceberg,
                        endpoint '{catalog_uri}'
                    );"
                )
            }
            _ => String::default(),
        };
        Ok(ret)
    }
}
