//! Object store access for Zarr reading.
//!
//! MVP backend: S3 (via `aws-sdk-s3`), mirroring the setup used by `s3_fdw`.
//! GCS/Azure/local/sshfs arrive on the v1 roadmap; the `ZarrStore` wrapper is
//! deliberately narrow (`get_object`) so those backends slot in without
//! touching the scan path.

use aws_config::BehaviorVersion;
use aws_sdk_s3 as s3;
use http::Uri;
use std::collections::HashMap;
use tokio::io::AsyncReadExt;

use supabase_wrappers::prelude::*;

use super::{ZarrFdwError, ZarrFdwResult};

/// A parsed S3 `s3://bucket/prefix` location.
#[derive(Debug, Clone)]
pub(crate) struct StoreUrl {
    pub bucket: String,
    /// Object-key prefix (no leading `/`), e.g. `sentinel2/2025.zarr`.
    pub prefix: String,
}

impl StoreUrl {
    pub fn parse(s: &str) -> ZarrFdwResult<Self> {
        let uri = s
            .parse::<Uri>()
            .map_err(|_| ZarrFdwError::InvalidStoreUrl(s.to_string()))?;
        if uri.scheme_str() != Some("s3") || uri.host().is_none() {
            return Err(ZarrFdwError::InvalidStoreUrl(s.to_string()));
        }
        let bucket = uri.host().expect("host checked above").to_owned();
        let prefix = uri.path().trim_matches('/').to_string();
        Ok(Self { bucket, prefix })
    }

    fn object_key(&self, key: &str) -> String {
        join_key(&self.prefix, key)
    }
}

/// Join two S3 object-key fragments without introducing or duplicating `/`.
pub(crate) fn join_key(prefix: &str, key: &str) -> String {
    let prefix = prefix.trim_matches('/');
    let key = key.trim_matches('/');
    match (prefix.is_empty(), key.is_empty()) {
        (true, _) => key.to_string(),
        (_, true) => prefix.to_string(),
        (false, false) => format!("{prefix}/{key}"),
    }
}

enum ClientAuth {
    Anonymous,
    Static {
        access_key: String,
        secret_key: String,
    },
    ProviderChain,
}

/// Object store client.
pub(crate) struct ZarrStore {
    pub rt: Runtime,
    pub client: Option<s3::Client>,
    pub url: StoreUrl,
}

impl ZarrStore {
    /// Build an S3-backed store from `CREATE SERVER` options.
    pub fn new(server: &ForeignServer) -> ZarrFdwResult<Self> {
        // Cannot use create_async_runtime() as the runtime needs multiple threads
        let rt = tokio::runtime::Runtime::new()
            .map_err(CreateRuntimeError::FailedToCreateAsyncRuntime)?;

        let store_url = require_option("store_url", &server.options)?;
        let url = StoreUrl::parse(store_url)?;

        let client = match &server.options {
            opts if opts.get("anonymous").map(String::as_str) == Some("true") => {
                Some(Self::build_client(&rt, opts, ClientAuth::Anonymous))
            }
            opts if opts.contains_key("vault_access_key_id") => {
                let vault_access_key_id = require_option("vault_access_key_id", opts)?.to_string();
                let vault_secret_access_key =
                    require_option("vault_secret_access_key", opts)?.to_string();
                let access_key = get_vault_secret(&vault_access_key_id);
                let secret_key = get_vault_secret(&vault_secret_access_key);
                match (access_key, secret_key) {
                    (Some(k), Some(s)) => Some(Self::build_client(
                        &rt,
                        opts,
                        ClientAuth::Static {
                            access_key: k,
                            secret_key: s,
                        },
                    )),
                    _ => None,
                }
            }
            opts if opts.contains_key("aws_access_key_id") => {
                let access_key = require_option("aws_access_key_id", opts)?.to_string();
                let secret_key = require_option("aws_secret_access_key", opts)?.to_string();
                Some(Self::build_client(
                    &rt,
                    opts,
                    ClientAuth::Static {
                        access_key,
                        secret_key,
                    },
                ))
            }
            opts => Some(Self::build_client(&rt, opts, ClientAuth::ProviderChain)),
        };

        Ok(Self { rt, client, url })
    }

    fn build_client(rt: &Runtime, opts: &HashMap<String, String>, auth: ClientAuth) -> s3::Client {
        let region = require_option_or("aws_region", opts, "us-east-1");
        let mut config_loader = aws_config::defaults(BehaviorVersion::latest())
            .region(s3::config::Region::new(region.to_string()));
        config_loader = match auth {
            ClientAuth::Anonymous => config_loader.no_credentials(),
            ClientAuth::Static {
                access_key,
                secret_key,
            } => config_loader.credentials_provider(s3::config::Credentials::new(
                access_key, secret_key, None, None, "zarr_fdw",
            )),
            ClientAuth::ProviderChain => config_loader,
        };
        // endpoint_url not supported as env var in rust https://github.com/awslabs/aws-sdk-rust/issues/932
        if let Some(endpoint_url) = opts.get("endpoint_url") {
            if endpoint_url.ends_with('/') {
                config_loader = config_loader.endpoint_url(endpoint_url);
            } else {
                config_loader = config_loader.endpoint_url(format!("{endpoint_url}/"));
            };
        }

        let config = rt.block_on(config_loader.load());
        let path_style_url = opts.get("path_style_url").map(|s| s.as_str()) == Some("true");
        let mut s3_config_builder = s3::config::Builder::from(&config);
        s3_config_builder = s3_config_builder.force_path_style(path_style_url);
        s3::Client::from_conf(s3_config_builder.build())
    }

    /// Fetch a full object by key (relative to the store prefix).
    pub async fn get_object(&self, key: &str) -> ZarrFdwResult<Vec<u8>> {
        let full_key = self.url.object_key(key);

        match &self.client {
            Some(client) => {
                let resp = client
                    .get_object()
                    .bucket(&self.url.bucket)
                    .key(&full_key)
                    .send()
                    .await?;
                let mut buf = Vec::new();
                resp.body.into_async_read().read_to_end(&mut buf).await?;
                Ok(buf)
            }
            None => Err(ZarrFdwError::InvalidStoreUrl(format!(
                "no credentials configured for server; cannot read 's3://{}/{}'",
                self.url.bucket, full_key
            ))),
        }
    }

    /// Synchronous fetch used from `begin_scan`/`iter_scan` (FDW callbacks are
    /// not async).
    pub fn get_object_sync(&self, key: &str) -> ZarrFdwResult<Vec<u8>> {
        self.rt.block_on(self.get_object(key))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_store_url() {
        let u = StoreUrl::parse("s3://cinecube/sentinel2/2025.zarr").unwrap();
        assert_eq!(u.bucket, "cinecube");
        assert_eq!(u.prefix, "sentinel2/2025.zarr");
    }

    #[test]
    fn parse_store_url_root() {
        let u = StoreUrl::parse("s3://cinecube/").unwrap();
        assert_eq!(u.bucket, "cinecube");
        assert_eq!(u.prefix, "");
    }

    #[test]
    fn parse_store_url_without_slash() {
        let u = StoreUrl::parse("s3://cinecube").unwrap();
        assert_eq!(u.bucket, "cinecube");
        assert_eq!(u.prefix, "");
    }

    #[test]
    fn reject_non_s3() {
        assert!(StoreUrl::parse("https://example.com/x").is_err());
    }

    #[test]
    fn parse_store_url_trailing_slash() {
        let u = StoreUrl::parse("s3://bucket/k.alt/").unwrap();
        assert_eq!(u.prefix, "k.alt");
    }

    #[test]
    fn joins_object_key_prefix_once() {
        let u = StoreUrl::parse("s3://bucket/grid/data.zarr").unwrap();
        assert_eq!(
            u.object_key("longitude/.zarray"),
            "grid/data.zarr/longitude/.zarray"
        );
        assert_eq!(u.object_key("/x/0/"), "grid/data.zarr/x/0");
    }

    #[test]
    fn joins_root_object_key_without_leading_slash() {
        let u = StoreUrl::parse("s3://bucket/").unwrap();
        assert_eq!(u.object_key("/.zarray"), ".zarray");
    }
}
