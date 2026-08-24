//! Object store access for Zarr reading.
//!
//! MVP backend: S3 (via `aws-sdk-s3`), mirroring the setup used by `s3_fdw`.
//! GCS/Azure/local/sshfs arrive on the v1 roadmap; the `ZarrStore` wrapper is
//! deliberately narrow (`get_object`) so those backends slot in without
//! touching the scan path.

use aws_config::BehaviorVersion;
use aws_sdk_s3 as s3;
use http::Uri;
use pgrx::pg_sys;
use std::collections::HashMap;
use std::future::Future;
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::time::{MissedTickBehavior, interval};

use supabase_wrappers::prelude::*;

use super::{ZarrFdwError, ZarrFdwResult};

/// Metadata objects are tiny JSON documents. Chunk callers provide a tighter
/// limit derived from their declared decoded layout.
pub(crate) const MAX_METADATA_OBJECT_BYTES: usize = 1024 * 1024;
const INTERRUPT_POLL_INTERVAL: Duration = Duration::from_millis(25);

enum Interruptible<T> {
    Ready(T),
    Interrupted,
}

fn postgres_interrupt_pending() -> bool {
    // PostgreSQL declares this as volatile sig_atomic_t because signal
    // handlers update it asynchronously.
    unsafe { std::ptr::read_volatile(&raw const pg_sys::InterruptPending) != 0 }
}

fn process_postgres_interrupts() {
    unsafe {
        if postgres_interrupt_pending() {
            pg_sys::ProcessInterrupts();
        }
    }
}

async fn await_interruptibly<F>(future: F) -> Interruptible<F::Output>
where
    F: Future,
{
    tokio::pin!(future);
    if postgres_interrupt_pending() {
        return Interruptible::Interrupted;
    }

    let mut ticker = interval(INTERRUPT_POLL_INTERVAL);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    ticker.tick().await;
    loop {
        tokio::select! {
            biased;
            _ = ticker.tick() => {
                if postgres_interrupt_pending() {
                    return Interruptible::Interrupted;
                }
            }
            result = &mut future => return Interruptible::Ready(result),
        }
    }
}

/// One bounded page of immediate child prefixes below a Zarr group.
///
/// S3's delimiter keeps discovery at the metadata hierarchy level. Array
/// nodes are identified with an exact `.zarray` GET before listing, so slash-
/// separated chunk keys are never traversed as groups.
pub(crate) struct DirectoryPage {
    pub child_prefixes: Vec<String>,
    pub next_continuation_token: Option<String>,
}

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

    fn relative_key(&self, key: &str) -> Option<String> {
        let key = key.trim_matches('/');
        if self.prefix.is_empty() {
            return Some(key.to_string());
        }
        if key == self.prefix {
            return Some(String::new());
        }
        key.strip_prefix(&format!("{}/", self.prefix))
            .map(str::to_string)
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AuthMode {
    Anonymous,
    Direct,
    Vault,
    ProviderChain,
}

fn boolean_option(options: &HashMap<String, String>, name: &str) -> ZarrFdwResult<bool> {
    match options.get(name).map(String::as_str) {
        None | Some("false") => Ok(false),
        Some("true") => Ok(true),
        Some(_) => Err(ZarrFdwError::InvalidOptionValue {
            option: name.to_string(),
            message: "must be 'true' or 'false'".to_string(),
        }),
    }
}

/// Validate the mutually exclusive authentication modes and return the one to
/// use. `anonymous=false` is neutral, so an option-free configuration keeps
/// the AWS SDK provider-chain behavior.
pub(crate) fn validate_auth_options(options: &HashMap<String, String>) -> ZarrFdwResult<AuthMode> {
    let anonymous = boolean_option(options, "anonymous")?;
    let _path_style_url = boolean_option(options, "path_style_url")?;

    let direct_id = options.contains_key("aws_access_key_id");
    let direct_secret = options.contains_key("aws_secret_access_key");
    let vault_id = options.contains_key("vault_access_key_id");
    let vault_secret = options.contains_key("vault_secret_access_key");

    match (direct_id, direct_secret) {
        (true, false) => {
            require_option("aws_secret_access_key", options)?;
        }
        (false, true) => {
            require_option("aws_access_key_id", options)?;
        }
        _ => {}
    }
    match (vault_id, vault_secret) {
        (true, false) => {
            require_option("vault_secret_access_key", options)?;
        }
        (false, true) => {
            require_option("vault_access_key_id", options)?;
        }
        _ => {}
    }

    for name in [
        "aws_access_key_id",
        "aws_secret_access_key",
        "vault_access_key_id",
        "vault_secret_access_key",
    ] {
        if options
            .get(name)
            .is_some_and(|value| value.trim().is_empty())
        {
            return Err(ZarrFdwError::InvalidOptionValue {
                option: name.to_string(),
                message: "must not be empty".to_string(),
            });
        }
    }

    let has_direct = direct_id && direct_secret;
    let has_vault = vault_id && vault_secret;
    if anonymous && (has_direct || has_vault) {
        return Err(ZarrFdwError::InvalidAuthenticationOptions(
            "anonymous authentication cannot be combined with explicit credentials".to_string(),
        ));
    }
    if has_direct && has_vault {
        return Err(ZarrFdwError::InvalidAuthenticationOptions(
            "direct and Vault credentials cannot both be configured".to_string(),
        ));
    }

    Ok(if anonymous {
        AuthMode::Anonymous
    } else if has_direct {
        AuthMode::Direct
    } else if has_vault {
        AuthMode::Vault
    } else {
        AuthMode::ProviderChain
    })
}

/// Object store client.
pub(crate) struct ZarrStore {
    pub rt: Runtime,
    pub client: s3::Client,
    pub url: StoreUrl,
}

impl ZarrStore {
    fn block_on_interruptibly<T, F>(&self, future: F) -> ZarrFdwResult<T>
    where
        F: Future<Output = ZarrFdwResult<T>>,
    {
        match self.rt.block_on(await_interruptibly(future)) {
            Interruptible::Ready(result) => result,
            Interruptible::Interrupted => {
                // `await_interruptibly` and its owned SDK future have been
                // dropped before PostgreSQL is allowed to raise ERROR.
                process_postgres_interrupts();
                Err(ZarrFdwError::InvalidMetadata(
                    "query interruption was requested".to_string(),
                ))
            }
        }
    }

    /// Build an S3-backed store from `CREATE SERVER` options.
    pub fn new(server: &ForeignServer) -> ZarrFdwResult<Self> {
        // Cannot use create_async_runtime() as the runtime needs multiple threads
        let rt = tokio::runtime::Runtime::new()
            .map_err(CreateRuntimeError::FailedToCreateAsyncRuntime)?;

        let store_url = require_option("store_url", &server.options)?;
        let url = StoreUrl::parse(store_url)?;

        let auth_mode = validate_auth_options(&server.options)?;
        let client = match auth_mode {
            AuthMode::Anonymous => Self::build_client(&rt, &server.options, ClientAuth::Anonymous),
            AuthMode::Direct => {
                let access_key = require_option("aws_access_key_id", &server.options)?.to_string();
                let secret_key =
                    require_option("aws_secret_access_key", &server.options)?.to_string();
                Self::build_client(
                    &rt,
                    &server.options,
                    ClientAuth::Static {
                        access_key,
                        secret_key,
                    },
                )
            }
            AuthMode::Vault => {
                let vault_access_key_id = require_option("vault_access_key_id", &server.options)?;
                let vault_secret_access_key =
                    require_option("vault_secret_access_key", &server.options)?;
                let access_key = get_vault_secret(vault_access_key_id).ok_or_else(|| {
                    ZarrFdwError::VaultSecretNotFound {
                        option: "vault_access_key_id".to_string(),
                    }
                })?;
                let secret_key = get_vault_secret(vault_secret_access_key).ok_or_else(|| {
                    ZarrFdwError::VaultSecretNotFound {
                        option: "vault_secret_access_key".to_string(),
                    }
                })?;
                Self::build_client(
                    &rt,
                    &server.options,
                    ClientAuth::Static {
                        access_key,
                        secret_key,
                    },
                )
            }
            AuthMode::ProviderChain => {
                Self::build_client(&rt, &server.options, ClientAuth::ProviderChain)
            }
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

    /// Fetch a full object by key (relative to the store prefix), returning
    /// `None` only when S3 explicitly reports `NoSuchKey` or HTTP 404.
    pub async fn get_object_optional(
        &self,
        key: &str,
        max_bytes: usize,
    ) -> ZarrFdwResult<Option<Vec<u8>>> {
        self.get_object_optional_owned(key.to_string(), max_bytes)
            .await
    }

    /// Create an owned object fetch future suitable for the foreground
    /// prefetch window. The future owns cloned SDK/url state and therefore
    /// never borrows the PostgreSQL scan object while it is queued.
    pub fn get_object_optional_owned(
        &self,
        key: String,
        max_bytes: usize,
    ) -> impl Future<Output = ZarrFdwResult<Option<Vec<u8>>>> + 'static {
        get_object_optional_owned(self.client.clone(), self.url.clone(), key, max_bytes)
    }

    /// Synchronous optional fetch used for sparse Zarr chunks.
    pub fn get_object_optional_sync(
        &self,
        key: &str,
        max_bytes: usize,
    ) -> ZarrFdwResult<Option<Vec<u8>>> {
        self.block_on_interruptibly(self.get_object_optional(key, max_bytes))
    }

    /// List one bounded page of immediate child prefixes below `path`.
    ///
    /// The caller owns pagination and global discovery limits. Only common
    /// prefixes are returned; ordinary objects (including dot-separated chunk
    /// keys) are deliberately ignored.
    pub async fn list_directory_page(
        &self,
        path: &str,
        continuation_token: Option<String>,
    ) -> ZarrFdwResult<DirectoryPage> {
        let key = self.url.object_key(path);
        let prefix = if key.is_empty() {
            String::new()
        } else {
            format!("{}/", key.trim_end_matches('/'))
        };
        let response = self
            .client
            .list_objects_v2()
            .bucket(&self.url.bucket)
            .prefix(prefix)
            .delimiter("/")
            .max_keys(1000)
            .set_continuation_token(continuation_token)
            .send()
            .await?;

        let mut child_prefixes = response
            .common_prefixes
            .unwrap_or_default()
            .into_iter()
            .filter_map(|entry| entry.prefix)
            .filter_map(|entry| self.url.relative_key(&entry))
            .map(|entry| entry.trim_matches('/').to_string())
            .collect::<Vec<_>>();
        child_prefixes.sort();
        child_prefixes.dedup();

        let next_continuation_token = if response.is_truncated.unwrap_or(false) {
            response.next_continuation_token
        } else {
            None
        };
        if response.is_truncated.unwrap_or(false) && next_continuation_token.is_none() {
            return Err(ZarrFdwError::InvalidMetadata(
                "S3 returned a truncated listing without a continuation token".to_string(),
            ));
        }

        Ok(DirectoryPage {
            child_prefixes,
            next_continuation_token,
        })
    }

    pub fn list_directory_page_sync(
        &self,
        path: &str,
        continuation_token: Option<String>,
    ) -> ZarrFdwResult<DirectoryPage> {
        self.block_on_interruptibly(self.list_directory_page(path, continuation_token))
    }
}

async fn get_object_optional_owned(
    client: s3::Client,
    url: StoreUrl,
    key: String,
    max_bytes: usize,
) -> ZarrFdwResult<Option<Vec<u8>>> {
    let full_key = url.object_key(&key);

    let resp = match client
        .get_object()
        .bucket(&url.bucket)
        .key(&full_key)
        .send()
        .await
    {
        Ok(response) => response,
        Err(error) => {
            let modeled_no_such_key = error
                .as_service_error()
                .is_some_and(|error| error.is_no_such_key());
            let status = error
                .raw_response()
                .map(|response| response.status().as_u16());
            if is_missing_object_response(modeled_no_such_key, status) {
                return Ok(None);
            }
            return Err(error.into());
        }
    };
    if let Some(content_length) = resp.content_length {
        let content_length = usize::try_from(content_length).map_err(|_| {
            ZarrFdwError::InvalidMetadata(format!(
                "object '{full_key}' length exceeds this platform's index capacity"
            ))
        })?;
        if content_length > max_bytes {
            return Err(ZarrFdwError::InvalidMetadata(format!(
                "object '{full_key}' is {content_length} bytes, exceeding the read limit of {max_bytes}"
            )));
        }
    }
    Ok(Some(
        read_bounded_object(resp.body.into_async_read(), max_bytes, &full_key).await?,
    ))
}

async fn read_bounded_object<R>(
    mut reader: R,
    max_bytes: usize,
    key: &str,
) -> ZarrFdwResult<Vec<u8>>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut buf = Vec::new();
    let mut block = [0u8; 64 * 1024];
    while buf.len() < max_bytes {
        let remaining = max_bytes - buf.len();
        let read_len = remaining.min(block.len());
        let count = reader.read(&mut block[..read_len]).await?;
        if count == 0 {
            return Ok(buf);
        }
        buf.try_reserve_exact(count).map_err(|_| {
            ZarrFdwError::InvalidMetadata(format!(
                "could not grow the object read buffer for '{key}'"
            ))
        })?;
        buf.extend_from_slice(&block[..count]);
    }

    // Probe one byte beyond the cap without growing the result vector.
    let mut extra = [0u8; 1];
    if reader.read(&mut extra).await? != 0 {
        return Err(ZarrFdwError::InvalidMetadata(format!(
            "object '{key}' exceeds the read limit of {max_bytes} bytes"
        )));
    }
    Ok(buf)
}

fn is_missing_object_response(modeled_no_such_key: bool, status: Option<u16>) -> bool {
    modeled_no_such_key || status == Some(404)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn options(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect()
    }

    #[test]
    fn bounded_object_reader_accepts_limit_and_rejects_limit_plus_one() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        assert_eq!(
            rt.block_on(read_bounded_object(
                std::io::Cursor::new(b"1234"),
                4,
                "exact"
            ))
            .unwrap(),
            b"1234"
        );
        assert!(
            rt.block_on(read_bounded_object(
                std::io::Cursor::new(b"12345"),
                4,
                "too-large"
            ))
            .is_err()
        );
    }

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

    #[test]
    fn converts_list_prefixes_back_to_store_relative_paths() {
        let url = StoreUrl::parse("s3://warehouse/zarr/e2e.zarr").unwrap();

        assert_eq!(
            url.relative_key("zarr/e2e.zarr/nested/raw/"),
            Some("nested/raw".to_string())
        );
        assert_eq!(url.relative_key("zarr/e2e.zarr"), Some(String::new()));
        assert_eq!(url.relative_key("other/prefix"), None);
    }

    #[test]
    fn only_no_such_key_or_http_404_is_optional_absence() {
        assert!(is_missing_object_response(true, None));
        assert!(is_missing_object_response(false, Some(404)));
        assert!(!is_missing_object_response(false, None));
        assert!(!is_missing_object_response(false, Some(403)));
        assert!(!is_missing_object_response(false, Some(500)));
    }

    #[test]
    fn selects_each_supported_auth_mode() {
        assert_eq!(
            validate_auth_options(&options(&[])).unwrap(),
            AuthMode::ProviderChain
        );
        assert_eq!(
            validate_auth_options(&options(&[("anonymous", "true")])).unwrap(),
            AuthMode::Anonymous
        );
        assert_eq!(
            validate_auth_options(&options(&[("anonymous", "false")])).unwrap(),
            AuthMode::ProviderChain
        );
        assert_eq!(
            validate_auth_options(&options(&[
                ("aws_access_key_id", "key"),
                ("aws_secret_access_key", "secret"),
            ]))
            .unwrap(),
            AuthMode::Direct
        );
        assert_eq!(
            validate_auth_options(&options(&[
                ("vault_access_key_id", "key-id"),
                ("vault_secret_access_key", "secret-id"),
            ]))
            .unwrap(),
            AuthMode::Vault
        );
        assert_eq!(
            validate_auth_options(&options(&[
                ("anonymous", "false"),
                ("aws_access_key_id", "key"),
                ("aws_secret_access_key", "secret"),
            ]))
            .unwrap(),
            AuthMode::Direct
        );
        assert_eq!(
            validate_auth_options(&options(&[
                ("anonymous", "false"),
                ("vault_access_key_id", "key-id"),
                ("vault_secret_access_key", "secret-id"),
            ]))
            .unwrap(),
            AuthMode::Vault
        );
    }

    #[test]
    fn rejects_invalid_boolean_options() {
        for (name, value) in [("anonymous", "TRUE"), ("path_style_url", "yes")] {
            let err = validate_auth_options(&options(&[(name, value)])).unwrap_err();
            assert_eq!(
                err.to_string(),
                format!("invalid value for option '{name}': must be 'true' or 'false'")
            );
        }
    }

    #[test]
    fn rejects_partial_credential_pairs() {
        for (present, missing) in [
            ("aws_access_key_id", "aws_secret_access_key"),
            ("aws_secret_access_key", "aws_access_key_id"),
            ("vault_access_key_id", "vault_secret_access_key"),
            ("vault_secret_access_key", "vault_access_key_id"),
        ] {
            let err = validate_auth_options(&options(&[(present, "value")])).unwrap_err();
            assert_eq!(
                err.to_string(),
                format!("required option `{missing}` is not specified")
            );
        }
    }

    #[test]
    fn rejects_empty_credentials_without_echoing_values() {
        for (pairs, empty_option) in [
            (
                vec![
                    ("aws_access_key_id", ""),
                    ("aws_secret_access_key", "secret"),
                ],
                "aws_access_key_id",
            ),
            (
                vec![
                    ("vault_access_key_id", "key-id"),
                    ("vault_secret_access_key", "  "),
                ],
                "vault_secret_access_key",
            ),
        ] {
            let err = validate_auth_options(&options(&pairs)).unwrap_err();
            assert_eq!(
                err.to_string(),
                format!("invalid value for option '{empty_option}': must not be empty")
            );
        }
    }

    #[test]
    fn rejects_conflicting_authentication_modes() {
        let anonymous_and_direct = options(&[
            ("anonymous", "true"),
            ("aws_access_key_id", "key"),
            ("aws_secret_access_key", "secret"),
        ]);
        assert_eq!(
            validate_auth_options(&anonymous_and_direct)
                .unwrap_err()
                .to_string(),
            "invalid authentication options: anonymous authentication cannot be combined with explicit credentials"
        );

        let anonymous_and_vault = options(&[
            ("anonymous", "true"),
            ("vault_access_key_id", "key-id"),
            ("vault_secret_access_key", "secret-id"),
        ]);
        assert_eq!(
            validate_auth_options(&anonymous_and_vault)
                .unwrap_err()
                .to_string(),
            "invalid authentication options: anonymous authentication cannot be combined with explicit credentials"
        );

        let direct_and_vault = options(&[
            ("aws_access_key_id", "key"),
            ("aws_secret_access_key", "secret"),
            ("vault_access_key_id", "key-id"),
            ("vault_secret_access_key", "secret-id"),
        ]);
        assert_eq!(
            validate_auth_options(&direct_and_vault)
                .unwrap_err()
                .to_string(),
            "invalid authentication options: direct and Vault credentials cannot both be configured"
        );
    }

    #[test]
    fn routing_options_do_not_change_authentication_mode() {
        let opts = options(&[
            ("endpoint_url", "http://localhost:9000"),
            ("path_style_url", "true"),
        ]);
        assert_eq!(
            validate_auth_options(&opts).unwrap(),
            AuthMode::ProviderChain
        );
    }
}
