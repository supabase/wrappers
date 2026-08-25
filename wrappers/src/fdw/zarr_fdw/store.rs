//! Bounded storage access for Zarr reading.
//!
//! The scan-facing [`ZarrStore`] owns PostgreSQL interruption handling while
//! backend implementations own format-specific object and directory access.

mod http;
mod local;

use ::http::Uri;
use aws_config::BehaviorVersion;
use aws_sdk_s3 as s3;
use futures_util::FutureExt;
use futures_util::future::LocalBoxFuture;
use pgrx::pg_sys;
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::time::{MissedTickBehavior, interval};

use supabase_wrappers::prelude::*;

use self::http::HttpBackend;
use self::local::LocalBackend;
use super::{ZarrFdwError, ZarrFdwResult};

/// Metadata objects are tiny JSON documents. Chunk callers provide a tighter
/// limit derived from their declared decoded layout.
pub(crate) const MAX_METADATA_OBJECT_BYTES: usize = 1024 * 1024;
const INTERRUPT_POLL_INTERVAL: Duration = Duration::from_millis(25);

pub(crate) type StoreFuture<T> = LocalBoxFuture<'static, ZarrFdwResult<T>>;

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub(crate) enum StorageBackendKind {
    S3,
    Local,
    Http,
}

impl StorageBackendKind {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::S3 => "s3",
            Self::Local => "local",
            Self::Http => "http",
        }
    }

    pub(crate) fn effective_max_concurrent_reads(self, configured: usize) -> usize {
        match self {
            Self::S3 | Self::Http => configured,
            // Local reads are poll-driven foreground file I/O. Scheduling
            // more than one cannot create useful kernel concurrency.
            Self::Local => 1,
        }
    }
}

pub(crate) trait StorageBackend: Send + Sync {
    fn kind(&self) -> StorageBackendKind;

    fn get_object_owned(&self, key: String, max_bytes: usize) -> StoreFuture<Option<Vec<u8>>>;

    fn get_range_owned(&self, identity: ReadIdentity) -> StoreFuture<Option<RangedObject>>;

    fn list_directory_page_owned(
        &self,
        path: String,
        continuation_token: Option<String>,
    ) -> StoreFuture<DirectoryPage>;
}

/// The exact object bytes represented by one storage read/cache entry.
///
/// Suffix requests are useful for end-located shard indexes. Successful
/// suffix reads are normalized to [`ReadRange::Exact`] in [`RangedObject`].
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub(crate) enum ReadRange {
    Whole,
    Exact { start: u64, length: u64 },
    Suffix { length: u64 },
}

/// Observed identity of one backend object generation.
///
/// S3's `version_id` is deliberately observational. Follow-up S3 and HTTP
/// reads use `If-Match`; local reads compare a capability-relative file
/// fingerprint. Variants make cross-backend cache/generation reuse impossible.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub(crate) enum ObjectGeneration {
    S3 {
        etag: String,
        version_id: Option<String>,
        total_len: u64,
    },
    Http {
        etag: String,
        total_len: u64,
    },
    Local {
        fingerprint: String,
        total_len: u64,
    },
}

impl ObjectGeneration {
    pub(crate) fn backend_kind(&self) -> StorageBackendKind {
        match self {
            Self::S3 { .. } => StorageBackendKind::S3,
            Self::Local { .. } => StorageBackendKind::Local,
            Self::Http { .. } => StorageBackendKind::Http,
        }
    }

    pub(crate) fn total_len(&self) -> u64 {
        match self {
            Self::S3 { total_len, .. }
            | Self::Local { total_len, .. }
            | Self::Http { total_len, .. } => *total_len,
        }
    }

    pub(crate) fn validator_is_empty(&self) -> bool {
        match self {
            Self::S3 { etag, .. } | Self::Http { etag, .. } => etag.is_empty(),
            Self::Local { fingerprint, .. } => fingerprint.is_empty(),
        }
    }

    pub(crate) fn s3_etag(&self) -> Option<&str> {
        match self {
            Self::S3 { etag, .. } => Some(etag),
            _ => None,
        }
    }

    pub(crate) fn http_etag(&self) -> Option<&str> {
        match self {
            Self::Http { etag, .. } => Some(etag),
            _ => None,
        }
    }
}

/// Complete identity for a query-local object read/cache entry.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub(crate) struct ReadIdentity {
    pub key: String,
    pub range: ReadRange,
    pub generation: Option<ObjectGeneration>,
}

impl ReadIdentity {
    pub(crate) fn whole(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            range: ReadRange::Whole,
            generation: None,
        }
    }

    pub(crate) fn exact(key: impl Into<String>, start: u64, length: u64) -> ZarrFdwResult<Self> {
        validate_nonempty_range(length)?;
        start.checked_add(length - 1).ok_or_else(|| {
            ZarrFdwError::InvalidMetadata("storage byte range end overflows u64".to_string())
        })?;
        Ok(Self {
            key: key.into(),
            range: ReadRange::Exact { start, length },
            generation: None,
        })
    }

    pub(crate) fn suffix(key: impl Into<String>, length: u64) -> ZarrFdwResult<Self> {
        validate_nonempty_range(length)?;
        Ok(Self {
            key: key.into(),
            range: ReadRange::Suffix { length },
            generation: None,
        })
    }

    /// Apply an observed generation to a follow-up exact read. The resulting
    /// request is sent with `If-Match` so an index and payload can never come
    /// from different shard generations.
    pub(crate) fn with_generation(mut self, generation: ObjectGeneration) -> Self {
        self.generation = Some(generation);
        self
    }
}

/// One exactly validated backend range response.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RangedObject {
    /// Resolved exact range plus the generation observed in the response.
    pub identity: ReadIdentity,
    pub total_len: u64,
    pub bytes: Vec<u8>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ContentRange {
    start: u64,
    end: u64,
    total: u64,
}

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
        let uri = s.parse::<Uri>().map_err(|_| {
            ZarrFdwError::InvalidStoreUrl("expected s3://bucket/prefix".to_string())
        })?;
        if uri.scheme_str() != Some("s3") || uri.host().is_none() {
            return Err(ZarrFdwError::InvalidStoreUrl(
                "expected s3://bucket/prefix".to_string(),
            ));
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

const S3_ONLY_OPTIONS: &[&str] = &[
    "anonymous",
    "aws_access_key_id",
    "aws_secret_access_key",
    "vault_access_key_id",
    "vault_secret_access_key",
    "aws_region",
    "endpoint_url",
    "path_style_url",
];

const HTTP_INSECURE_OPTION: &str = "allow_insecure_http";

/// Validate the configured storage scheme and its backend-specific options.
///
/// PostgreSQL privilege checks are deliberately separate: the DDL validator
/// checks the current user, while runtime construction checks the cataloged
/// foreign-server owner so delegated `USAGE` continues to work.
pub(crate) fn validate_store_options(
    options: &HashMap<String, String>,
) -> ZarrFdwResult<StorageBackendKind> {
    let store_url = require_option("store_url", options)?;
    if store_url.starts_with("s3://") {
        if options.contains_key(HTTP_INSECURE_OPTION) {
            return Err(ZarrFdwError::InvalidOptionValue {
                option: HTTP_INSECURE_OPTION.to_string(),
                message: "is only valid for http:// stores".to_string(),
            });
        }
        StoreUrl::parse(store_url)?;
        validate_auth_options(options)?;
        return Ok(StorageBackendKind::S3);
    }
    if store_url.starts_with("file:") {
        LocalBackend::validate_url(store_url)?;
        if let Some(option) = S3_ONLY_OPTIONS
            .iter()
            .find(|option| options.contains_key(**option))
        {
            return Err(ZarrFdwError::InvalidOptionValue {
                option: (*option).to_string(),
                message: "is only valid for s3:// stores".to_string(),
            });
        }
        if options.contains_key(HTTP_INSECURE_OPTION) {
            return Err(ZarrFdwError::InvalidOptionValue {
                option: HTTP_INSECURE_OPTION.to_string(),
                message: "is only valid for http:// stores".to_string(),
            });
        }
        return Ok(StorageBackendKind::Local);
    }
    let scheme = store_url
        .split_once(':')
        .map(|(scheme, _)| scheme)
        .unwrap_or_default();
    if scheme.eq_ignore_ascii_case("https") || scheme.eq_ignore_ascii_case("http") {
        if let Some(option) = S3_ONLY_OPTIONS
            .iter()
            .find(|option| options.contains_key(**option))
        {
            return Err(ZarrFdwError::InvalidOptionValue {
                option: (*option).to_string(),
                message: "is only valid for s3:// stores".to_string(),
            });
        }
        let allow_insecure_http = boolean_option(options, HTTP_INSECURE_OPTION)?;
        HttpBackend::validate_url(store_url, allow_insecure_http)?;
        return Ok(StorageBackendKind::Http);
    }
    Err(ZarrFdwError::InvalidStoreUrl(
        "storage URL scheme is unsupported".to_string(),
    ))
}

/// Enforce the `CREATE/ALTER SERVER` privilege boundary for trusted stores.
pub(crate) fn validate_store_definition_privilege(kind: StorageBackendKind) -> ZarrFdwResult<()> {
    if !unsafe { pg_sys::superuser() } {
        return match kind {
            StorageBackendKind::Local => Err(ZarrFdwError::FileStoreDefinitionRequiresSuperuser),
            StorageBackendKind::Http => Err(ZarrFdwError::HttpStoreDefinitionRequiresSuperuser),
            StorageBackendKind::S3 => Ok(()),
        };
    }
    Ok(())
}

fn validate_file_server_owner(server_oid: pg_sys::Oid) -> ZarrFdwResult<()> {
    if server_oid == pg_sys::Oid::INVALID {
        return Err(ZarrFdwError::FileStoreOwnerRequiresSuperuser);
    }
    let server = unsafe { pg_sys::GetForeignServer(server_oid) };
    if server.is_null() || !unsafe { pg_sys::superuser_arg((*server).owner) } {
        return Err(ZarrFdwError::FileStoreOwnerRequiresSuperuser);
    }
    Ok(())
}

fn validate_http_server_owner(server_oid: pg_sys::Oid) -> ZarrFdwResult<()> {
    if server_oid == pg_sys::Oid::INVALID {
        return Err(ZarrFdwError::HttpStoreOwnerRequiresSuperuser);
    }
    let server = unsafe { pg_sys::GetForeignServer(server_oid) };
    if server.is_null() || !unsafe { pg_sys::superuser_arg((*server).owner) } {
        return Err(ZarrFdwError::HttpStoreOwnerRequiresSuperuser);
    }
    Ok(())
}

struct S3Backend {
    client: s3::Client,
    url: StoreUrl,
}

impl StorageBackend for S3Backend {
    fn kind(&self) -> StorageBackendKind {
        StorageBackendKind::S3
    }

    fn get_object_owned(&self, key: String, max_bytes: usize) -> StoreFuture<Option<Vec<u8>>> {
        get_object_optional_owned(self.client.clone(), self.url.clone(), key, max_bytes)
            .boxed_local()
    }

    fn get_range_owned(&self, identity: ReadIdentity) -> StoreFuture<Option<RangedObject>> {
        get_object_range_owned(self.client.clone(), self.url.clone(), identity).boxed_local()
    }

    fn list_directory_page_owned(
        &self,
        path: String,
        continuation_token: Option<String>,
    ) -> StoreFuture<DirectoryPage> {
        list_directory_page_owned(
            self.client.clone(),
            self.url.clone(),
            path,
            continuation_token,
        )
        .boxed_local()
    }
}

/// Query-local storage coordinator.
pub(crate) struct ZarrStore {
    pub rt: Runtime,
    backend: Arc<dyn StorageBackend>,
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

    /// Build the configured store from `CREATE SERVER` options.
    pub fn new(server: &ForeignServer) -> ZarrFdwResult<Self> {
        // Cannot use create_async_runtime() as the runtime needs multiple threads
        let rt = tokio::runtime::Runtime::new()
            .map_err(CreateRuntimeError::FailedToCreateAsyncRuntime)?;

        let kind = validate_store_options(&server.options)?;
        let store_url = require_option("store_url", &server.options)?;
        let backend: Arc<dyn StorageBackend> = match kind {
            StorageBackendKind::S3 => {
                let url = StoreUrl::parse(store_url)?;
                let auth_mode = validate_auth_options(&server.options)?;
                let client = match auth_mode {
                    AuthMode::Anonymous => {
                        Self::build_client(&rt, &server.options, ClientAuth::Anonymous)
                    }
                    AuthMode::Direct => {
                        let access_key =
                            require_option("aws_access_key_id", &server.options)?.to_string();
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
                        let vault_access_key_id =
                            require_option("vault_access_key_id", &server.options)?;
                        let vault_secret_access_key =
                            require_option("vault_secret_access_key", &server.options)?;
                        let access_key =
                            get_vault_secret(vault_access_key_id).ok_or_else(|| {
                                ZarrFdwError::VaultSecretNotFound {
                                    option: "vault_access_key_id".to_string(),
                                }
                            })?;
                        let secret_key =
                            get_vault_secret(vault_secret_access_key).ok_or_else(|| {
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
                Arc::new(S3Backend { client, url })
            }
            StorageBackendKind::Local => {
                validate_file_server_owner(server.server_oid)?;
                Arc::new(LocalBackend::new(store_url)?)
            }
            StorageBackendKind::Http => {
                validate_http_server_owner(server.server_oid)?;
                let allow_insecure_http = boolean_option(&server.options, HTTP_INSECURE_OPTION)?;
                Arc::new(HttpBackend::new(store_url, allow_insecure_http)?)
            }
        };

        Ok(Self { rt, backend })
    }

    pub(crate) fn backend_kind(&self) -> StorageBackendKind {
        self.backend.kind()
    }

    pub(crate) fn backend_label(&self) -> &'static str {
        self.backend_kind().label()
    }

    pub(crate) fn effective_max_concurrent_reads(&self, configured: usize) -> usize {
        self.backend_kind()
            .effective_max_concurrent_reads(configured)
    }

    /// Fail before any backend request when hierarchy discovery is requested
    /// from a readable-but-non-listable HTTP store.
    pub(crate) fn require_listing(&self) -> ZarrFdwResult<()> {
        if self.backend_kind() == StorageBackendKind::Http {
            return Err(ZarrFdwError::UnsupportedExecutionFeature(
                http::HTTP_LISTING_UNSUPPORTED.to_string(),
            ));
        }
        Ok(())
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

    /// Fetch a full object by key (relative to the store root), returning
    /// `None` only when the backend explicitly reports that it is absent.
    pub async fn get_object_optional(
        &self,
        key: &str,
        max_bytes: usize,
    ) -> ZarrFdwResult<Option<Vec<u8>>> {
        self.get_object_optional_owned(key.to_string(), max_bytes)
            .await
    }

    /// Create an owned object fetch future suitable for the foreground
    /// prefetch window. The future owns cloned backend state and therefore
    /// never borrows the PostgreSQL scan object while it is queued.
    pub fn get_object_optional_owned(
        &self,
        key: String,
        max_bytes: usize,
    ) -> StoreFuture<Option<Vec<u8>>> {
        self.backend.get_object_owned(key, max_bytes)
    }

    /// Synchronous optional fetch used for sparse Zarr chunks.
    pub fn get_object_optional_sync(
        &self,
        key: &str,
        max_bytes: usize,
    ) -> ZarrFdwResult<Option<Vec<u8>>> {
        self.block_on_interruptibly(self.get_object_optional(key, max_bytes))
    }

    /// Create an owned, exactly bounded storage range request. This method
    /// never falls back to reading the complete object.
    pub(crate) fn get_object_range_owned(
        &self,
        identity: ReadIdentity,
    ) -> StoreFuture<Option<RangedObject>> {
        self.backend.get_range_owned(identity)
    }

    /// Synchronous range fetch for eager coordinate and shard-index reads.
    pub(crate) fn get_object_range_sync(
        &self,
        identity: ReadIdentity,
    ) -> ZarrFdwResult<Option<RangedObject>> {
        self.block_on_interruptibly(self.get_object_range_owned(identity))
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
        self.backend
            .list_directory_page_owned(path.to_string(), continuation_token)
            .await
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

async fn list_directory_page_owned(
    client: s3::Client,
    url: StoreUrl,
    path: String,
    continuation_token: Option<String>,
) -> ZarrFdwResult<DirectoryPage> {
    let key = url.object_key(&path);
    let prefix = if key.is_empty() {
        String::new()
    } else {
        format!("{}/", key.trim_end_matches('/'))
    };
    let response = client
        .list_objects_v2()
        .bucket(&url.bucket)
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
        .filter_map(|entry| url.relative_key(&entry))
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

async fn get_object_range_owned(
    client: s3::Client,
    url: StoreUrl,
    identity: ReadIdentity,
) -> ZarrFdwResult<Option<RangedObject>> {
    if identity
        .generation
        .as_ref()
        .is_some_and(|generation| generation.backend_kind() != StorageBackendKind::S3)
    {
        return Err(ZarrFdwError::InvalidMetadata(
            "storage object generation belongs to a different backend".to_string(),
        ));
    }
    let full_key = url.object_key(&identity.key);
    let range_header = range_header(&identity.range)?;
    let expected_length = range_length(&identity.range)?;
    let expected_length_usize = usize::try_from(expected_length).map_err(|_| {
        ZarrFdwError::InvalidMetadata(format!(
            "range read for object '{full_key}' exceeds this platform's index capacity"
        ))
    })?;

    let mut request = client
        .get_object()
        .bucket(&url.bucket)
        .key(&full_key)
        .range(range_header);
    if let Some(generation) = &identity.generation {
        let etag = generation.s3_etag().ok_or_else(|| {
            ZarrFdwError::InvalidMetadata(
                "storage object generation belongs to a different backend".to_string(),
            )
        })?;
        request = request.if_match(etag.to_string());
    }
    let resp = match request.send().await {
        Ok(response) => response,
        Err(error) => {
            let modeled_no_such_key = error
                .as_service_error()
                .is_some_and(|error| error.is_no_such_key());
            let status = error
                .raw_response()
                .map(|response| response.status().as_u16());
            if is_missing_object_response(modeled_no_such_key, status) {
                return if identity.generation.is_some() {
                    Err(ZarrFdwError::InvalidMetadata(format!(
                        "object '{full_key}' changed while reading a shard (generation-conditioned S3 range is now missing)"
                    )))
                } else {
                    Ok(None)
                };
            }
            if status == Some(412) {
                return Err(ZarrFdwError::InvalidMetadata(format!(
                    "object '{full_key}' changed while reading a shard (S3 If-Match precondition failed)"
                )));
            }
            return Err(error.into());
        }
    };

    let content_range =
        required_content_range(resp.content_range.as_deref()).map_err(|message| {
            ZarrFdwError::InvalidMetadata(format!(
                "invalid Content-Range for object '{full_key}': {message}"
            ))
        })?;
    validate_content_range(&identity.range, content_range).map_err(|message| {
        ZarrFdwError::InvalidMetadata(format!(
            "invalid Content-Range for object '{full_key}': {message}"
        ))
    })?;

    let content_length = resp.content_length.ok_or_else(|| {
        ZarrFdwError::InvalidMetadata(format!(
            "range response for object '{full_key}' omitted Content-Length"
        ))
    })?;
    let content_length = u64::try_from(content_length).map_err(|_| {
        ZarrFdwError::InvalidMetadata(format!(
            "range response for object '{full_key}' has a negative Content-Length"
        ))
    })?;
    if content_length != expected_length {
        return Err(ZarrFdwError::InvalidMetadata(format!(
            "range response for object '{full_key}' has Content-Length {content_length}, expected exactly {expected_length} bytes"
        )));
    }

    let etag = resp
        .e_tag
        .clone()
        .filter(|etag| !etag.is_empty())
        .ok_or_else(|| {
            ZarrFdwError::InvalidMetadata(format!(
                "range response for object '{full_key}' omitted ETag required for shard consistency"
            ))
        })?;
    if let Some(expected) = &identity.generation
        && expected.s3_etag() != Some(etag.as_str())
    {
        return Err(ZarrFdwError::InvalidMetadata(format!(
            "object '{full_key}' changed while reading a shard (response ETag did not match If-Match)"
        )));
    }
    if let Some(expected) = &identity.generation
        && expected.total_len() != content_range.total
    {
        return Err(ZarrFdwError::InvalidMetadata(format!(
            "object '{full_key}' changed while reading a shard (Content-Range total {} did not match indexed object length {})",
            content_range.total,
            expected.total_len()
        )));
    }
    let generation = ObjectGeneration::S3 {
        etag,
        version_id: resp.version_id.clone(),
        total_len: content_range.total,
    };
    let bytes = read_bounded_object(
        resp.body.into_async_read(),
        expected_length_usize,
        &full_key,
    )
    .await?;
    if bytes.len() != expected_length_usize {
        return Err(ZarrFdwError::InvalidMetadata(format!(
            "range response for object '{full_key}' returned {} body bytes, expected exactly {expected_length} bytes",
            bytes.len()
        )));
    }

    Ok(Some(RangedObject {
        identity: ReadIdentity {
            key: identity.key,
            range: ReadRange::Exact {
                start: content_range.start,
                length: expected_length,
            },
            generation: Some(generation),
        },
        total_len: content_range.total,
        bytes,
    }))
}

fn validate_nonempty_range(length: u64) -> ZarrFdwResult<()> {
    if length == 0 {
        return Err(ZarrFdwError::InvalidMetadata(
            "storage byte range length must be greater than zero".to_string(),
        ));
    }
    Ok(())
}

fn range_length(range: &ReadRange) -> ZarrFdwResult<u64> {
    match range {
        ReadRange::Whole => Err(ZarrFdwError::InvalidMetadata(
            "whole-object identity cannot be used for a storage range read".to_string(),
        )),
        ReadRange::Exact { length, .. } | ReadRange::Suffix { length } => {
            validate_nonempty_range(*length)?;
            Ok(*length)
        }
    }
}

fn range_header(range: &ReadRange) -> ZarrFdwResult<String> {
    match range {
        ReadRange::Whole => Err(ZarrFdwError::InvalidMetadata(
            "whole-object identity cannot be used for a storage range read".to_string(),
        )),
        ReadRange::Exact { start, length } => {
            validate_nonempty_range(*length)?;
            let end = start.checked_add(length - 1).ok_or_else(|| {
                ZarrFdwError::InvalidMetadata("storage byte range end overflows u64".to_string())
            })?;
            Ok(format!("bytes={start}-{end}"))
        }
        ReadRange::Suffix { length } => {
            validate_nonempty_range(*length)?;
            Ok(format!("bytes=-{length}"))
        }
    }
}

fn parse_content_range(value: &str) -> Result<ContentRange, String> {
    let bytes = value
        .strip_prefix("bytes ")
        .ok_or_else(|| "expected canonical 'bytes START-END/TOTAL'".to_string())?;
    let (range, total) = bytes
        .split_once('/')
        .ok_or_else(|| "expected canonical 'bytes START-END/TOTAL'".to_string())?;
    if total == "*" {
        return Err("wildcard total length is not accepted".to_string());
    }
    let (start, end) = range
        .split_once('-')
        .ok_or_else(|| "expected canonical 'bytes START-END/TOTAL'".to_string())?;
    let start = parse_canonical_u64(start, "start")?;
    let end = parse_canonical_u64(end, "end")?;
    let total = parse_canonical_u64(total, "total")?;
    if start > end {
        return Err("range start exceeds range end".to_string());
    }
    if end >= total {
        return Err("range end must be smaller than total object length".to_string());
    }
    Ok(ContentRange { start, end, total })
}

fn required_content_range(value: Option<&str>) -> Result<ContentRange, String> {
    let value = value.ok_or_else(|| {
        "header is absent; refusing a full-object fallback for a range request".to_string()
    })?;
    parse_content_range(value)
}

fn parse_canonical_u64(value: &str, label: &str) -> Result<u64, String> {
    if value.is_empty()
        || (value.len() > 1 && value.starts_with('0'))
        || !value.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(format!("{label} is not a canonical unsigned integer"));
    }
    value
        .parse::<u64>()
        .map_err(|_| format!("{label} exceeds u64"))
}

fn validate_content_range(range: &ReadRange, actual: ContentRange) -> Result<(), String> {
    let actual_length = actual
        .end
        .checked_sub(actual.start)
        .and_then(|length| length.checked_add(1))
        .ok_or_else(|| "returned byte length overflows u64".to_string())?;
    match range {
        ReadRange::Whole => Err("whole-object request cannot have a range response".to_string()),
        ReadRange::Exact { start, length } => {
            let expected_end = start
                .checked_add(length.checked_sub(1).ok_or_else(|| {
                    "requested byte range length must be greater than zero".to_string()
                })?)
                .ok_or_else(|| "requested byte range end overflows u64".to_string())?;
            if actual.start != *start || actual.end != expected_end || actual_length != *length {
                return Err(format!(
                    "returned bytes {}-{}/{}, expected exactly {start}-{expected_end}",
                    actual.start, actual.end, actual.total
                ));
            }
            Ok(())
        }
        ReadRange::Suffix { length } => {
            if actual_length != *length || actual.end.checked_add(1) != Some(actual.total) {
                return Err(format!(
                    "returned suffix bytes {}-{}/{}, expected exactly the final {length} bytes",
                    actual.start, actual.end, actual.total
                ));
            }
            Ok(())
        }
    }
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
    fn range_headers_use_checked_inclusive_http_bounds() {
        assert_eq!(ReadIdentity::whole("/array/c/0/").key, "/array/c/0/");
        assert_eq!(
            range_header(&ReadRange::Exact {
                start: 10,
                length: 4
            })
            .unwrap(),
            "bytes=10-13"
        );
        assert_eq!(
            range_header(&ReadRange::Suffix { length: 68 }).unwrap(),
            "bytes=-68"
        );
        assert!(
            range_header(&ReadRange::Exact {
                start: 0,
                length: 0
            })
            .is_err()
        );
        assert!(
            range_header(&ReadRange::Exact {
                start: u64::MAX,
                length: 2
            })
            .is_err()
        );
    }

    #[test]
    fn content_range_parser_is_canonical_and_checked() {
        assert_eq!(
            parse_content_range("bytes 10-13/100").unwrap(),
            ContentRange {
                start: 10,
                end: 13,
                total: 100
            }
        );
        for invalid in [
            "bytes 10-13/*",
            "bytes 010-13/100",
            "Bytes 10-13/100",
            "bytes 13-10/100",
            "bytes 10-100/100",
            "bytes 10-13/",
        ] {
            assert!(parse_content_range(invalid).is_err(), "{invalid}");
        }
        assert!(required_content_range(None).is_err());
        assert!(required_content_range(Some("not-a-content-range")).is_err());
    }

    #[test]
    fn content_range_must_match_exact_or_full_suffix_request() {
        let exact = ReadRange::Exact {
            start: 10,
            length: 4,
        };
        assert!(
            validate_content_range(
                &exact,
                ContentRange {
                    start: 10,
                    end: 13,
                    total: 100
                }
            )
            .is_ok()
        );
        assert!(
            validate_content_range(
                &exact,
                ContentRange {
                    start: 10,
                    end: 12,
                    total: 100
                }
            )
            .is_err()
        );

        let suffix = ReadRange::Suffix { length: 4 };
        assert!(
            validate_content_range(
                &suffix,
                ContentRange {
                    start: 96,
                    end: 99,
                    total: 100
                }
            )
            .is_ok()
        );
        assert!(
            validate_content_range(
                &suffix,
                ContentRange {
                    start: 0,
                    end: 2,
                    total: 3
                }
            )
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
    fn validates_backend_specific_store_options() {
        assert_eq!(
            validate_store_options(&options(&[("store_url", "s3://bucket/root")])).unwrap(),
            StorageBackendKind::S3
        );
        assert_eq!(
            validate_store_options(&options(&[("store_url", "file:///tmp/zarr")])).unwrap(),
            StorageBackendKind::Local
        );
        assert_eq!(
            validate_store_options(&options(&[(
                "store_url",
                "https://objects.example.test/root.zarr"
            )]))
            .unwrap(),
            StorageBackendKind::Http
        );
        assert_eq!(
            validate_store_options(&options(&[
                ("store_url", "http://objects.example.test/root.zarr"),
                ("allow_insecure_http", "true"),
            ]))
            .unwrap(),
            StorageBackendKind::Http
        );
        let error = validate_store_options(&options(&[
            ("store_url", "file:///tmp/zarr"),
            ("anonymous", "true"),
        ]))
        .unwrap_err();
        assert_eq!(
            error.to_string(),
            "invalid value for option 'anonymous': is only valid for s3:// stores"
        );
        assert!(
            validate_store_options(&options(&[(
                "store_url",
                "http://objects.example.test/root.zarr"
            )]))
            .is_err()
        );
        assert!(
            validate_store_options(&options(&[
                ("store_url", "https://objects.example.test/root.zarr"),
                ("anonymous", "true"),
            ]))
            .is_err()
        );
        let error = validate_store_options(&options(&[(
            "store_url",
            "HTTPS://user:secret@objects.example.test/root.zarr",
        )]))
        .unwrap_err()
        .to_string();
        assert!(!error.contains("user"));
        assert!(!error.contains("secret"));
    }

    #[test]
    fn backend_read_concurrency_is_truthful() {
        assert_eq!(StorageBackendKind::S3.effective_max_concurrent_reads(8), 8);
        assert_eq!(
            StorageBackendKind::Http.effective_max_concurrent_reads(8),
            8
        );
        assert_eq!(
            StorageBackendKind::Local.effective_max_concurrent_reads(8),
            1
        );
    }

    #[test]
    fn generation_backend_tags_are_disjoint() {
        let s3 = ObjectGeneration::S3 {
            etag: "\"etag\"".to_string(),
            version_id: None,
            total_len: 1,
        };
        let local = ObjectGeneration::Local {
            fingerprint: "1:2:1:3:4:5:6".to_string(),
            total_len: 1,
        };
        let http = ObjectGeneration::Http {
            etag: "\"etag\"".to_string(),
            total_len: 1,
        };
        assert_eq!(s3.backend_kind(), StorageBackendKind::S3);
        assert_eq!(local.backend_kind(), StorageBackendKind::Local);
        assert_eq!(http.backend_kind(), StorageBackendKind::Http);
        assert_ne!(s3, local);
        assert_ne!(s3, http);
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
