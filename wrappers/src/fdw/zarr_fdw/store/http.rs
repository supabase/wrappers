//! Trusted, anonymous, read-only HTTP(S) object storage.

use futures_util::FutureExt;
use reqwest::header::{
    ACCEPT_ENCODING, CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_RANGE, ETAG, IF_MATCH, RANGE,
};
use reqwest::{Client, Response, StatusCode, Url, redirect};
use std::time::Duration;

use super::{
    ContentRange, DirectoryPage, ObjectGeneration, RangedObject, ReadIdentity, ReadRange,
    StorageBackend, StorageBackendKind, StoreFuture, parse_canonical_u64, range_header,
    range_length, required_content_range, validate_content_range,
};
use crate::fdw::zarr_fdw::{ZarrFdwError, ZarrFdwResult};

const MAX_CONFIGURED_URL_BYTES: usize = 8 * 1024;
const MAX_STORAGE_KEY_BYTES: usize = 8 * 1024;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

pub(crate) const HTTP_LISTING_UNSUPPORTED: &str = "HTTP(S) Zarr stores do not support hierarchy listing; configure an explicit array path or OME multiscale selection";

#[derive(Clone)]
pub(super) struct HttpBackend {
    client: Client,
    root: HttpStoreUrl,
}

#[derive(Clone, Debug)]
struct HttpStoreUrl {
    root: Url,
}

impl HttpBackend {
    pub(super) fn validate_url(raw: &str, allow_insecure_http: bool) -> ZarrFdwResult<()> {
        HttpStoreUrl::parse(raw, allow_insecure_http).map(|_| ())
    }

    pub(super) fn new(raw: &str, allow_insecure_http: bool) -> ZarrFdwResult<Self> {
        let root = HttpStoreUrl::parse(raw, allow_insecure_http)?;
        let client = Client::builder()
            .use_rustls_tls()
            .min_tls_version(reqwest::tls::Version::TLS_1_2)
            .https_only(!allow_insecure_http)
            .redirect(redirect::Policy::none())
            .referer(false)
            .no_proxy()
            .no_gzip()
            .no_brotli()
            .no_deflate()
            .no_zstd()
            .retry(reqwest::retry::never())
            .connect_timeout(CONNECT_TIMEOUT)
            .user_agent(concat!("wrappers-zarr/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(|_| http_access_error("<store>", "client initialization failed"))?;
        Ok(Self { client, root })
    }

    async fn read_whole(self, key: String, max_bytes: usize) -> ZarrFdwResult<Option<Vec<u8>>> {
        let object_url = self.root.object_url(&key)?;
        let response = self
            .client
            .get(object_url)
            .header(ACCEPT_ENCODING, "identity")
            .send()
            .await
            .map_err(|_| http_access_error(&key, "transport error"))?;

        match response.status() {
            StatusCode::NOT_FOUND => return Ok(None),
            StatusCode::OK => {}
            status if status.is_redirection() => return Err(redirect_error(&key)),
            status => {
                return Err(protocol_error(
                    &key,
                    format!(
                        "whole-object response returned status {}, expected 200 or 404",
                        status.as_u16()
                    ),
                ));
            }
        }

        validate_identity_encoding(&response, &key)?;
        let declared_length = optional_content_length(&response, &key, "whole-object")?;
        if declared_length.is_some_and(|length| length > max_bytes as u64) {
            return Err(protocol_error(
                &key,
                format!(
                    "object length {} exceeds the read limit of {max_bytes} bytes",
                    declared_length.expect("checked Some above")
                ),
            ));
        }
        let bytes = read_bounded_response(response, max_bytes, &key).await?;
        if let Some(declared_length) = declared_length
            && u64::try_from(bytes.len()).ok() != Some(declared_length)
        {
            return Err(protocol_error(
                &key,
                format!(
                    "whole-object body returned {} bytes, Content-Length declared {declared_length}",
                    bytes.len()
                ),
            ));
        }
        Ok(Some(bytes))
    }

    async fn read_range(self, identity: ReadIdentity) -> ZarrFdwResult<Option<RangedObject>> {
        if identity
            .generation
            .as_ref()
            .is_some_and(|generation| generation.backend_kind() != StorageBackendKind::Http)
        {
            return Err(ZarrFdwError::InvalidMetadata(
                "storage object generation belongs to a different backend".to_string(),
            ));
        }

        let object_url = self.root.object_url(&identity.key)?;
        let requested_range = range_header(&identity.range)?;
        let expected_length = range_length(&identity.range)?;
        let expected_length_usize = usize::try_from(expected_length).map_err(|_| {
            protocol_error(
                &identity.key,
                "range length exceeds this platform's index capacity",
            )
        })?;
        let mut request = self
            .client
            .get(object_url)
            .header(ACCEPT_ENCODING, "identity")
            .header(RANGE, requested_range);
        if let Some(generation) = &identity.generation {
            let etag = generation.http_etag().ok_or_else(|| {
                ZarrFdwError::InvalidMetadata(
                    "storage object generation belongs to a different backend".to_string(),
                )
            })?;
            request = request.header(IF_MATCH, etag);
        }
        let response = request
            .send()
            .await
            .map_err(|_| http_access_error(&identity.key, "transport error"))?;

        match response.status() {
            StatusCode::PARTIAL_CONTENT => {}
            StatusCode::NOT_FOUND if identity.generation.is_none() => return Ok(None),
            StatusCode::NOT_FOUND | StatusCode::PRECONDITION_FAILED
                if identity.generation.is_some() =>
            {
                return Err(changed_while_reading(
                    &identity.key,
                    "generation-conditioned object is missing or If-Match failed",
                ));
            }
            StatusCode::RANGE_NOT_SATISFIABLE if identity.generation.is_some() => {
                return Err(changed_while_reading(
                    &identity.key,
                    "generation-conditioned range is no longer satisfiable",
                ));
            }
            StatusCode::OK => {
                return Err(protocol_error(
                    &identity.key,
                    "range response returned status 200; server ignored Range",
                ));
            }
            status if status.is_redirection() => return Err(redirect_error(&identity.key)),
            status => {
                return Err(protocol_error(
                    &identity.key,
                    format!(
                        "range response returned status {}, expected 206 or 404",
                        status.as_u16()
                    ),
                ));
            }
        }

        validate_identity_encoding(&response, &identity.key)?;
        let content_range = response_content_range(&response, &identity.key)?;
        validate_content_range(&identity.range, content_range).map_err(|message| {
            protocol_error(&identity.key, format!("invalid Content-Range: {message}"))
        })?;
        if let Some(content_length) = optional_content_length(&response, &identity.key, "range")?
            && content_length != expected_length
        {
            return Err(protocol_error(
                &identity.key,
                format!(
                    "range Content-Length {content_length} does not equal the requested {expected_length} bytes"
                ),
            ));
        }
        let etag = required_strong_etag(&response, &identity.key)?;
        if let Some(expected) = &identity.generation {
            if expected.http_etag() != Some(etag.as_str()) {
                return Err(changed_while_reading(
                    &identity.key,
                    "response ETag did not match If-Match",
                ));
            }
            if expected.total_len() != content_range.total {
                return Err(changed_while_reading(
                    &identity.key,
                    "Content-Range total changed after the shard index read",
                ));
            }
        }

        let bytes = read_bounded_response(response, expected_length_usize, &identity.key).await?;
        if bytes.len() != expected_length_usize {
            return Err(protocol_error(
                &identity.key,
                format!(
                    "range body returned {} bytes, expected exactly {expected_length} bytes",
                    bytes.len()
                ),
            ));
        }
        let generation = ObjectGeneration::Http {
            etag,
            total_len: content_range.total,
        };
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
}

impl StorageBackend for HttpBackend {
    fn kind(&self) -> StorageBackendKind {
        StorageBackendKind::Http
    }

    fn get_object_owned(&self, key: String, max_bytes: usize) -> StoreFuture<Option<Vec<u8>>> {
        let backend = self.clone();
        async move { backend.read_whole(key, max_bytes).await }.boxed_local()
    }

    fn get_range_owned(&self, identity: ReadIdentity) -> StoreFuture<Option<RangedObject>> {
        let backend = self.clone();
        async move { backend.read_range(identity).await }.boxed_local()
    }

    fn list_directory_page_owned(
        &self,
        _path: String,
        _continuation_token: Option<String>,
    ) -> StoreFuture<DirectoryPage> {
        async move {
            Err(ZarrFdwError::UnsupportedExecutionFeature(
                HTTP_LISTING_UNSUPPORTED.to_string(),
            ))
        }
        .boxed_local()
    }
}

impl HttpStoreUrl {
    fn parse(raw: &str, allow_insecure_http: bool) -> ZarrFdwResult<Self> {
        if raw.len() > MAX_CONFIGURED_URL_BYTES
            || raw.chars().any(char::is_control)
            || raw.contains('\\')
        {
            return Err(invalid_url(
                "URL is too long or contains backslash or control characters",
            ));
        }
        reject_ambiguous_percent_encoding(raw)
            .map_err(|message| invalid_url(format!("configured path {message}")))?;
        let mut root = Url::parse(raw).map_err(|_| invalid_url("URL is not valid"))?;
        match root.scheme() {
            "https" if allow_insecure_http => {
                return Err(ZarrFdwError::InvalidOptionValue {
                    option: "allow_insecure_http".to_string(),
                    message: "may be 'true' only for http:// stores".to_string(),
                });
            }
            "https" => {}
            "http" if allow_insecure_http => {}
            "http" => {
                return Err(ZarrFdwError::InvalidOptionValue {
                    option: "allow_insecure_http".to_string(),
                    message: "must be 'true' for http:// stores".to_string(),
                });
            }
            _ => {
                return Err(invalid_url(
                    "scheme must be https:// or explicitly enabled http://",
                ));
            }
        }
        if root.cannot_be_a_base()
            || root.host_str().is_none()
            || !root.username().is_empty()
            || root.password().is_some()
            || root.query().is_some()
            || root.fragment().is_some()
        {
            return Err(invalid_url(
                "host is required; credentials, query, and fragment are not allowed",
            ));
        }
        let raw_path = configured_raw_path(raw);
        if root.port().is_none() && root.port_or_known_default().is_none()
            || raw_path
                .split('/')
                .any(|component| matches!(component, "." | ".."))
            || root.path()[1..].contains("//")
        {
            return Err(invalid_url("port or path is invalid"));
        }
        root.path_segments_mut()
            .map_err(|_| invalid_url("URL cannot be used as an object root"))?
            .pop_if_empty()
            .push("");
        Ok(Self { root })
    }

    fn object_url(&self, key: &str) -> ZarrFdwResult<Url> {
        validate_http_key(key)?;
        let mut url = self.root.clone();
        {
            let mut segments = url
                .path_segments_mut()
                .map_err(|_| invalid_url("URL cannot be used as an object root"))?;
            segments.pop_if_empty();
            for component in key.split('/') {
                segments.push(component);
            }
        }
        Ok(url)
    }
}

fn configured_raw_path(raw: &str) -> &str {
    let Some((_, after_scheme)) = raw.split_once("://") else {
        return "";
    };
    let Some(path_start) = after_scheme.find('/') else {
        return "";
    };
    let path = &after_scheme[path_start..];
    let end = path.find(['?', '#']).unwrap_or(path.len());
    &path[..end]
}

fn validate_http_key(key: &str) -> ZarrFdwResult<()> {
    if key.is_empty() || key.len() > MAX_STORAGE_KEY_BYTES {
        return Err(invalid_key("must be nonempty and at most 8192 bytes"));
    }
    if key.starts_with('/') || key.ends_with('/') {
        return Err(invalid_key("must be a relative object path"));
    }
    if key
        .chars()
        .any(|character| matches!(character, '\\' | '?' | '#') || character.is_control())
    {
        return Err(invalid_key(
            "backslash, query, fragment, and control characters are not allowed",
        ));
    }
    if key
        .split('/')
        .any(|component| component.is_empty() || matches!(component, "." | ".."))
    {
        return Err(invalid_key(
            "empty, '.' and '..' components are not allowed",
        ));
    }
    reject_ambiguous_percent_encoding(key).map_err(invalid_key)
}

fn reject_ambiguous_percent_encoding(value: &str) -> Result<(), &'static str> {
    let lower = value.to_ascii_lowercase();
    if lower.contains("%2f") || lower.contains("%5c") || lower.contains("%2e") {
        return Err("must not contain percent-encoded slash, backslash, or dot");
    }
    Ok(())
}

fn response_content_range(response: &Response, key: &str) -> ZarrFdwResult<ContentRange> {
    let value = single_header(response, CONTENT_RANGE.as_str(), key, "Content-Range")?;
    required_content_range(value.as_deref())
        .map_err(|message| protocol_error(key, format!("invalid Content-Range: {message}")))
}

fn optional_content_length(
    response: &Response,
    key: &str,
    phase: &str,
) -> ZarrFdwResult<Option<u64>> {
    let Some(value) = single_header(response, CONTENT_LENGTH.as_str(), key, "Content-Length")?
    else {
        return Ok(None);
    };
    parse_canonical_u64(&value, "Content-Length")
        .map(Some)
        .map_err(|message| {
            protocol_error(key, format!("invalid {phase} Content-Length: {message}"))
        })
}

fn validate_identity_encoding(response: &Response, key: &str) -> ZarrFdwResult<()> {
    let Some(value) = single_header(response, CONTENT_ENCODING.as_str(), key, "Content-Encoding")?
    else {
        return Ok(());
    };
    if !value.eq_ignore_ascii_case("identity") {
        return Err(protocol_error(
            key,
            "response Content-Encoding is not identity",
        ));
    }
    Ok(())
}

fn required_strong_etag(response: &Response, key: &str) -> ZarrFdwResult<String> {
    let value = single_header(response, ETAG.as_str(), key, "ETag")?.ok_or_else(|| {
        protocol_error(
            key,
            "range response omitted strong ETag required for shard consistency",
        )
    })?;
    validate_strong_etag(&value)
        .map_err(|message| protocol_error(key, format!("invalid range ETag: {message}")))?;
    Ok(value)
}

fn validate_strong_etag(value: &str) -> Result<(), &'static str> {
    if value.starts_with("W/") {
        return Err("weak ETag is not accepted");
    }
    let bytes = value.as_bytes();
    if bytes.len() < 2 || bytes.first() != Some(&b'"') || bytes.last() != Some(&b'"') {
        return Err("expected a quoted strong ETag");
    }
    if bytes[1..bytes.len() - 1]
        .iter()
        .any(|byte| *byte == b'"' || *byte < 0x21 || *byte == 0x7f)
    {
        return Err("strong ETag contains invalid opaque-tag bytes");
    }
    Ok(())
}

fn single_header(
    response: &Response,
    name: &str,
    key: &str,
    display_name: &str,
) -> ZarrFdwResult<Option<String>> {
    let mut values = response.headers().get_all(name).iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(protocol_error(
            key,
            format!("response has multiple {display_name} headers"),
        ));
    }
    let value = value
        .to_str()
        .map_err(|_| protocol_error(key, format!("response has invalid {display_name} header")))?;
    Ok(Some(value.to_string()))
}

async fn read_bounded_response(
    mut response: Response,
    max_bytes: usize,
    key: &str,
) -> ZarrFdwResult<Vec<u8>> {
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| http_access_error(key, "response body read failed"))?
    {
        let next = bytes
            .len()
            .checked_add(chunk.len())
            .ok_or_else(|| protocol_error(key, "response body length exceeds platform capacity"))?;
        if next > max_bytes {
            return Err(protocol_error(
                key,
                format!("response body exceeds the read limit of {max_bytes} bytes"),
            ));
        }
        bytes
            .try_reserve_exact(chunk.len())
            .map_err(|_| protocol_error(key, "could not grow the bounded HTTP response buffer"))?;
        bytes.extend_from_slice(&chunk);
        tokio::task::yield_now().await;
    }
    Ok(bytes)
}

fn invalid_url(message: impl Into<String>) -> ZarrFdwError {
    ZarrFdwError::InvalidHttpStoreUrl(message.into())
}

fn invalid_key(message: impl Into<String>) -> ZarrFdwError {
    ZarrFdwError::InvalidHttpStorageKey(message.into())
}

fn protocol_error(key: &str, message: impl Into<String>) -> ZarrFdwError {
    ZarrFdwError::InvalidMetadata(format!(
        "HTTP storage protocol error for object '{key}': {}",
        message.into()
    ))
}

fn redirect_error(key: &str) -> ZarrFdwError {
    protocol_error(key, "redirect response was rejected")
}

fn changed_while_reading(key: &str, category: &'static str) -> ZarrFdwError {
    ZarrFdwError::InvalidMetadata(format!(
        "HTTP storage object '{key}' changed while reading a shard: {category}"
    ))
}

fn http_access_error(key: impl Into<String>, category: &'static str) -> ZarrFdwError {
    ZarrFdwError::HttpStorageAccess {
        key: key.into(),
        category,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_https_and_explicit_insecure_http() {
        assert!(HttpStoreUrl::parse("https://example.test/root.zarr", false).is_ok());
        assert!(HttpStoreUrl::parse("http://example.test/root.zarr", false).is_err());
        assert!(HttpStoreUrl::parse("http://example.test/root.zarr", true).is_ok());
        assert!(HttpStoreUrl::parse("https://example.test/root.zarr", true).is_err());
    }

    #[test]
    fn rejects_credentials_query_fragment_and_ambiguous_paths() {
        for invalid in [
            "https://user@example.test/root.zarr",
            "https://example.test/root.zarr?token=secret",
            "https://example.test/root.zarr#fragment",
            "https://example.test/root%2fzarr",
            "https://example.test/root//zarr",
        ] {
            assert!(HttpStoreUrl::parse(invalid, false).is_err(), "{invalid}");
        }
    }

    #[test]
    fn object_keys_append_components_without_replacing_origin() {
        let root = HttpStoreUrl::parse("https://example.test/base/root.zarr", false).unwrap();
        let url = root.object_url("nested values/c/0").unwrap();
        assert_eq!(
            url.as_str(),
            "https://example.test/base/root.zarr/nested%20values/c/0"
        );
        assert_eq!(url.host_str(), Some("example.test"));
        for invalid in [
            "",
            "/absolute",
            "trailing/",
            "../escape",
            "a//b",
            "a\\b",
            "a?query",
            "%2e%2e/escape",
        ] {
            assert!(root.object_url(invalid).is_err(), "{invalid}");
        }
    }

    #[test]
    fn strong_etags_are_quoted_and_not_weak() {
        assert!(validate_strong_etag("\"generation-1\"").is_ok());
        for invalid in [
            "generation-1",
            "W/\"generation-1\"",
            "\"bad value\"",
            "\"a\"b\"",
        ] {
            assert!(validate_strong_etag(invalid).is_err(), "{invalid}");
        }
    }

    #[test]
    fn backend_generation_is_typed() {
        let generation = ObjectGeneration::Http {
            etag: "\"generation-1\"".to_string(),
            total_len: 10,
        };
        assert_eq!(generation.backend_kind(), StorageBackendKind::Http);
        assert_eq!(generation.http_etag(), Some("\"generation-1\""));
        assert_eq!(generation.s3_etag(), None);
        assert_eq!(generation.total_len(), 10);
    }

    #[test]
    fn listing_error_is_stable() {
        assert_eq!(
            HTTP_LISTING_UNSUPPORTED,
            "HTTP(S) Zarr stores do not support hierarchy listing; configure an explicit array path or OME multiscale selection"
        );
    }
}
