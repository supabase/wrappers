//! Capability-confined read-only local filesystem storage.

use cap_std::ambient_authority;
use cap_std::fs::{Dir, Metadata, MetadataExt, OpenOptions, OpenOptionsExt};
use futures_util::FutureExt;
use std::collections::BinaryHeap;
use std::io::{ErrorKind, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use url::Url;

use super::{
    DirectoryPage, ObjectGeneration, RangedObject, ReadIdentity, ReadRange, StorageBackend,
    StorageBackendKind, StoreFuture, join_key,
};
use crate::fdw::zarr_fdw::{ZarrFdwError, ZarrFdwResult};

const READ_BLOCK_BYTES: usize = 64 * 1024;
const LIST_PAGE_ENTRIES: usize = 1_000;
const LIST_YIELD_ENTRIES: usize = 64;

#[derive(Clone)]
pub(super) struct LocalBackend {
    root_path: PathBuf,
    root: Arc<Mutex<Option<Arc<Dir>>>>,
}

impl LocalBackend {
    pub(super) fn validate_url(raw: &str) -> ZarrFdwResult<PathBuf> {
        parse_file_url(raw)
    }

    pub(super) fn new(raw: &str) -> ZarrFdwResult<Self> {
        Ok(Self {
            root_path: parse_file_url(raw)?,
            root: Arc::new(Mutex::new(None)),
        })
    }

    fn root(&self) -> ZarrFdwResult<Arc<Dir>> {
        let mut root = self
            .root
            .lock()
            .map_err(|_| local_access_error("<root>", "configured root lock was poisoned"))?;
        if let Some(root) = root.as_ref() {
            return Ok(Arc::clone(root));
        }

        let opened =
            Dir::open_ambient_dir(&self.root_path, ambient_authority()).map_err(|error| {
                local_access_error("<root>", format!("could not open configured root: {error}"))
            })?;
        let metadata = opened.dir_metadata().map_err(|error| {
            local_access_error(
                "<root>",
                format!("could not inspect configured root: {error}"),
            )
        })?;
        if !metadata.is_dir() {
            return Err(local_access_error(
                "<root>",
                "configured root is not a directory",
            ));
        }
        let opened = Arc::new(opened);
        *root = Some(Arc::clone(&opened));
        Ok(opened)
    }

    async fn read_whole(self, key: String, max_bytes: usize) -> ZarrFdwResult<Option<Vec<u8>>> {
        validate_storage_key(&key, false)?;
        let root = self.root()?;
        let Some((mut file, before)) = open_regular_file(&root, &key)? else {
            return Ok(None);
        };
        let generation = local_generation(&before);
        let length = usize::try_from(generation.total_len()).map_err(|_| {
            ZarrFdwError::InvalidMetadata(format!(
                "local storage object '{key}' length exceeds this platform's index capacity"
            ))
        })?;
        if length > max_bytes {
            return Err(ZarrFdwError::InvalidMetadata(format!(
                "local storage object '{key}' is {length} bytes, exceeding the read limit of {max_bytes}"
            )));
        }

        let bytes = read_exact_blocks(&mut file, length, &key).await?;
        let mut extra = [0_u8; 1];
        if file
            .read(&mut extra)
            .map_err(|error| local_access_error(&key, error.to_string()))?
            != 0
        {
            return Err(changed_while_reading(&key));
        }
        let after = file.metadata().map_err(|error| {
            local_access_error(&key, format!("could not inspect open file: {error}"))
        })?;
        ensure_generation(&key, &generation, &after)?;
        Ok(Some(bytes))
    }

    async fn read_range(self, identity: ReadIdentity) -> ZarrFdwResult<Option<RangedObject>> {
        validate_storage_key(&identity.key, false)?;
        let root = self.root()?;
        let Some((mut file, before)) = open_regular_file(&root, &identity.key)? else {
            return if identity.generation.is_some() {
                Err(changed_while_reading(&identity.key))
            } else {
                Ok(None)
            };
        };
        let generation = local_generation(&before);
        if let Some(expected) = &identity.generation {
            ensure_same_generation(&identity.key, expected, &generation)?;
        }

        let (start, length) =
            resolved_range(&identity.range, generation.total_len(), &identity.key)?;
        let length_usize = usize::try_from(length).map_err(|_| {
            ZarrFdwError::InvalidMetadata(format!(
                "local range read for object '{}' exceeds this platform's index capacity",
                identity.key
            ))
        })?;
        file.seek(SeekFrom::Start(start))
            .map_err(|error| local_access_error(&identity.key, error.to_string()))?;
        let bytes = read_exact_blocks(&mut file, length_usize, &identity.key).await?;
        let after = file.metadata().map_err(|error| {
            local_access_error(
                &identity.key,
                format!("could not inspect open file: {error}"),
            )
        })?;
        ensure_generation(&identity.key, &generation, &after)?;

        Ok(Some(RangedObject {
            identity: ReadIdentity {
                key: identity.key,
                range: ReadRange::Exact { start, length },
                generation: Some(generation.clone()),
            },
            total_len: generation.total_len(),
            bytes,
        }))
    }

    async fn list_page(
        self,
        path: String,
        continuation_token: Option<String>,
    ) -> ZarrFdwResult<DirectoryPage> {
        validate_storage_key(&path, true)?;
        let after = continuation_child(&path, continuation_token.as_deref())?;
        let root = self.root()?;
        let entries = if path.is_empty() {
            root.entries()
        } else {
            root.read_dir(&path)
        };
        let entries = match entries {
            Ok(entries) => entries,
            Err(error) if error.kind() == ErrorKind::NotFound => {
                return Ok(DirectoryPage {
                    child_prefixes: Vec::new(),
                    next_continuation_token: None,
                });
            }
            Err(error) => {
                return Err(local_access_error(
                    display_path(&path),
                    format!("could not list directory: {error}"),
                ));
            }
        };

        let mut names = BinaryHeap::with_capacity(LIST_PAGE_ENTRIES + 1);
        let mut scanned = 0_usize;
        for entry in entries {
            let entry = entry.map_err(|error| {
                local_access_error(
                    display_path(&path),
                    format!("could not read directory entry: {error}"),
                )
            })?;
            scanned = scanned.saturating_add(1);
            if scanned % LIST_YIELD_ENTRIES == 0 {
                tokio::task::yield_now().await;
            }
            let file_type = entry.file_type().map_err(|error| {
                local_access_error(
                    display_path(&path),
                    format!("could not inspect directory entry: {error}"),
                )
            })?;
            if !file_type.is_dir() {
                continue;
            }
            let Ok(name) = entry.file_name().into_string() else {
                continue;
            };
            if validate_child_name(&name).is_err()
                || after
                    .as_ref()
                    .is_some_and(|after| name.as_str() <= after.as_str())
            {
                continue;
            }
            if names.len() < LIST_PAGE_ENTRIES + 1 {
                names.push(name);
            } else if names
                .peek()
                .is_some_and(|largest| name.as_str() < largest.as_str())
            {
                names.pop();
                names.push(name);
            }
        }

        let mut names = names.into_sorted_vec();
        let has_more = names.len() > LIST_PAGE_ENTRIES;
        if has_more {
            names.truncate(LIST_PAGE_ENTRIES);
        }
        let child_prefixes = names
            .iter()
            .map(|name| join_key(&path, name))
            .collect::<Vec<_>>();
        let next_continuation_token = if has_more {
            child_prefixes.last().cloned()
        } else {
            None
        };
        Ok(DirectoryPage {
            child_prefixes,
            next_continuation_token,
        })
    }
}

impl StorageBackend for LocalBackend {
    fn kind(&self) -> StorageBackendKind {
        StorageBackendKind::Local
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
        path: String,
        continuation_token: Option<String>,
    ) -> StoreFuture<DirectoryPage> {
        let backend = self.clone();
        async move { backend.list_page(path, continuation_token).await }.boxed_local()
    }
}

fn parse_file_url(raw: &str) -> ZarrFdwResult<PathBuf> {
    if !raw.starts_with("file:///") {
        return Err(ZarrFdwError::InvalidFileStoreUrl(
            "expected file:///absolute/path".to_string(),
        ));
    }
    let url = Url::parse(raw).map_err(|_| {
        ZarrFdwError::InvalidFileStoreUrl("expected file:///absolute/path".to_string())
    })?;
    if url.scheme() != "file"
        || url.cannot_be_a_base()
        || url.host_str().is_some()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(ZarrFdwError::InvalidFileStoreUrl(
            "host, credentials, query, and fragment are not allowed".to_string(),
        ));
    }
    let path = url.to_file_path().map_err(|_| {
        ZarrFdwError::InvalidFileStoreUrl("path must be absolute UTF-8".to_string())
    })?;
    let path_text = path.to_str().ok_or_else(|| {
        ZarrFdwError::InvalidFileStoreUrl("path must be absolute UTF-8".to_string())
    })?;
    if !path.is_absolute() || path_text.contains('\0') {
        return Err(ZarrFdwError::InvalidFileStoreUrl(
            "path must be absolute UTF-8".to_string(),
        ));
    }
    Ok(path)
}

fn validate_storage_key(key: &str, allow_empty: bool) -> ZarrFdwResult<()> {
    if key.is_empty() {
        return if allow_empty {
            Ok(())
        } else {
            Err(invalid_key("must not be empty"))
        };
    }
    if key.starts_with('/') || key.ends_with('/') || Path::new(key).is_absolute() {
        return Err(invalid_key("must be a relative object path"));
    }
    if key.contains('\\') || key.chars().any(|character| character.is_ascii_control()) {
        return Err(invalid_key(
            "backslash and ASCII control characters are not allowed",
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
    Ok(())
}

fn validate_child_name(name: &str) -> ZarrFdwResult<()> {
    validate_storage_key(name, false)?;
    if name.contains('/') {
        return Err(invalid_key(
            "directory child and continuation token must contain one path component",
        ));
    }
    Ok(())
}

fn continuation_child(path: &str, token: Option<&str>) -> ZarrFdwResult<Option<String>> {
    let Some(token) = token else {
        return Ok(None);
    };
    validate_storage_key(token, false)?;
    let child = if path.is_empty() {
        token
    } else {
        token
            .strip_prefix(&format!("{path}/"))
            .ok_or_else(|| invalid_key("continuation token does not belong to the directory"))?
    };
    validate_child_name(child)?;
    Ok(Some(child.to_string()))
}

fn open_regular_file(
    root: &Dir,
    key: &str,
) -> ZarrFdwResult<Option<(cap_std::fs::File, Metadata)>> {
    let mut options = OpenOptions::new();
    options.read(true).custom_flags(libc::O_NONBLOCK);
    let file = match root.open_with(key, &options) {
        Ok(file) => file,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(local_access_error(key, error.to_string())),
    };
    let metadata = file.metadata().map_err(|error| {
        local_access_error(key, format!("could not inspect open file: {error}"))
    })?;
    if !metadata.is_file() {
        return Err(ZarrFdwError::InvalidMetadata(format!(
            "local storage object '{key}' is not a regular file"
        )));
    }
    Ok(Some((file, metadata)))
}

async fn read_exact_blocks(
    file: &mut cap_std::fs::File,
    expected: usize,
    key: &str,
) -> ZarrFdwResult<Vec<u8>> {
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(expected).map_err(|_| {
        ZarrFdwError::InvalidMetadata(format!(
            "could not allocate {expected} bytes for local storage object '{key}'"
        ))
    })?;
    let mut block = [0_u8; READ_BLOCK_BYTES];
    while bytes.len() < expected {
        let remaining = expected - bytes.len();
        let read_len = remaining.min(block.len());
        let count = file
            .read(&mut block[..read_len])
            .map_err(|error| local_access_error(key, error.to_string()))?;
        if count == 0 {
            return Err(ZarrFdwError::InvalidMetadata(format!(
                "local storage object '{key}' returned {} bytes, expected exactly {expected} bytes",
                bytes.len()
            )));
        }
        bytes.extend_from_slice(&block[..count]);
        tokio::task::yield_now().await;
    }
    Ok(bytes)
}

fn resolved_range(range: &ReadRange, total: u64, key: &str) -> ZarrFdwResult<(u64, u64)> {
    let (start, length) = match range {
        ReadRange::Whole => {
            return Err(ZarrFdwError::InvalidMetadata(
                "whole-object identity cannot be used for a storage range read".to_string(),
            ));
        }
        ReadRange::Exact { start, length } => (*start, *length),
        ReadRange::Suffix { length } => {
            let start = total.checked_sub(*length).ok_or_else(|| {
                ZarrFdwError::InvalidMetadata(format!(
                    "local suffix range for object '{key}' exceeds object length {total}"
                ))
            })?;
            (start, *length)
        }
    };
    if length == 0 {
        return Err(ZarrFdwError::InvalidMetadata(
            "storage byte range length must be greater than zero".to_string(),
        ));
    }
    let end = start.checked_add(length).ok_or_else(|| {
        ZarrFdwError::InvalidMetadata("storage byte range end overflows u64".to_string())
    })?;
    if end > total {
        return Err(ZarrFdwError::InvalidMetadata(format!(
            "local byte range {start}..{end} for object '{key}' exceeds object length {total}"
        )));
    }
    Ok((start, length))
}

fn local_generation(metadata: &Metadata) -> ObjectGeneration {
    let total_len = metadata.size();
    ObjectGeneration::Local {
        fingerprint: format!(
            "{}:{}:{total_len}:{}:{}:{}:{}",
            metadata.dev(),
            metadata.ino(),
            metadata.mtime(),
            metadata.mtime_nsec(),
            metadata.ctime(),
            metadata.ctime_nsec()
        ),
        total_len,
    }
}

fn ensure_generation(
    key: &str,
    expected: &ObjectGeneration,
    actual: &Metadata,
) -> ZarrFdwResult<()> {
    ensure_same_generation(key, expected, &local_generation(actual))
}

fn ensure_same_generation(
    key: &str,
    expected: &ObjectGeneration,
    actual: &ObjectGeneration,
) -> ZarrFdwResult<()> {
    if expected.backend_kind() != StorageBackendKind::Local
        || actual.backend_kind() != StorageBackendKind::Local
        || expected != actual
    {
        return Err(changed_while_reading(key));
    }
    Ok(())
}

fn changed_while_reading(key: &str) -> ZarrFdwError {
    ZarrFdwError::InvalidMetadata(format!(
        "local storage object '{key}' changed while reading"
    ))
}

fn invalid_key(message: impl Into<String>) -> ZarrFdwError {
    ZarrFdwError::InvalidLocalStorageKey(message.into())
}

fn local_access_error(key: impl Into<String>, message: impl Into<String>) -> ZarrFdwError {
    ZarrFdwError::LocalStorageAccess {
        key: key.into(),
        message: message.into(),
    }
}

fn display_path(path: &str) -> &str {
    if path.is_empty() { "/" } else { path }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_ROOT: AtomicU64 = AtomicU64::new(0);

    struct TestRoot {
        path: PathBuf,
    }

    impl TestRoot {
        fn new() -> Self {
            let id = NEXT_ROOT.fetch_add(1, Ordering::Relaxed);
            let path =
                std::env::temp_dir().join(format!("zarr-fdw-local-{}-{id}", std::process::id()));
            fs::create_dir(&path).unwrap();
            Self { path }
        }

        fn backend(&self) -> LocalBackend {
            let url = Url::from_directory_path(&self.path).unwrap();
            LocalBackend::new(url.as_str()).unwrap()
        }
    }

    impl Drop for TestRoot {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    #[test]
    fn file_urls_are_absolute_and_authority_free() {
        assert!(parse_file_url("file:///tmp/example.zarr").is_ok());
        for invalid in [
            "file:relative",
            "file://host/tmp/zarr",
            "file:///tmp/zarr?query=1",
            "file:///tmp/zarr#fragment",
            "https://host/tmp/zarr",
        ] {
            assert!(parse_file_url(invalid).is_err(), "{invalid}");
        }
    }

    #[test]
    fn keys_reject_escaping_and_ambiguous_components() {
        assert!(validate_storage_key("nested/array/zarr.json", false).is_ok());
        assert!(validate_storage_key("", true).is_ok());
        for invalid in [
            "",
            "/absolute",
            "trailing/",
            "double//separator",
            "./child",
            "parent/../child",
            "back\\slash",
            "control\ncharacter",
            "nul\0character",
        ] {
            assert!(validate_storage_key(invalid, false).is_err(), "{invalid:?}");
        }
        assert_eq!(
            continuation_child("nested", Some("nested/child")).unwrap(),
            Some("child".to_string())
        );
        assert!(continuation_child("nested", Some("other/child")).is_err());
        assert!(continuation_child("", Some("nested/child")).is_err());
    }

    #[test]
    fn whole_reads_are_bounded_and_missing_is_optional() {
        let root = TestRoot::new();
        fs::write(root.path.join("object"), b"abcdef").unwrap();
        let backend = root.backend();
        let runtime = tokio::runtime::Runtime::new().unwrap();

        assert_eq!(
            runtime
                .block_on(backend.clone().read_whole("object".to_string(), 6))
                .unwrap(),
            Some(b"abcdef".to_vec())
        );
        assert!(
            runtime
                .block_on(backend.clone().read_whole("object".to_string(), 5))
                .is_err()
        );
        assert_eq!(
            runtime
                .block_on(backend.read_whole("missing".to_string(), 6))
                .unwrap(),
            None
        );
    }

    #[test]
    fn exact_and_suffix_reads_normalize_to_exact_ranges() {
        let root = TestRoot::new();
        fs::write(root.path.join("object"), b"0123456789").unwrap();
        let backend = root.backend();
        let runtime = tokio::runtime::Runtime::new().unwrap();

        let exact = runtime
            .block_on(
                backend
                    .clone()
                    .read_range(ReadIdentity::exact("object", 2, 4).unwrap()),
            )
            .unwrap()
            .unwrap();
        assert_eq!(exact.bytes.as_slice(), b"2345");
        assert_eq!(
            exact.identity.range,
            ReadRange::Exact {
                start: 2,
                length: 4
            }
        );
        assert_eq!(exact.total_len, 10);

        let suffix = runtime
            .block_on(backend.read_range(ReadIdentity::suffix("object", 3).unwrap()))
            .unwrap()
            .unwrap();
        assert_eq!(suffix.bytes.as_slice(), b"789");
        assert_eq!(
            suffix.identity.range,
            ReadRange::Exact {
                start: 7,
                length: 3
            }
        );
    }

    #[test]
    fn ranges_reject_bounds_nonregular_files_and_absolute_keys() {
        let root = TestRoot::new();
        fs::write(root.path.join("object"), b"0123").unwrap();
        fs::create_dir(root.path.join("directory")).unwrap();
        let backend = root.backend();
        let runtime = tokio::runtime::Runtime::new().unwrap();

        assert!(
            runtime
                .block_on(
                    backend
                        .clone()
                        .read_range(ReadIdentity::exact("object", 3, 2).unwrap())
                )
                .is_err()
        );
        assert!(
            runtime
                .block_on(
                    backend
                        .clone()
                        .read_range(ReadIdentity::suffix("object", 5).unwrap())
                )
                .is_err()
        );
        let error = runtime
            .block_on(
                backend
                    .clone()
                    .read_range(ReadIdentity::exact("directory", 0, 1).unwrap()),
            )
            .unwrap_err();
        assert!(error.to_string().contains("is not a regular file"));
        assert!(
            runtime
                .block_on(backend.read_range(ReadIdentity::exact("/object", 0, 1).unwrap()))
                .is_err()
        );
    }

    #[test]
    fn generation_condition_detects_replacement_and_disappearance() {
        let root = TestRoot::new();
        fs::write(root.path.join("shard"), b"abcdefgh").unwrap();
        let backend = root.backend();
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let indexed = runtime
            .block_on(
                backend
                    .clone()
                    .read_range(ReadIdentity::suffix("shard", 4).unwrap()),
            )
            .unwrap()
            .unwrap();
        let generation = indexed.identity.generation.unwrap();

        fs::write(root.path.join("replacement"), b"ABCDEFGH").unwrap();
        fs::rename(root.path.join("replacement"), root.path.join("shard")).unwrap();
        let replaced = ReadIdentity::exact("shard", 0, 4)
            .unwrap()
            .with_generation(generation.clone());
        assert!(
            runtime
                .block_on(backend.clone().read_range(replaced))
                .unwrap_err()
                .to_string()
                .contains("changed while reading")
        );

        fs::remove_file(root.path.join("shard")).unwrap();
        let missing = ReadIdentity::exact("shard", 0, 4)
            .unwrap()
            .with_generation(generation);
        assert!(
            runtime
                .block_on(backend.read_range(missing))
                .unwrap_err()
                .to_string()
                .contains("changed while reading")
        );
    }

    #[test]
    fn local_ranges_reject_s3_generations() {
        let root = TestRoot::new();
        fs::write(root.path.join("object"), b"0123").unwrap();
        let backend = root.backend();
        let identity = ReadIdentity::exact("object", 0, 1)
            .unwrap()
            .with_generation(ObjectGeneration::S3 {
                etag: "\"s3-etag\"".to_string(),
                version_id: None,
                total_len: 4,
            });
        let runtime = tokio::runtime::Runtime::new().unwrap();
        assert!(runtime.block_on(backend.read_range(identity)).is_err());
    }

    #[test]
    fn directory_listing_is_sorted_bounded_and_paginated() {
        let root = TestRoot::new();
        for index in (0..=LIST_PAGE_ENTRIES).rev() {
            fs::create_dir(root.path.join(format!("child-{index:04}"))).unwrap();
        }
        fs::write(root.path.join("ordinary-object"), b"ignored").unwrap();
        let backend = root.backend();
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let first = runtime
            .block_on(backend.clone().list_page(String::new(), None))
            .unwrap();
        assert_eq!(first.child_prefixes.len(), LIST_PAGE_ENTRIES);
        assert_eq!(first.child_prefixes.first().unwrap(), "child-0000");
        assert_eq!(first.child_prefixes.last().unwrap(), "child-0999");
        assert_eq!(first.next_continuation_token.as_deref(), Some("child-0999"));

        let second = runtime
            .block_on(backend.list_page(String::new(), first.next_continuation_token))
            .unwrap();
        assert_eq!(second.child_prefixes, vec!["child-1000".to_string()]);
        assert!(second.next_continuation_token.is_none());
    }

    #[cfg(unix)]
    #[test]
    fn capability_root_rejects_symlink_escape() {
        use std::os::unix::fs::symlink;

        let root = TestRoot::new();
        let outside = TestRoot::new();
        fs::write(outside.path.join("secret"), b"outside").unwrap();
        symlink(&outside.path, root.path.join("escape")).unwrap();
        let backend = root.backend();
        let runtime = tokio::runtime::Runtime::new().unwrap();
        assert!(
            runtime
                .block_on(backend.read_whole("escape/secret".to_string(), 64))
                .is_err()
        );
    }

    #[cfg(unix)]
    #[test]
    fn nonblocking_open_rejects_fifo_without_reading_it() {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;

        let root = TestRoot::new();
        let fifo = root.path.join("fifo");
        let fifo = CString::new(fifo.as_os_str().as_bytes()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(fifo.as_ptr(), 0o600) }, 0);

        let backend = root.backend();
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let error = runtime
            .block_on(backend.read_whole("fifo".to_string(), 64))
            .unwrap_err();
        assert!(error.to_string().contains("is not a regular file"));
    }
}
