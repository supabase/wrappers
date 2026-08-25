//! Query-local cache for complete bounded Zarr storage reads.

use lru::LruCache;
use std::sync::Arc;

use super::store::ReadIdentity;

/// A complete whole-object or exact-range response cached before decoding.
///
/// Missing objects are cached explicitly because a sparse Zarr chunk uses the
/// array's fill-value semantics. Other storage errors must never enter the
/// cache.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum CachedObject {
    Present(Arc<[u8]>),
    Missing,
}

impl CachedObject {
    fn resident_bytes(&self) -> usize {
        match self {
            Self::Present(bytes) => bytes.len(),
            Self::Missing => 0,
        }
    }
}

/// Byte- and entry-bounded LRU owned by one PostgreSQL scan execution.
///
/// The cache is deliberately not global: cached bytes must not cross server,
/// role, credential, or query-lifecycle boundaries. A zero byte or entry
/// limit disables it without requiring a separate optional field at call
/// sites.
pub(crate) struct CompressedChunkCache {
    entries: LruCache<ReadIdentity, CachedObject>,
    resident_bytes: usize,
    max_bytes: usize,
    max_entries: usize,
    evictions: usize,
}

impl CompressedChunkCache {
    pub(crate) fn new(max_bytes: usize, max_entries: usize) -> Self {
        Self {
            entries: LruCache::unbounded(),
            resident_bytes: 0,
            max_bytes,
            max_entries,
            evictions: 0,
        }
    }

    #[cfg(test)]
    pub(crate) fn get(&mut self, key: &str) -> Option<CachedObject> {
        self.get_identity(&ReadIdentity::whole(key))
    }

    /// Look up bytes by the complete key/range/generation identity. This
    /// prevents a whole object, shard index, and inner payload from aliasing.
    pub(crate) fn get_identity(&mut self, identity: &ReadIdentity) -> Option<CachedObject> {
        self.entries.get(identity).cloned()
    }

    /// Insert a complete encoded object. Returns `false` when caching is
    /// disabled or the object is larger than the entire byte budget.
    #[cfg(test)]
    pub(crate) fn insert_present(&mut self, key: String, bytes: Arc<[u8]>) -> bool {
        self.insert_present_identity(ReadIdentity::whole(key), bytes)
    }

    pub(crate) fn insert_present_identity(
        &mut self,
        identity: ReadIdentity,
        bytes: Arc<[u8]>,
    ) -> bool {
        self.insert(identity, CachedObject::Present(bytes))
    }

    /// Cache an explicit object-not-found response.
    #[cfg(test)]
    pub(crate) fn insert_missing(&mut self, key: String) -> bool {
        self.insert_missing_identity(ReadIdentity::whole(key))
    }

    pub(crate) fn insert_missing_identity(&mut self, identity: ReadIdentity) -> bool {
        self.insert(identity, CachedObject::Missing)
    }

    #[cfg(test)]
    pub(crate) fn clear(&mut self) {
        self.entries.clear();
        self.resident_bytes = 0;
    }

    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub(crate) fn resident_bytes(&self) -> usize {
        self.resident_bytes
    }

    pub(crate) fn evictions(&self) -> usize {
        self.evictions
    }

    fn insert(&mut self, identity: ReadIdentity, object: CachedObject) -> bool {
        if let Some(previous) = self.entries.pop(&identity) {
            self.resident_bytes = self
                .resident_bytes
                .saturating_sub(previous.resident_bytes());
        }

        let object_bytes = object.resident_bytes();
        if self.max_bytes == 0 || self.max_entries == 0 || object_bytes > self.max_bytes {
            return false;
        }

        while self.entries.len() >= self.max_entries
            || self
                .resident_bytes
                .checked_add(object_bytes)
                .is_none_or(|next| next > self.max_bytes)
        {
            let Some((_key, evicted)) = self.entries.pop_lru() else {
                break;
            };
            self.resident_bytes = self.resident_bytes.saturating_sub(evicted.resident_bytes());
            self.evictions = self.evictions.saturating_add(1);
        }

        self.resident_bytes += object_bytes;
        self.entries.put(identity, object);
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bytes(value: u8, len: usize) -> Arc<[u8]> {
        Arc::from(vec![value; len])
    }

    #[test]
    fn weighted_lru_evicts_by_bytes_and_promotes_hits() {
        let mut cache = CompressedChunkCache::new(6, 3);
        assert!(cache.insert_present("a".to_string(), bytes(1, 3)));
        assert!(cache.insert_present("b".to_string(), bytes(2, 3)));

        assert_eq!(cache.get("a"), Some(CachedObject::Present(bytes(1, 3))));
        assert!(cache.insert_present("c".to_string(), bytes(3, 3)));

        assert!(cache.get("b").is_none());
        assert!(cache.get("a").is_some());
        assert!(cache.get("c").is_some());
        assert_eq!(cache.resident_bytes(), 6);
    }

    #[test]
    fn entry_limit_bounds_zero_weight_missing_objects() {
        let mut cache = CompressedChunkCache::new(16, 2);
        assert!(cache.insert_missing("a".to_string()));
        assert!(cache.insert_missing("b".to_string()));
        assert!(cache.insert_missing("c".to_string()));

        assert!(cache.get("a").is_none());
        assert_eq!(cache.get("b"), Some(CachedObject::Missing));
        assert_eq!(cache.get("c"), Some(CachedObject::Missing));
        assert_eq!(cache.len(), 2);
        assert_eq!(cache.resident_bytes(), 0);
    }

    #[test]
    fn oversized_replacement_removes_a_stale_value_and_bypasses_cache() {
        let mut cache = CompressedChunkCache::new(4, 2);
        assert!(cache.insert_present("chunk".to_string(), bytes(1, 4)));
        assert!(!cache.insert_present("chunk".to_string(), bytes(2, 5)));
        assert!(cache.get("chunk").is_none());
        assert_eq!(cache.resident_bytes(), 0);
    }

    #[test]
    fn zero_limit_disables_and_clear_resets_accounting() {
        let mut disabled = CompressedChunkCache::new(0, 8);
        assert!(!disabled.insert_missing("missing".to_string()));
        assert_eq!(disabled.len(), 0);

        let mut cache = CompressedChunkCache::new(8, 2);
        assert!(cache.insert_present("chunk".to_string(), bytes(1, 8)));
        cache.clear();
        assert!(cache.is_empty());
        assert_eq!(cache.resident_bytes(), 0);
    }

    #[test]
    fn whole_ranges_and_generations_do_not_alias() {
        let mut cache = CompressedChunkCache::new(32, 8);
        let whole = ReadIdentity::whole("shard");
        let exact = ReadIdentity::exact("shard", 0, 4).unwrap();
        let generation = super::super::store::ObjectGeneration {
            etag: "etag-a".to_string(),
            version_id: None,
            total_len: 64,
        };
        let exact_generation = exact.clone().with_generation(generation);

        assert!(cache.insert_present_identity(whole.clone(), bytes(1, 4)));
        assert!(cache.insert_present_identity(exact.clone(), bytes(2, 4)));
        assert!(cache.insert_present_identity(exact_generation.clone(), bytes(3, 4)));

        assert_eq!(
            cache.get_identity(&whole),
            Some(CachedObject::Present(bytes(1, 4)))
        );
        assert_eq!(
            cache.get_identity(&exact),
            Some(CachedObject::Present(bytes(2, 4)))
        );
        assert_eq!(
            cache.get_identity(&exact_generation),
            Some(CachedObject::Present(bytes(3, 4)))
        );
    }
}
