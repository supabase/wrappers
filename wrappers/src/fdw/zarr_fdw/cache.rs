//! Query-local cache for complete encoded Zarr chunk objects.

use lru::LruCache;
use std::sync::Arc;

/// A complete object response cached before decompression.
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
    entries: LruCache<String, CachedObject>,
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

    pub(crate) fn get(&mut self, key: &str) -> Option<CachedObject> {
        self.entries.get(key).cloned()
    }

    /// Insert a complete encoded object. Returns `false` when caching is
    /// disabled or the object is larger than the entire byte budget.
    pub(crate) fn insert_present(&mut self, key: String, bytes: Arc<[u8]>) -> bool {
        self.insert(key, CachedObject::Present(bytes))
    }

    /// Cache an explicit object-not-found response.
    pub(crate) fn insert_missing(&mut self, key: String) -> bool {
        self.insert(key, CachedObject::Missing)
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

    fn insert(&mut self, key: String, object: CachedObject) -> bool {
        if let Some(previous) = self.entries.pop(&key) {
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
        self.entries.put(key, object);
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
}
