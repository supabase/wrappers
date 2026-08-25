//! Bounded Zarr v3 `sharding_indexed` metadata and index execution.
//!
//! Only range-addressable shards are accepted: one top-level sharding codec,
//! the existing direct inner codec pipeline, and a fixed-size little-endian
//! index encoded as `bytes -> [crc32c]?`. Shard objects are never read whole.

use std::sync::Arc;

use lru::LruCache;
use serde_json::{Map, Value};

use super::codec::CodecPipeline;
use super::store::{RangedObject, ReadIdentity, ReadRange};
use super::{ZarrFdwError, ZarrFdwResult};

const INDEX_ENTRY_BYTES: usize = 16;
const CRC32C_BYTES: usize = 4;
const INDEX_INTERRUPT_POLL_ENTRIES: usize = 4096;
const CRC_INTERRUPT_POLL_BYTES: usize = 1024 * 1024;
pub(crate) const MAX_SHARD_INDEX_BYTES: usize = 64 * 1024 * 1024 + CRC32C_BYTES;
const MISSING_SENTINEL: u64 = u64::MAX;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum IndexLocation {
    Start,
    End,
}

impl IndexLocation {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Start => "start",
            Self::End => "end",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ShardIndexCodec {
    pub checksum: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ShardingConfig {
    /// Native outer regular-grid chunk shape: one storage object per cell.
    pub shard_shape: Vec<u64>,
    /// Executor-visible logical chunk shape within each shard.
    pub inner_chunk_shape: Vec<u64>,
    pub chunks_per_shard: Vec<u64>,
    pub inner_codecs: CodecPipeline,
    pub index_codec: ShardIndexCodec,
    pub index_location: IndexLocation,
    pub index_entry_count: usize,
    pub decoded_index_bytes: usize,
    pub encoded_index_bytes: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ShardChunkAddress {
    pub shard_indices: Vec<u64>,
    pub inner_indices: Vec<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum StorageLayout {
    Direct,
    Sharded(ShardingConfig),
}

impl Default for StorageLayout {
    fn default() -> Self {
        Self::Direct
    }
}

impl StorageLayout {
    pub(crate) fn ordered_label(&self) -> String {
        match self {
            Self::Direct => "direct".to_string(),
            Self::Sharded(config) => format!(
                "sharding_indexed (index: {})",
                config.index_location.label()
            ),
        }
    }
}

impl ShardingConfig {
    /// Parse the complete top-level v3 codec list for a range-readable shard.
    /// The outer list must contain exactly one `sharding_indexed` codec.
    /// Returns the config and executor-normalized dtype produced by the inner
    /// pipeline.
    pub(crate) fn from_v3(
        native_dtype: &str,
        shard_shape: &[u64],
        codecs: &Value,
    ) -> ZarrFdwResult<(Self, String)> {
        let codecs = codecs.as_array().ok_or_else(|| {
            ZarrFdwError::InvalidMetadata("Zarr v3 codecs must be an array".to_string())
        })?;
        if codecs.len() != 1 {
            return Err(ZarrFdwError::InvalidMetadata(
                "range-readable sharded arrays require exactly one top-level sharding_indexed codec"
                    .to_string(),
            ));
        }
        let codec = codec_object(&codecs[0], "sharding codec")?;
        validate_fields(
            codec,
            &["name", "configuration", "must_understand"],
            "sharding codec",
        )?;
        validate_must_understand(codec, "sharding codec")?;
        if required_string(codec, "name", "sharding codec")? != "sharding_indexed" {
            return Err(ZarrFdwError::InvalidMetadata(
                "range-readable sharded arrays require a top-level sharding_indexed codec"
                    .to_string(),
            ));
        }
        let configuration = required_object(codec, "configuration", "sharding codec")?;
        validate_fields(
            configuration,
            &["chunk_shape", "codecs", "index_codecs", "index_location"],
            "sharding_indexed configuration",
        )?;

        if shard_shape.is_empty() {
            return Err(ZarrFdwError::InvalidMetadata(
                "sharding_indexed requires a non-empty shard shape".to_string(),
            ));
        }
        let inner_chunk_shape = required_u64_array(
            configuration,
            "chunk_shape",
            "sharding_indexed configuration",
        )?;
        if inner_chunk_shape.len() != shard_shape.len() {
            return Err(ZarrFdwError::InvalidMetadata(format!(
                "sharding_indexed inner chunk rank {} does not match shard rank {}",
                inner_chunk_shape.len(),
                shard_shape.len()
            )));
        }

        let mut chunks_per_shard = Vec::with_capacity(shard_shape.len());
        for (axis, (&shard, &inner)) in shard_shape.iter().zip(inner_chunk_shape.iter()).enumerate()
        {
            if shard == 0 || inner == 0 {
                return Err(ZarrFdwError::InvalidMetadata(format!(
                    "sharding_indexed chunk dimensions must be greater than zero on axis {axis}"
                )));
            }
            if shard % inner != 0 {
                return Err(ZarrFdwError::InvalidMetadata(format!(
                    "sharding_indexed inner chunk dimension {inner} does not evenly divide shard dimension {shard} on axis {axis}"
                )));
            }
            chunks_per_shard.push(shard / inner);
        }

        let index_entry_count = chunks_per_shard.iter().try_fold(1usize, |count, &extent| {
            let extent = usize::try_from(extent).map_err(|_| {
                ZarrFdwError::InvalidMetadata(
                    "shard index shape exceeds this platform's index capacity".to_string(),
                )
            })?;
            count.checked_mul(extent).ok_or_else(|| {
                ZarrFdwError::InvalidMetadata(
                    "shard index entry count exceeds this platform's index capacity".to_string(),
                )
            })
        })?;
        let decoded_index_bytes = index_entry_count
            .checked_mul(INDEX_ENTRY_BYTES)
            .ok_or_else(|| {
                ZarrFdwError::InvalidMetadata(
                    "decoded shard index byte size exceeds this platform's index capacity"
                        .to_string(),
                )
            })?;

        let index_codecs = configuration.get("index_codecs").ok_or_else(|| {
            ZarrFdwError::InvalidMetadata(
                "sharding_indexed configuration is missing 'index_codecs'".to_string(),
            )
        })?;
        let index_codec = parse_index_codecs(index_codecs)?;
        let encoded_index_bytes = decoded_index_bytes
            .checked_add(if index_codec.checksum {
                CRC32C_BYTES
            } else {
                0
            })
            .ok_or_else(|| {
                ZarrFdwError::InvalidMetadata(
                    "encoded shard index byte size exceeds this platform's index capacity"
                        .to_string(),
                )
            })?;
        if encoded_index_bytes > MAX_SHARD_INDEX_BYTES {
            return Err(ZarrFdwError::InvalidMetadata(format!(
                "encoded shard index requires {encoded_index_bytes} bytes, exceeding the safety limit of {MAX_SHARD_INDEX_BYTES}"
            )));
        }

        let inner_codecs = configuration.get("codecs").ok_or_else(|| {
            ZarrFdwError::InvalidMetadata(
                "sharding_indexed configuration is missing 'codecs'".to_string(),
            )
        })?;
        let (inner_codecs, normalized_dtype) =
            CodecPipeline::from_v3(native_dtype, shard_shape.len(), inner_codecs)?;

        let index_location = match configuration.get("index_location") {
            None => IndexLocation::End,
            Some(Value::String(location)) if location == "start" => IndexLocation::Start,
            Some(Value::String(location)) if location == "end" => IndexLocation::End,
            Some(Value::String(location)) => {
                return Err(ZarrFdwError::InvalidMetadata(format!(
                    "sharding_indexed index_location must be 'start' or 'end', got '{location}'"
                )));
            }
            Some(_) => {
                return Err(ZarrFdwError::InvalidMetadata(
                    "sharding_indexed index_location must be a string".to_string(),
                ));
            }
        };

        Ok((
            Self {
                shard_shape: shard_shape.to_vec(),
                inner_chunk_shape,
                chunks_per_shard,
                inner_codecs,
                index_codec,
                index_location,
                index_entry_count,
                decoded_index_bytes,
                encoded_index_bytes,
            },
            normalized_dtype,
        ))
    }

    /// Exact start range or suffix range used to fetch this shard's index.
    pub(crate) fn index_read_identity(
        &self,
        shard_key: impl Into<String>,
    ) -> ZarrFdwResult<ReadIdentity> {
        let length = u64::try_from(self.encoded_index_bytes).map_err(|_| {
            ZarrFdwError::InvalidMetadata(
                "encoded shard index size exceeds the Zarr u64 range capacity".to_string(),
            )
        })?;
        match self.index_location {
            IndexLocation::Start => ReadIdentity::exact(shard_key, 0, length),
            IndexLocation::End => ReadIdentity::suffix(shard_key, length),
        }
    }

    /// Map an executor logical-chunk coordinate to its outer shard key
    /// coordinate and C-order inner-index coordinate.
    pub(crate) fn chunk_address(
        &self,
        logical_chunk_indices: &[u64],
    ) -> ZarrFdwResult<ShardChunkAddress> {
        let (shard_indices, inner_indices) = self.split_logical_indices(logical_chunk_indices)?;
        Ok(ShardChunkAddress {
            shard_indices,
            inner_indices,
        })
    }

    pub(crate) fn split_logical_indices(
        &self,
        logical_chunk_indices: &[u64],
    ) -> ZarrFdwResult<(Vec<u64>, Vec<u64>)> {
        if logical_chunk_indices.len() != self.chunks_per_shard.len() {
            return Err(ZarrFdwError::InvalidMetadata(format!(
                "logical chunk index rank {} does not match sharded array rank {}",
                logical_chunk_indices.len(),
                self.chunks_per_shard.len()
            )));
        }
        let mut shard_indices = Vec::with_capacity(logical_chunk_indices.len());
        let mut inner_indices = Vec::with_capacity(logical_chunk_indices.len());
        for (&logical, &per_shard) in logical_chunk_indices
            .iter()
            .zip(self.chunks_per_shard.iter())
        {
            if per_shard == 0 {
                return Err(ZarrFdwError::InvalidMetadata(
                    "chunks per shard must be greater than zero".to_string(),
                ));
            }
            shard_indices.push(logical / per_shard);
            inner_indices.push(logical % per_shard);
        }
        Ok((shard_indices, inner_indices))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ShardEntry {
    Missing,
    Present { offset: u64, nbytes: u64 },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ShardIndex {
    entries: Vec<ShardEntry>,
    chunks_per_shard: Vec<u64>,
    index_identity: ReadIdentity,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ShardIndexDecode {
    Decoded(ShardIndex),
    Interrupted,
}

impl ShardIndex {
    /// Decode and validate one exactly ranged index response. All offsets and
    /// lengths are checked against the shard and index regions. Physical
    /// payload order and shared/overlapping payload representations remain
    /// unconstrained, as required by the sharding specification.
    pub(crate) fn decode_interruptible<F>(
        config: &ShardingConfig,
        response: RangedObject,
        mut interrupt_pending: F,
    ) -> ZarrFdwResult<ShardIndexDecode>
    where
        F: FnMut() -> bool,
    {
        let shard_key = response.identity.key.clone();
        if response.bytes.len() != config.encoded_index_bytes {
            return Err(shard_error(
                &shard_key,
                format!(
                    "shard index range returned {} bytes, expected exactly {} bytes",
                    response.bytes.len(),
                    config.encoded_index_bytes
                ),
            ));
        }
        let generation = response.identity.generation.as_ref().ok_or_else(|| {
            shard_error(
                &shard_key,
                "shard index response is missing its object generation",
            )
        })?;
        if generation.validator_is_empty() {
            return Err(shard_error(
                &shard_key,
                "shard index response has an empty object-generation validator",
            ));
        }
        if generation.total_len() != response.total_len {
            return Err(shard_error(
                &shard_key,
                format!(
                    "shard index generation length {} does not match Content-Range total {}",
                    generation.total_len(),
                    response.total_len
                ),
            ));
        }
        validate_index_range(config, &response)?;
        if interrupt_pending() {
            return Ok(ShardIndexDecode::Interrupted);
        }

        let mut bytes = response.bytes;
        if config.index_codec.checksum {
            let payload_len = bytes.len().checked_sub(CRC32C_BYTES).ok_or_else(|| {
                shard_error(
                    &shard_key,
                    "shard index codec index 1 ('crc32c'): index is truncated before the checksum",
                )
            })?;
            let expected = u32::from_le_bytes(
                bytes[payload_len..]
                    .try_into()
                    .expect("crc suffix is exactly four bytes"),
            );
            let mut actual = 0u32;
            for block in bytes[..payload_len].chunks(CRC_INTERRUPT_POLL_BYTES) {
                if interrupt_pending() {
                    return Ok(ShardIndexDecode::Interrupted);
                }
                actual = crc32c::crc32c_append(actual, block);
            }
            if actual != expected {
                return Err(shard_error(
                    &shard_key,
                    format!(
                        "shard index codec index 1 ('crc32c'): checksum mismatch: expected {expected:#010x}, computed {actual:#010x}"
                    ),
                ));
            }
            bytes.truncate(payload_len);
        }
        if bytes.len() != config.decoded_index_bytes {
            return Err(shard_error(
                &shard_key,
                format!(
                    "decoded shard index has {} bytes, expected exactly {} bytes",
                    bytes.len(),
                    config.decoded_index_bytes
                ),
            ));
        }

        let index_region = index_region(config, response.total_len)?;
        let mut entries = Vec::new();
        entries
            .try_reserve_exact(config.index_entry_count)
            .map_err(|_| {
                shard_error(
                    &shard_key,
                    format!(
                        "could not allocate {} shard index entries",
                        config.index_entry_count
                    ),
                )
            })?;
        for (index, pair) in bytes.chunks_exact(INDEX_ENTRY_BYTES).enumerate() {
            if index % INDEX_INTERRUPT_POLL_ENTRIES == 0 && interrupt_pending() {
                return Ok(ShardIndexDecode::Interrupted);
            }
            let offset = u64::from_le_bytes(pair[..8].try_into().expect("eight offset bytes"));
            let nbytes = u64::from_le_bytes(pair[8..].try_into().expect("eight length bytes"));
            let entry = match (offset == MISSING_SENTINEL, nbytes == MISSING_SENTINEL) {
                (true, true) => ShardEntry::Missing,
                (true, false) | (false, true) => {
                    return Err(shard_error(
                        &shard_key,
                        format!(
                            "shard index entry {index} uses a mixed uint64 missing sentinel; offset and nbytes must both be 2^64 - 1"
                        ),
                    ));
                }
                (false, false) => {
                    if nbytes == 0 {
                        return Err(shard_error(
                            &shard_key,
                            format!("shard index entry {index} has zero encoded bytes"),
                        ));
                    }
                    let end = offset.checked_add(nbytes).ok_or_else(|| {
                        shard_error(
                            &shard_key,
                            format!("shard index entry {index} byte range overflows u64"),
                        )
                    })?;
                    if end > response.total_len {
                        return Err(shard_error(
                            &shard_key,
                            format!(
                                "inner chunk byte range {offset}..{end} from shard index entry {index} exceeds shard object length {}",
                                response.total_len
                            ),
                        ));
                    }
                    if offset < index_region.1 && end > index_region.0 {
                        return Err(shard_error(
                            &shard_key,
                            format!(
                                "inner chunk byte range {offset}..{end} from shard index entry {index} overlaps shard index region {}..{}",
                                index_region.0, index_region.1
                            ),
                        ));
                    }
                    ShardEntry::Present { offset, nbytes }
                }
            };
            entries.push(entry);
        }
        if entries.len() != config.index_entry_count {
            return Err(shard_error(
                &shard_key,
                format!(
                    "decoded shard index has {} entries, expected exactly {}",
                    entries.len(),
                    config.index_entry_count
                ),
            ));
        }
        if interrupt_pending() {
            return Ok(ShardIndexDecode::Interrupted);
        }

        Ok(ShardIndexDecode::Decoded(Self {
            entries,
            chunks_per_shard: config.chunks_per_shard.clone(),
            index_identity: response.identity,
        }))
    }

    pub(crate) fn entry(&self, inner_indices: &[u64]) -> ZarrFdwResult<ShardEntry> {
        if inner_indices.len() != self.chunks_per_shard.len() {
            return Err(shard_error(
                &self.index_identity.key,
                format!(
                    "inner chunk index rank {} does not match shard rank {}",
                    inner_indices.len(),
                    self.chunks_per_shard.len()
                ),
            ));
        }
        let flat = inner_indices
            .iter()
            .zip(self.chunks_per_shard.iter())
            .enumerate()
            .try_fold(0usize, |flat, (axis, (&index, &extent))| {
                if index >= extent {
                    return Err(shard_error(
                        &self.index_identity.key,
                        format!(
                            "inner chunk index {index} is outside shard extent {extent} on axis {axis}"
                        ),
                    ));
                }
                let extent = usize::try_from(extent).map_err(|_| {
                    shard_error(
                        &self.index_identity.key,
                        "shard index extent exceeds this platform's index capacity",
                    )
                })?;
                let index = usize::try_from(index).map_err(|_| {
                    shard_error(
                        &self.index_identity.key,
                        "inner chunk index exceeds this platform's index capacity",
                    )
                })?;
                flat.checked_mul(extent)
                    .and_then(|flat| flat.checked_add(index))
                    .ok_or_else(|| {
                        shard_error(
                            &self.index_identity.key,
                            "flat shard index position exceeds this platform's index capacity",
                        )
                    })
            })?;
        self.entries.get(flat).copied().ok_or_else(|| {
            shard_error(
                &self.index_identity.key,
                "flat shard index position is outside the decoded index",
            )
        })
    }

    /// Build a generation-conditioned range request for one present entry.
    pub(crate) fn payload_read_identity(
        &self,
        entry: ShardEntry,
    ) -> ZarrFdwResult<Option<ReadIdentity>> {
        let ShardEntry::Present { offset, nbytes } = entry else {
            return Ok(None);
        };
        let generation = self.index_identity.generation.clone().ok_or_else(|| {
            shard_error(
                &self.index_identity.key,
                "decoded shard index is missing its object generation",
            )
        })?;
        Ok(Some(
            ReadIdentity::exact(self.index_identity.key.clone(), offset, nbytes)?
                .with_generation(generation),
        ))
    }

    pub(crate) fn index_identity(&self) -> &ReadIdentity {
        &self.index_identity
    }

    fn resident_bytes(&self) -> usize {
        self.entries
            .capacity()
            .checked_mul(std::mem::size_of::<ShardEntry>())
            .unwrap_or(usize::MAX)
    }
}

/// Query-local byte- and entry-bounded cache of decoded shard indexes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum CachedShardIndex {
    Present(Arc<ShardIndex>),
    Missing,
}

impl CachedShardIndex {
    fn resident_bytes(&self) -> usize {
        match self {
            Self::Present(index) => index.resident_bytes(),
            Self::Missing => 0,
        }
    }
}

pub(crate) struct ShardIndexCache {
    entries: LruCache<ReadIdentity, CachedShardIndex>,
    resident_bytes: usize,
    max_bytes: usize,
    max_entries: usize,
    evictions: usize,
}

impl ShardIndexCache {
    pub(crate) fn new(max_bytes: usize, max_entries: usize) -> Self {
        Self {
            entries: LruCache::unbounded(),
            resident_bytes: 0,
            max_bytes,
            max_entries,
            evictions: 0,
        }
    }

    pub(crate) fn get(&mut self, request: &ReadIdentity) -> Option<CachedShardIndex> {
        self.entries.get(request).cloned()
    }

    /// Cache an index under the request identity that located it (start exact
    /// or end suffix). The cached value retains the resolved exact identity
    /// and observed generation needed by payload reads.
    pub(crate) fn insert_present(&mut self, request: ReadIdentity, index: Arc<ShardIndex>) -> bool {
        self.insert(request, CachedShardIndex::Present(index))
    }

    /// Cache an explicit absent outer shard. Missing entries consume the
    /// entry budget but no byte budget, preventing repeated range GETs for
    /// every logical inner chunk in the shard.
    pub(crate) fn insert_missing(&mut self, request: ReadIdentity) -> bool {
        self.insert(request, CachedShardIndex::Missing)
    }

    fn insert(&mut self, request: ReadIdentity, value: CachedShardIndex) -> bool {
        if let Some(previous) = self.entries.pop(&request) {
            self.resident_bytes = self
                .resident_bytes
                .saturating_sub(previous.resident_bytes());
        }
        let bytes = value.resident_bytes();
        if self.max_bytes == 0 || self.max_entries == 0 || bytes > self.max_bytes {
            return false;
        }
        while self.entries.len() >= self.max_entries
            || self
                .resident_bytes
                .checked_add(bytes)
                .is_none_or(|total| total > self.max_bytes)
        {
            let Some((_key, evicted)) = self.entries.pop_lru() else {
                break;
            };
            self.resident_bytes = self.resident_bytes.saturating_sub(evicted.resident_bytes());
            self.evictions = self.evictions.saturating_add(1);
        }
        self.resident_bytes += bytes;
        self.entries.put(request, value);
        true
    }

    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    pub(crate) fn resident_bytes(&self) -> usize {
        self.resident_bytes
    }

    pub(crate) fn evictions(&self) -> usize {
        self.evictions
    }
}

fn parse_index_codecs(value: &Value) -> ZarrFdwResult<ShardIndexCodec> {
    let codecs = value.as_array().ok_or_else(|| {
        ZarrFdwError::InvalidMetadata("sharding_indexed index_codecs must be an array".to_string())
    })?;
    if !(1..=2).contains(&codecs.len()) {
        return Err(ZarrFdwError::InvalidMetadata(
            "supported shard index codecs are exactly bytes -> [crc32c]?".to_string(),
        ));
    }
    let bytes = codec_object(&codecs[0], "shard index codec index 0")?;
    validate_fields(
        bytes,
        &["name", "configuration", "must_understand"],
        "shard index codec index 0",
    )?;
    validate_must_understand(bytes, "shard index codec index 0")?;
    if required_string(bytes, "name", "shard index codec index 0")? != "bytes" {
        return Err(ZarrFdwError::InvalidMetadata(
            "shard index codec index 0 must be 'bytes'".to_string(),
        ));
    }
    let configuration = required_object(bytes, "configuration", "shard index codec index 0")?;
    validate_fields(
        configuration,
        &["endian"],
        "shard index bytes configuration",
    )?;
    if required_string(configuration, "endian", "shard index bytes configuration")? != "little" {
        return Err(ZarrFdwError::InvalidMetadata(
            "shard index bytes endian must be 'little'".to_string(),
        ));
    }

    let checksum = codecs.len() == 2;
    if checksum {
        let crc = codec_object(&codecs[1], "shard index codec index 1")?;
        validate_fields(
            crc,
            &["name", "configuration", "must_understand"],
            "shard index codec index 1",
        )?;
        validate_must_understand(crc, "shard index codec index 1")?;
        if required_string(crc, "name", "shard index codec index 1")? != "crc32c" {
            return Err(ZarrFdwError::InvalidMetadata(
                "shard index codec index 1 must be 'crc32c'".to_string(),
            ));
        }
        if let Some(configuration) = crc.get("configuration") {
            let configuration = configuration.as_object().ok_or_else(|| {
                ZarrFdwError::InvalidMetadata(
                    "shard index crc32c configuration must be an object".to_string(),
                )
            })?;
            validate_fields(configuration, &[], "shard index crc32c configuration")?;
        }
    }
    Ok(ShardIndexCodec { checksum })
}

fn validate_index_range(config: &ShardingConfig, response: &RangedObject) -> ZarrFdwResult<()> {
    let length = u64::try_from(config.encoded_index_bytes).map_err(|_| {
        shard_error(
            &response.identity.key,
            "encoded shard index length exceeds u64",
        )
    })?;
    let ReadRange::Exact {
        start,
        length: actual_length,
    } = &response.identity.range
    else {
        return Err(shard_error(
            &response.identity.key,
            "shard index response was not normalized to an exact byte range",
        ));
    };
    if *actual_length != length {
        return Err(shard_error(
            &response.identity.key,
            format!("shard index range length is {actual_length}, expected exactly {length} bytes"),
        ));
    }
    let expected_start = match config.index_location {
        IndexLocation::Start => 0,
        IndexLocation::End => response.total_len.checked_sub(length).ok_or_else(|| {
            shard_error(
                &response.identity.key,
                format!(
                    "shard object length {} is smaller than its {length}-byte index",
                    response.total_len
                ),
            )
        })?,
    };
    if *start != expected_start {
        return Err(shard_error(
            &response.identity.key,
            format!(
                "shard index starts at byte {start}, expected byte {expected_start} for index_location '{}'",
                config.index_location.label()
            ),
        ));
    }
    Ok(())
}

fn index_region(config: &ShardingConfig, total_len: u64) -> ZarrFdwResult<(u64, u64)> {
    let length = u64::try_from(config.encoded_index_bytes).map_err(|_| {
        ZarrFdwError::InvalidMetadata("encoded shard index length exceeds u64".to_string())
    })?;
    match config.index_location {
        IndexLocation::Start => Ok((0, length)),
        IndexLocation::End => total_len
            .checked_sub(length)
            .map(|start| (start, total_len))
            .ok_or_else(|| {
                ZarrFdwError::InvalidMetadata(format!(
                    "shard object length {total_len} is smaller than its {length}-byte index"
                ))
            }),
    }
}

fn codec_object<'a>(value: &'a Value, context: &str) -> ZarrFdwResult<&'a Map<String, Value>> {
    value
        .as_object()
        .ok_or_else(|| ZarrFdwError::InvalidMetadata(format!("{context} must be an object")))
}

fn required_object<'a>(
    object: &'a Map<String, Value>,
    field: &str,
    context: &str,
) -> ZarrFdwResult<&'a Map<String, Value>> {
    object.get(field).and_then(Value::as_object).ok_or_else(|| {
        ZarrFdwError::InvalidMetadata(format!("{context} field '{field}' must be an object"))
    })
}

fn required_string<'a>(
    object: &'a Map<String, Value>,
    field: &str,
    context: &str,
) -> ZarrFdwResult<&'a str> {
    object.get(field).and_then(Value::as_str).ok_or_else(|| {
        ZarrFdwError::InvalidMetadata(format!("{context} field '{field}' must be a string"))
    })
}

fn required_u64_array(
    object: &Map<String, Value>,
    field: &str,
    context: &str,
) -> ZarrFdwResult<Vec<u64>> {
    object
        .get(field)
        .and_then(Value::as_array)
        .ok_or_else(|| {
            ZarrFdwError::InvalidMetadata(format!("{context} field '{field}' must be an array"))
        })?
        .iter()
        .map(|value| {
            value.as_u64().ok_or_else(|| {
                ZarrFdwError::InvalidMetadata(format!(
                    "{context} field '{field}' must contain non-negative integers"
                ))
            })
        })
        .collect()
}

fn validate_fields(
    object: &Map<String, Value>,
    allowed: &[&str],
    context: &str,
) -> ZarrFdwResult<()> {
    if let Some(field) = object
        .keys()
        .find(|field| !allowed.contains(&field.as_str()))
    {
        return Err(ZarrFdwError::InvalidMetadata(format!(
            "{context} contains unsupported field '{field}'"
        )));
    }
    Ok(())
}

fn validate_must_understand(object: &Map<String, Value>, context: &str) -> ZarrFdwResult<()> {
    if object
        .get("must_understand")
        .is_some_and(|value| !value.is_boolean())
    {
        return Err(ZarrFdwError::InvalidMetadata(format!(
            "{context} must_understand must be a boolean"
        )));
    }
    Ok(())
}

fn shard_error(key: &str, message: impl Into<String>) -> ZarrFdwError {
    ZarrFdwError::InvalidMetadata(format!("shard '{key}': {}", message.into()))
}

#[cfg(test)]
mod tests {
    use super::super::store::ObjectGeneration;
    use super::*;

    fn codecs(index_location: &str, checksum: bool) -> Value {
        let mut index_codecs = vec![serde_json::json!({
            "name":"bytes",
            "configuration":{"endian":"little"}
        })];
        if checksum {
            index_codecs.push(serde_json::json!({"name":"crc32c"}));
        }
        serde_json::json!([{
            "name":"sharding_indexed",
            "configuration":{
                "chunk_shape":[1,3,2],
                "codecs":[{"name":"bytes","configuration":{"endian":"little"}}],
                "index_codecs":index_codecs,
                "index_location":index_location
            }
        }])
    }

    fn config(location: &str, checksum: bool) -> ShardingConfig {
        ShardingConfig::from_v3("float32", &[2, 3, 4], &codecs(location, checksum))
            .unwrap()
            .0
    }

    fn encoded_index(entries: &[(u64, u64)], checksum: bool) -> Vec<u8> {
        let mut bytes = Vec::new();
        for &(offset, nbytes) in entries {
            bytes.extend_from_slice(&offset.to_le_bytes());
            bytes.extend_from_slice(&nbytes.to_le_bytes());
        }
        if checksum {
            bytes.extend_from_slice(&crc32c::crc32c(&bytes).to_le_bytes());
        }
        bytes
    }

    fn response(config: &ShardingConfig, total_len: u64, bytes: Vec<u8>) -> RangedObject {
        let index_len = config.encoded_index_bytes as u64;
        let start = match config.index_location {
            IndexLocation::Start => 0,
            IndexLocation::End => total_len - index_len,
        };
        RangedObject {
            identity: ReadIdentity::exact("array/c/0/0/0", start, index_len)
                .unwrap()
                .with_generation(ObjectGeneration::S3 {
                    etag: "\"etag-1\"".to_string(),
                    version_id: Some("version-observed-only".to_string()),
                    total_len,
                }),
            total_len,
            bytes,
        }
    }

    #[test]
    fn parses_start_end_and_checked_index_size() {
        let end = config("end", true);
        assert_eq!(end.chunks_per_shard, vec![2, 1, 2]);
        assert_eq!(end.index_entry_count, 4);
        assert_eq!(end.decoded_index_bytes, 64);
        assert_eq!(end.encoded_index_bytes, 68);
        assert_eq!(end.index_location, IndexLocation::End);
        assert!(matches!(
            end.index_read_identity("shard").unwrap().range,
            ReadRange::Suffix { length: 68 }
        ));
        assert_eq!(
            end.chunk_address(&[3, 0, 5]).unwrap(),
            ShardChunkAddress {
                shard_indices: vec![1, 0, 2],
                inner_indices: vec![1, 0, 1]
            }
        );

        let start = config("start", false);
        assert_eq!(start.encoded_index_bytes, 64);
        assert!(matches!(
            start.index_read_identity("shard").unwrap().range,
            ReadRange::Exact {
                start: 0,
                length: 64
            }
        ));

        let mut default_end = codecs("end", true);
        default_end[0]["configuration"]
            .as_object_mut()
            .unwrap()
            .remove("index_location");
        assert_eq!(
            ShardingConfig::from_v3("float32", &[2, 3, 4], &default_end)
                .unwrap()
                .0
                .index_location,
            IndexLocation::End
        );
    }

    #[test]
    fn rejects_invalid_sharding_metadata() {
        let mut invalid = codecs("end", true);
        invalid[0]["configuration"]["chunk_shape"] = serde_json::json!([1, 2, 2]);
        assert!(ShardingConfig::from_v3("float32", &[2, 3, 4], &invalid).is_err());

        let mut invalid = codecs("middle", true);
        assert!(ShardingConfig::from_v3("float32", &[2, 3, 4], &invalid).is_err());
        invalid[0]["configuration"]["index_location"] = serde_json::json!("end");
        invalid[0]["configuration"]["index_codecs"] =
            serde_json::json!([{"name":"gzip","configuration":{"level":1}}]);
        assert!(ShardingConfig::from_v3("float32", &[2, 3, 4], &invalid).is_err());

        let extra_outer = serde_json::json!([
            codecs("end", true)[0].clone(),
            {"name":"crc32c"}
        ]);
        assert!(ShardingConfig::from_v3("float32", &[2, 3, 4], &extra_outer).is_err());
    }

    #[test]
    fn decodes_c_order_entries_and_generation_conditioned_payload_ranges() {
        let config = config("end", true);
        let entries = [(0, 24), (24, 24), (48, 24), (72, 24)];
        let bytes = encoded_index(&entries, true);
        let total_len = 96 + config.encoded_index_bytes as u64;
        let decoded = match ShardIndex::decode_interruptible(
            &config,
            response(&config, total_len, bytes),
            || false,
        )
        .unwrap()
        {
            ShardIndexDecode::Decoded(index) => index,
            ShardIndexDecode::Interrupted => panic!("unexpected interrupt"),
        };
        assert_eq!(
            decoded.entry(&[1, 0, 0]).unwrap(),
            ShardEntry::Present {
                offset: 48,
                nbytes: 24
            }
        );
        let payload = decoded
            .payload_read_identity(decoded.entry(&[0, 0, 1]).unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(
            payload.range,
            ReadRange::Exact {
                start: 24,
                length: 24
            }
        );
        assert_eq!(payload.generation.unwrap().s3_etag(), Some("\"etag-1\""));
    }

    #[test]
    fn both_uint64_max_values_are_the_only_missing_sentinel() {
        let config = config("end", false);
        let valid = [(MISSING_SENTINEL, MISSING_SENTINEL), (0, 1), (1, 1), (2, 1)];
        let total_len = 3 + config.encoded_index_bytes as u64;
        let decoded = ShardIndex::decode_interruptible(
            &config,
            response(&config, total_len, encoded_index(&valid, false)),
            || false,
        )
        .unwrap();
        let ShardIndexDecode::Decoded(decoded) = decoded else {
            panic!("unexpected interrupt")
        };
        assert_eq!(decoded.entry(&[0, 0, 0]).unwrap(), ShardEntry::Missing);
        assert!(
            decoded
                .payload_read_identity(ShardEntry::Missing)
                .unwrap()
                .is_none()
        );

        for mixed in [(MISSING_SENTINEL, 1), (1, MISSING_SENTINEL)] {
            let entries = [mixed, (0, 1), (1, 1), (2, 1)];
            assert!(
                ShardIndex::decode_interruptible(
                    &config,
                    response(&config, total_len, encoded_index(&entries, false)),
                    || false,
                )
                .is_err()
            );
        }
    }

    #[test]
    fn rejects_checksum_oob_index_overlap_and_overflow() {
        let config = config("end", true);
        let total_len = 100 + config.encoded_index_bytes as u64;
        let valid = [(0, 10), (10, 10), (20, 10), (30, 10)];

        let mut bad_crc = encoded_index(&valid, true);
        let last = bad_crc.len() - 1;
        bad_crc[last] ^= 0xff;
        assert!(
            ShardIndex::decode_interruptible(
                &config,
                response(&config, total_len, bad_crc),
                || false,
            )
            .is_err()
        );

        for entries in [
            [(0, 10), (10, 10), (20, 10), (99, 2)],
            [(0, 10), (10, 10), (20, 10), (100, 1)],
            [(0, 10), (10, 10), (20, 10), (u64::MAX - 1, 4)],
        ] {
            assert!(
                ShardIndex::decode_interruptible(
                    &config,
                    response(&config, total_len, encoded_index(&entries, true)),
                    || false,
                )
                .is_err()
            );
        }
    }

    #[test]
    fn start_index_forbids_payload_overlap_and_decode_is_interruptible() {
        let config = config("start", false);
        let index_len = config.encoded_index_bytes as u64;
        let overlap = [
            (0, 1),
            (index_len, 1),
            (index_len + 1, 1),
            (index_len + 2, 1),
        ];
        assert!(
            ShardIndex::decode_interruptible(
                &config,
                response(&config, index_len + 3, encoded_index(&overlap, false)),
                || false,
            )
            .is_err()
        );

        let valid = [
            (index_len, 1),
            (index_len + 1, 1),
            (index_len + 2, 1),
            (index_len + 3, 1),
        ];
        assert_eq!(
            ShardIndex::decode_interruptible(
                &config,
                response(&config, index_len + 4, encoded_index(&valid, false)),
                || true,
            )
            .unwrap(),
            ShardIndexDecode::Interrupted
        );
    }

    #[test]
    fn index_cache_is_bounded_and_keys_start_and_end_requests_distinctly() {
        let config = config("end", false);
        let entries = [(0, 1), (1, 1), (2, 1), (3, 1)];
        let total_len = 4 + config.encoded_index_bytes as u64;
        let ShardIndexDecode::Decoded(index) = ShardIndex::decode_interruptible(
            &config,
            response(&config, total_len, encoded_index(&entries, false)),
            || false,
        )
        .unwrap() else {
            panic!("unexpected interrupt")
        };
        let index = Arc::new(index);
        let request = config.index_read_identity("array/c/0/0/0").unwrap();
        let mut cache = ShardIndexCache::new(index.resident_bytes() * 2, 1);
        assert!(cache.insert_present(request.clone(), Arc::clone(&index)));
        assert!(matches!(
            cache.get(&request),
            Some(CachedShardIndex::Present(_))
        ));
        assert_eq!(cache.len(), 1);
        assert!(cache.resident_bytes() > 0);
        assert_eq!(cache.evictions(), 0);

        let other = config.index_read_identity("array/c/0/0/1").unwrap();
        assert!(cache.insert_present(other.clone(), index));
        assert!(cache.get(&request).is_none());
        assert!(cache.get(&other).is_some());
        assert_eq!(cache.evictions(), 1);

        let missing = config.index_read_identity("array/c/0/0/2").unwrap();
        assert!(cache.insert_missing(missing.clone()));
        assert_eq!(cache.get(&missing), Some(CachedShardIndex::Missing));
        assert_eq!(cache.resident_bytes(), 0);
    }

    #[test]
    fn index_cache_charges_the_actual_entry_allocation() {
        let config = config("end", true);
        let entries = [(0, 24), (24, 24), (48, 24), (72, 24)];
        let total_len = 96 + config.encoded_index_bytes as u64;
        let index = match ShardIndex::decode_interruptible(
            &config,
            response(&config, total_len, encoded_index(&entries, true)),
            || false,
        )
        .unwrap()
        {
            ShardIndexDecode::Decoded(index) => index,
            ShardIndexDecode::Interrupted => panic!("decode was not interrupted"),
        };
        let actual_allocation = index
            .entries
            .capacity()
            .checked_mul(std::mem::size_of::<ShardEntry>())
            .unwrap();
        assert_eq!(index.resident_bytes(), actual_allocation);
        assert!(actual_allocation > config.decoded_index_bytes);

        let mut underfunded = ShardIndexCache::new(config.decoded_index_bytes, 1);
        assert!(!underfunded.insert_present(
            config.index_read_identity("array/c/0/0/0").unwrap(),
            Arc::new(index),
        ));
        assert_eq!(underfunded.resident_bytes(), 0);
    }
}
