//! Query-local Zarr execution metrics and their typed EXPLAIN representation.
//!
//! This module contains no PostgreSQL calls. The shared FDW framework owns
//! rendering [`ExplainProperty`] values, which keeps the counters usable in
//! ordinary Rust tests and avoids coupling scan bookkeeping to `pg_sys`.

use std::time::Duration;

use supabase_wrappers::prelude::ExplainProperty;

/// The purpose of an object-store GET made while executing a scan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReadKind {
    Metadata,
    Coordinate,
    Data,
}

/// Actual work performed by one query-local Zarr FDW instance.
///
/// Byte counters use encoded object bytes for remote I/O and decoded bytes for
/// in-memory payloads. All updates saturate: observability must never make an
/// otherwise valid query fail because a counter overflowed.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ZarrScanMetrics {
    pub(crate) metadata_get_calls: u64,
    pub(crate) coordinate_get_calls: u64,
    pub(crate) data_get_calls: u64,
    pub(crate) metadata_encoded_bytes: u64,
    pub(crate) coordinate_encoded_bytes: u64,
    pub(crate) data_encoded_bytes: u64,
    pub(crate) coordinate_decoded_bytes: u64,
    pub(crate) data_decoded_bytes: u64,
    pub(crate) fill_bytes_synthesized: u64,
    pub(crate) chunks_total: u64,
    pub(crate) chunks_selected: u64,
    pub(crate) chunks_coordinate_pruned: u64,
    pub(crate) chunks_requested: u64,
    pub(crate) chunks_present: u64,
    pub(crate) chunks_missing: u64,
    pub(crate) cache_hits: u64,
    pub(crate) cache_misses: u64,
    pub(crate) cache_evictions: u64,
    pub(crate) shard_index_get_calls: u64,
    pub(crate) shard_index_encoded_bytes: u64,
    pub(crate) shard_payload_get_calls: u64,
    pub(crate) shard_payload_encoded_bytes: u64,
    pub(crate) shard_index_cache_hits: u64,
    pub(crate) shard_index_cache_misses: u64,
    pub(crate) shard_index_cache_evictions: u64,
    pub(crate) logical_cells_examined: u64,
    /// Present only when the FDW evaluates every scan qual exactly.
    pub(crate) logical_cells_matched: Option<u64>,
    pub(crate) tuples_emitted: u64,
    pub(crate) rescans: u64,
    pub(crate) decompression_micros: u64,
    pub(crate) decoding_micros: u64,
    pub(crate) aggregate_micros: u64,
}

/// Runtime metadata and configured resource bounds shown beside actual work.
pub(crate) struct ZarrExplainContext<'a> {
    pub(crate) array: &'a str,
    pub(crate) dimensions: &'a [String],
    pub(crate) shape: &'a [u64],
    pub(crate) chunk_shape: &'a [usize],
    pub(crate) dtype: &'a str,
    pub(crate) codec: &'a str,
    pub(crate) storage_layout: &'a str,
    pub(crate) shard_shape: Option<&'a [u64]>,
    pub(crate) index_location: Option<&'a str>,
    pub(crate) aggregate_mode: &'a str,
    pub(crate) max_concurrent_reads: usize,
    pub(crate) max_inflight_bytes: usize,
    pub(crate) compressed_cache_bytes: usize,
    pub(crate) cache_entries: usize,
    pub(crate) cache_resident_bytes: usize,
    pub(crate) shard_index_cache_bytes: usize,
    pub(crate) shard_index_cache_entries: usize,
    pub(crate) shard_index_cache_resident_bytes: usize,
}

impl ZarrScanMetrics {
    /// Record one actual remote GET. `encoded_bytes` is `None` for a missing
    /// object and `Some(0)` for a present empty object.
    pub(crate) fn record_remote_get(&mut self, kind: ReadKind, encoded_bytes: Option<usize>) {
        self.record_remote_request(kind);
        if let Some(encoded_bytes) = encoded_bytes {
            self.record_remote_response_bytes(kind, encoded_bytes);
        }
    }

    /// Record a remote request when it starts. Keeping this separate from the
    /// response bytes lets cancelled or prefetched-but-unconsumed work remain
    /// visible even when no response reaches the scan loop.
    pub(crate) fn record_remote_request(&mut self, kind: ReadKind) {
        match kind {
            ReadKind::Metadata => {
                saturating_increment(&mut self.metadata_get_calls);
            }
            ReadKind::Coordinate => {
                saturating_increment(&mut self.coordinate_get_calls);
            }
            ReadKind::Data => {
                saturating_increment(&mut self.data_get_calls);
            }
        }
    }

    /// Record bytes received from a completed remote request without changing
    /// its request count.
    pub(crate) fn record_remote_response_bytes(&mut self, kind: ReadKind, encoded_bytes: usize) {
        let bytes = usize_to_u64(encoded_bytes);
        match kind {
            ReadKind::Metadata => saturating_add(&mut self.metadata_encoded_bytes, bytes),
            ReadKind::Coordinate => saturating_add(&mut self.coordinate_encoded_bytes, bytes),
            ReadKind::Data => saturating_add(&mut self.data_encoded_bytes, bytes),
        }
    }

    /// Set the current candidate-chunk selection. Selection is a property of
    /// the current scan bounds rather than cumulative work across rescans.
    pub(crate) fn set_chunk_selection(&mut self, total: u64, selected: u64) {
        self.chunks_total = total;
        self.chunks_selected = selected;
        self.chunks_coordinate_pruned = total.saturating_sub(selected);
    }

    /// Record one data-chunk request when it enters the ordered window.
    pub(crate) fn record_chunk_request(&mut self) {
        saturating_increment(&mut self.chunks_requested);
    }

    /// Record the outcome of one consumed data-chunk request.
    pub(crate) fn record_chunk_result(&mut self, present: bool) {
        if present {
            saturating_increment(&mut self.chunks_present);
        } else {
            saturating_increment(&mut self.chunks_missing);
        }
    }

    pub(crate) fn record_cache_lookup(&mut self, hit: bool) {
        if hit {
            saturating_increment(&mut self.cache_hits);
        } else {
            saturating_increment(&mut self.cache_misses);
        }
    }

    pub(crate) fn record_cache_evictions(&mut self, count: usize) {
        saturating_add(&mut self.cache_evictions, usize_to_u64(count));
    }

    pub(crate) fn record_shard_index_get(&mut self, encoded_bytes: Option<usize>) {
        saturating_increment(&mut self.shard_index_get_calls);
        if let Some(encoded_bytes) = encoded_bytes {
            saturating_add(
                &mut self.shard_index_encoded_bytes,
                usize_to_u64(encoded_bytes),
            );
        }
    }

    pub(crate) fn record_shard_payload_get(&mut self, encoded_bytes: Option<usize>) {
        saturating_increment(&mut self.shard_payload_get_calls);
        if let Some(encoded_bytes) = encoded_bytes {
            saturating_add(
                &mut self.shard_payload_encoded_bytes,
                usize_to_u64(encoded_bytes),
            );
        }
    }

    pub(crate) fn record_shard_index_cache_lookup(&mut self, hit: bool) {
        if hit {
            saturating_increment(&mut self.shard_index_cache_hits);
        } else {
            saturating_increment(&mut self.shard_index_cache_misses);
        }
    }

    pub(crate) fn record_shard_index_cache_evictions(&mut self, count: usize) {
        saturating_add(&mut self.shard_index_cache_evictions, usize_to_u64(count));
    }

    pub(crate) fn record_decoded_bytes(
        &mut self,
        kind: ReadKind,
        decoded_bytes: usize,
        synthesized_fill: bool,
    ) {
        let decoded_bytes = usize_to_u64(decoded_bytes);
        if synthesized_fill {
            saturating_add(&mut self.fill_bytes_synthesized, decoded_bytes);
        }
        match kind {
            ReadKind::Metadata => {}
            ReadKind::Coordinate => {
                saturating_add(&mut self.coordinate_decoded_bytes, decoded_bytes);
            }
            ReadKind::Data => {
                saturating_add(&mut self.data_decoded_bytes, decoded_bytes);
            }
        }
    }

    pub(crate) fn record_cells(&mut self, examined: usize, matched: Option<usize>) {
        saturating_add(&mut self.logical_cells_examined, usize_to_u64(examined));
        if let Some(matched) = matched {
            let total = self.logical_cells_matched.get_or_insert(0);
            saturating_add(total, usize_to_u64(matched));
        }
    }

    pub(crate) fn record_tuple_emitted(&mut self) {
        saturating_increment(&mut self.tuples_emitted);
    }

    pub(crate) fn record_rescan(&mut self) {
        saturating_increment(&mut self.rescans);
    }

    pub(crate) fn record_decompression_time(&mut self, elapsed: Duration) {
        saturating_add(&mut self.decompression_micros, duration_micros(elapsed));
    }

    pub(crate) fn record_decoding_time(&mut self, elapsed: Duration) {
        saturating_add(&mut self.decoding_micros, duration_micros(elapsed));
    }

    pub(crate) fn record_aggregate_time(&mut self, elapsed: Duration) {
        saturating_add(&mut self.aggregate_micros, duration_micros(elapsed));
    }

    pub(crate) fn total_get_calls(&self) -> u64 {
        self.metadata_get_calls
            .saturating_add(self.coordinate_get_calls)
            .saturating_add(self.data_get_calls)
    }

    pub(crate) fn total_encoded_bytes(&self) -> u64 {
        self.metadata_encoded_bytes
            .saturating_add(self.coordinate_encoded_bytes)
            .saturating_add(self.data_encoded_bytes)
    }

    pub(crate) fn total_decoded_bytes(&self) -> u64 {
        self.coordinate_decoded_bytes
            .saturating_add(self.data_decoded_bytes)
    }

    /// Build structured properties for the shared framework EXPLAIN hook.
    pub(crate) fn explain_properties(
        &self,
        context: ZarrExplainContext<'_>,
    ) -> Vec<ExplainProperty> {
        let mut properties = vec![
            ExplainProperty::text("Zarr Array", context.array),
            ExplainProperty::text(
                "Zarr Dimensions",
                format!("[{}]", context.dimensions.join(", ")),
            ),
            ExplainProperty::text("Zarr Shape", format!("{:?}", context.shape)),
            ExplainProperty::text("Zarr Chunk Shape", format!("{:?}", context.chunk_shape)),
            ExplainProperty::text("Zarr Dtype", context.dtype),
            ExplainProperty::text("Zarr Codec", context.codec),
            ExplainProperty::text("Zarr Storage Layout", context.storage_layout),
            ExplainProperty::text("Zarr Aggregate Pushdown", context.aggregate_mode),
            ExplainProperty::text("Zarr Chunk-Stat Pruning", "disabled"),
            ExplainProperty::unsigned(
                "Zarr Max Concurrent Reads",
                usize_to_u64(context.max_concurrent_reads),
            ),
            ExplainProperty::unsigned_with_unit(
                "Zarr Max Inflight Bytes",
                usize_to_u64(context.max_inflight_bytes),
                "bytes",
            ),
            ExplainProperty::unsigned_with_unit(
                "Zarr Compressed Cache Capacity",
                usize_to_u64(context.compressed_cache_bytes),
                "bytes",
            ),
            ExplainProperty::unsigned(
                "Zarr Compressed Cache Entries",
                usize_to_u64(context.cache_entries),
            ),
            ExplainProperty::unsigned_with_unit(
                "Zarr Compressed Cache Resident",
                usize_to_u64(context.cache_resident_bytes),
                "bytes",
            ),
            ExplainProperty::unsigned_with_unit(
                "Zarr Shard Index Cache Capacity",
                usize_to_u64(context.shard_index_cache_bytes),
                "bytes",
            ),
            ExplainProperty::unsigned(
                "Zarr Shard Index Cache Entries",
                usize_to_u64(context.shard_index_cache_entries),
            ),
            ExplainProperty::unsigned_with_unit(
                "Zarr Shard Index Cache Resident",
                usize_to_u64(context.shard_index_cache_resident_bytes),
                "bytes",
            ),
            ExplainProperty::unsigned("Zarr Chunks Total", self.chunks_total),
            ExplainProperty::unsigned("Zarr Chunks Selected", self.chunks_selected),
            ExplainProperty::unsigned(
                "Zarr Chunks Coordinate-Pruned",
                self.chunks_coordinate_pruned,
            ),
            ExplainProperty::unsigned("Zarr Chunks Requested", self.chunks_requested),
            ExplainProperty::unsigned("Zarr Chunks Present", self.chunks_present),
            ExplainProperty::unsigned("Zarr Chunks Missing", self.chunks_missing),
            ExplainProperty::unsigned("Zarr Remote GET Calls", self.total_get_calls()),
            ExplainProperty::unsigned("Zarr Metadata GET Calls", self.metadata_get_calls),
            ExplainProperty::unsigned("Zarr Coordinate GET Calls", self.coordinate_get_calls),
            ExplainProperty::unsigned("Zarr Data GET Calls", self.data_get_calls),
            ExplainProperty::unsigned("Zarr Cache Hits", self.cache_hits),
            ExplainProperty::unsigned("Zarr Cache Misses", self.cache_misses),
            ExplainProperty::unsigned("Zarr Cache Evictions", self.cache_evictions),
            ExplainProperty::unsigned("Zarr Shard Index GET Calls", self.shard_index_get_calls),
            ExplainProperty::unsigned("Zarr Shard Payload GET Calls", self.shard_payload_get_calls),
            ExplainProperty::unsigned("Zarr Shard Index Cache Hits", self.shard_index_cache_hits),
            ExplainProperty::unsigned(
                "Zarr Shard Index Cache Misses",
                self.shard_index_cache_misses,
            ),
            ExplainProperty::unsigned(
                "Zarr Shard Index Cache Evictions",
                self.shard_index_cache_evictions,
            ),
            ExplainProperty::unsigned_with_unit(
                "Zarr Remote Encoded Bytes",
                self.total_encoded_bytes(),
                "bytes",
            ),
            ExplainProperty::unsigned_with_unit(
                "Zarr Metadata Encoded Bytes",
                self.metadata_encoded_bytes,
                "bytes",
            ),
            ExplainProperty::unsigned_with_unit(
                "Zarr Coordinate Encoded Bytes",
                self.coordinate_encoded_bytes,
                "bytes",
            ),
            ExplainProperty::unsigned_with_unit(
                "Zarr Data Encoded Bytes",
                self.data_encoded_bytes,
                "bytes",
            ),
            ExplainProperty::unsigned_with_unit(
                "Zarr Shard Index Encoded Bytes",
                self.shard_index_encoded_bytes,
                "bytes",
            ),
            ExplainProperty::unsigned_with_unit(
                "Zarr Shard Payload Encoded Bytes",
                self.shard_payload_encoded_bytes,
                "bytes",
            ),
            ExplainProperty::unsigned_with_unit(
                "Zarr Decoded Bytes",
                self.total_decoded_bytes(),
                "bytes",
            ),
            ExplainProperty::unsigned_with_unit(
                "Zarr Coordinate Decoded Bytes",
                self.coordinate_decoded_bytes,
                "bytes",
            ),
            ExplainProperty::unsigned_with_unit(
                "Zarr Data Decoded Bytes",
                self.data_decoded_bytes,
                "bytes",
            ),
            ExplainProperty::unsigned_with_unit(
                "Zarr Fill Bytes Synthesized",
                self.fill_bytes_synthesized,
                "bytes",
            ),
            ExplainProperty::unsigned("Zarr Logical Cells Examined", self.logical_cells_examined),
            ExplainProperty::unsigned("Zarr Tuples Emitted", self.tuples_emitted),
            ExplainProperty::unsigned("Zarr Rescans", self.rescans),
            ExplainProperty::unsigned_with_unit(
                "Zarr Decompression Time",
                self.decompression_micros,
                "us",
            ),
            ExplainProperty::unsigned_with_unit("Zarr Decoding Time", self.decoding_micros, "us"),
            ExplainProperty::unsigned_with_unit("Zarr Aggregate Time", self.aggregate_micros, "us"),
        ];

        if let Some(shard_shape) = context.shard_shape {
            properties.push(ExplainProperty::text(
                "Zarr Shard Shape",
                format!("{shard_shape:?}"),
            ));
        }
        if let Some(index_location) = context.index_location {
            properties.push(ExplainProperty::text(
                "Zarr Shard Index Location",
                index_location,
            ));
        }

        if let Some(matched) = self.logical_cells_matched {
            properties.push(ExplainProperty::unsigned(
                "Zarr Logical Cells Matched",
                matched,
            ));
        }

        properties
    }
}

fn saturating_increment(value: &mut u64) {
    *value = value.saturating_add(1);
}

fn saturating_add(value: &mut u64, increment: u64) {
    *value = value.saturating_add(increment);
}

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn duration_micros(elapsed: Duration) -> u64 {
    u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use supabase_wrappers::prelude::ExplainValue;

    #[test]
    fn records_remote_cache_fill_and_cell_work_without_double_counting_bytes() {
        let mut metrics = ZarrScanMetrics::default();
        metrics.set_chunk_selection(4, 4);
        metrics.record_remote_get(ReadKind::Metadata, Some(1_127));
        metrics.record_remote_get(ReadKind::Coordinate, Some(64));

        for _ in 0..3 {
            metrics.record_chunk_request();
            metrics.record_cache_lookup(false);
            metrics.record_remote_get(ReadKind::Data, Some(96));
            metrics.record_chunk_result(true);
            metrics.record_decoded_bytes(ReadKind::Data, 96, false);
        }
        metrics.record_chunk_request();
        metrics.record_cache_lookup(false);
        metrics.record_remote_get(ReadKind::Data, None);
        metrics.record_chunk_result(false);
        metrics.record_decoded_bytes(ReadKind::Data, 96, true);
        metrics.record_decoded_bytes(ReadKind::Coordinate, 64, true);
        metrics.record_cells(40, Some(30));
        metrics.record_tuple_emitted();

        assert_eq!(metrics.total_get_calls(), 6);
        assert_eq!(metrics.total_encoded_bytes(), 1_479);
        assert_eq!(metrics.total_decoded_bytes(), 448);
        assert_eq!(metrics.chunks_requested, 4);
        assert_eq!(metrics.chunks_present, 3);
        assert_eq!(metrics.chunks_missing, 1);
        assert_eq!(metrics.fill_bytes_synthesized, 160);
        assert_eq!(metrics.logical_cells_matched, Some(30));
    }

    #[test]
    fn explain_properties_preserve_numeric_types_and_unknown_match_count() {
        let metrics = ZarrScanMetrics::default();
        let dimensions = vec!["time".to_string(), "y".to_string(), "x".to_string()];
        let shape = [2, 5, 6];
        let chunk_shape = [1, 5, 3];
        let properties = metrics.explain_properties(ZarrExplainContext {
            array: "cube/reflectance",
            dimensions: &dimensions,
            shape: &shape,
            chunk_shape: &chunk_shape,
            dtype: "<f4",
            codec: "blosc",
            storage_layout: "direct",
            shard_shape: None,
            index_location: None,
            aggregate_mode: "disabled",
            max_concurrent_reads: 4,
            max_inflight_bytes: 8 * 1024 * 1024,
            compressed_cache_bytes: 1024 * 1024,
            cache_entries: 0,
            cache_resident_bytes: 0,
            shard_index_cache_bytes: 0,
            shard_index_cache_entries: 0,
            shard_index_cache_resident_bytes: 0,
        });

        assert!(properties.iter().any(|property| {
            property.label == "Zarr Chunks Requested"
                && matches!(
                    &property.value,
                    ExplainValue::Unsigned {
                        value: 0,
                        unit: None
                    }
                )
        }));
        assert!(
            !properties
                .iter()
                .any(|property| property.label == "Zarr Logical Cells Matched")
        );
    }
}
