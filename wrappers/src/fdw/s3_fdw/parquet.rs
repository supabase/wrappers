use crate::stats;
use arrow_array::{Array, RecordBatch, array};
use aws_sdk_s3 as s3;
use bytes::Bytes;
use chrono::NaiveDate;
use futures::{FutureExt, TryStreamExt, future::BoxFuture};
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::async_reader::{
    AsyncFileReader, ParquetRecordBatchStream, ParquetRecordBatchStreamBuilder,
};
use parquet::errors::{ParquetError, Result as ParquetResult};
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader};
use pgrx::datum::datetime_support::to_timestamp;
use pgrx::pg_sys;
use pgrx::prelude::Date;
use std::io::Cursor;
use std::ops::Range;
use std::sync::Arc;

use supabase_wrappers::prelude::*;

use super::{S3FdwError, S3FdwResult};

// convert an error to parquet error
#[inline]
fn to_parquet_error(err: impl std::error::Error + Send + Sync + 'static) -> ParquetError {
    ParquetError::External(Box::new(err))
}

// async reader for a single S3 parquet file, fetching byte ranges on demand
pub(super) struct S3ParquetReader {
    client: s3::Client,
    bucket: String,
    object: String,
}

impl S3ParquetReader {
    fn new(client: &s3::Client, bucket: &str, object: &str) -> Self {
        S3ParquetReader {
            client: client.clone(),
            bucket: bucket.to_owned(),
            object: object.to_owned(),
        }
    }

    async fn get_object_size(&self) -> ParquetResult<u64> {
        let output = self
            .client
            .get_object_attributes()
            .bucket(&self.bucket)
            .key(&self.object)
            .object_attributes(s3::types::ObjectAttributes::ObjectSize)
            .send()
            .await
            .map_err(to_parquet_error)?;
        output
            .object_size()
            .map(|size| size as u64)
            .ok_or_else(|| ParquetError::General("object has no size attribute".to_string()))
    }
}

impl AsyncFileReader for S3ParquetReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, ParquetResult<Bytes>> {
        async move {
            if range.is_empty() {
                return Ok(Bytes::new());
            }
            let output = self
                .client
                .get_object()
                .bucket(&self.bucket)
                .key(&self.object)
                .range(format!("bytes={}-{}", range.start, range.end - 1))
                .send()
                .await
                .map_err(to_parquet_error)?;
            let bytes = output
                .body
                .collect()
                .await
                .map_err(to_parquet_error)?
                .into_bytes();
            if bytes.len() as u64 != range.end - range.start {
                return Err(ParquetError::EOF(format!(
                    "expected to read {} bytes, got {}",
                    range.end - range.start,
                    bytes.len()
                )));
            }
            Ok(bytes)
        }
        .boxed()
    }

    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, ParquetResult<Arc<ParquetMetaData>>> {
        async move {
            let object_size = self.get_object_size().await?;
            let metadata = ParquetMetaDataReader::new()
                .with_page_index_policy(PageIndexPolicy::from(
                    options.is_some_and(|o| o.page_index()),
                ))
                .with_metadata_options(options.map(|o| o.metadata_options().clone()))
                .load_and_finish(self, object_size)
                .await?;
            Ok(Arc::new(metadata))
        }
        .boxed()
    }
}

// S3 parquet file read manager
#[derive(Default)]
pub(super) struct S3Parquet {
    // parquet record batch stream reader
    stream: Option<ParquetRecordBatchStream<Box<dyn AsyncFileReader>>>,

    // a record batch
    batch: Option<RecordBatch>,
    batch_idx: usize,
}

impl S3Parquet {
    const FDW_NAME: &'static str = "S3Fdw";

    // open batch stream from local buffer
    pub(super) async fn open_local_stream(&mut self, buf: Vec<u8>) -> S3FdwResult<()> {
        let cursor: Box<dyn AsyncFileReader> = Box::new(Cursor::new(buf));
        let builder = ParquetRecordBatchStreamBuilder::new(cursor).await?;
        let stream = builder.build()?;
        self.stream = Some(stream);
        Ok(())
    }

    // open async record batch stream which reads the file from S3 on demand
    pub(super) async fn open_async_stream(
        &mut self,
        client: &s3::Client,
        bucket: &str,
        object: &str,
        tgt_cols: &[Column],
    ) -> S3FdwResult<()> {
        let rdr: Box<dyn AsyncFileReader> = Box::new(S3ParquetReader::new(client, bucket, object));
        let builder = ParquetRecordBatchStreamBuilder::new(rdr).await?;

        // get parquet file metadata
        let file_metadata = builder.metadata().file_metadata();
        let schema = file_metadata.schema_descr();
        let cols = schema.columns();

        // find target column indexes in parquest columns
        let project_indexes = tgt_cols
            .iter()
            .map(|tgt_col| {
                cols.iter()
                    .position(|col| col.name() == tgt_col.name)
                    .unwrap_or_else(|| {
                        panic!("column '{}' not found in parquet file", tgt_col.name)
                    })
            })
            .collect::<Vec<usize>>();

        // set up projections for the builder
        let mask = ProjectionMask::roots(schema, project_indexes);
        let stream = builder.with_projection(mask).build()?;

        self.stream = Some(stream);
        self.batch = None;
        self.batch_idx = 0;

        Ok(())
    }

    // refill record batch
    pub(super) async fn refill(&mut self) -> S3FdwResult<Option<()>> {
        // if there are still records in the batch
        if let Some(batch) = &self.batch
            && self.batch_idx < batch.num_rows()
        {
            return Ok(Some(()));
        }

        // otherwise, read one moe batch
        if let Some(stream) = &mut self.stream {
            let result = stream.try_next().await?;
            return Ok(result.map(|batch| {
                stats::inc_stats(
                    Self::FDW_NAME,
                    stats::Metric::RowsIn,
                    batch.num_rows() as i64,
                );
                stats::inc_stats(
                    Self::FDW_NAME,
                    stats::Metric::BytesIn,
                    batch.get_array_memory_size() as i64,
                );

                self.batch = Some(batch);
                self.batch_idx = 0;
            }));
        }

        Ok(None)
    }

    // read one row from record batch
    pub(super) fn read_into_row(
        &mut self,
        row: &mut Row,
        tgt_cols: &Vec<Column>,
    ) -> S3FdwResult<Option<()>> {
        if let Some(batch) = &self.batch {
            for tgt_col in tgt_cols {
                let col = batch
                    .column_by_name(&tgt_col.name)
                    .ok_or(S3FdwError::ColumnNotFound(tgt_col.name.clone()))?;

                macro_rules! col_to_cell {
                    ($array_type:ident, $cell_type:ident) => {{
                        let arr = col
                            .as_any()
                            .downcast_ref::<array::$array_type>()
                            .ok_or(S3FdwError::ColumnTypeNotMatch(tgt_col.name.clone()))?;
                        if arr.is_null(self.batch_idx) {
                            None
                        } else {
                            Some(Cell::$cell_type(arr.value(self.batch_idx)))
                        }
                    }};
                }

                let cell = match tgt_col.type_oid {
                    pg_sys::BOOLOID => col_to_cell!(BooleanArray, Bool),
                    pg_sys::CHAROID => col_to_cell!(Int8Array, I8),
                    pg_sys::INT2OID => col_to_cell!(Int16Array, I16),
                    pg_sys::FLOAT4OID => col_to_cell!(Float32Array, F32),
                    pg_sys::INT4OID => col_to_cell!(Int32Array, I32),
                    pg_sys::FLOAT8OID => col_to_cell!(Float64Array, F64),
                    pg_sys::INT8OID => col_to_cell!(Int64Array, I64),
                    pg_sys::NUMERICOID => {
                        let arr = col
                            .as_any()
                            .downcast_ref::<array::Float64Array>()
                            .ok_or(S3FdwError::ColumnTypeNotMatch(tgt_col.name.clone()))?;
                        if arr.is_null(self.batch_idx) {
                            None
                        } else {
                            let value = arr.value(self.batch_idx);
                            let num = pgrx::AnyNumeric::try_from(value)?;
                            Some(Cell::Numeric(num))
                        }
                    }
                    pg_sys::TEXTOID => {
                        // 'text' type can be converted from StringArray, StringViewArray and
                        // generic BinaryArray
                        if col.is_null(self.batch_idx) {
                            None
                        } else if let Some(arr) = col.as_any().downcast_ref::<array::StringArray>()
                        {
                            Some(Cell::String(arr.value(self.batch_idx).to_string()))
                        } else if let Some(arr) =
                            col.as_any().downcast_ref::<array::StringViewArray>()
                        {
                            Some(Cell::String(arr.value(self.batch_idx).to_string()))
                        } else if let Some(arr) = col.as_any().downcast_ref::<array::BinaryArray>()
                        {
                            let s = String::from_utf8_lossy(arr.value(self.batch_idx));
                            Some(Cell::String(s.to_string()))
                        } else {
                            return Err(S3FdwError::ColumnTypeNotMatch(tgt_col.name.clone()));
                        }
                    }
                    pg_sys::DATEOID => {
                        let arr = col
                            .as_any()
                            .downcast_ref::<array::Date64Array>()
                            .ok_or(S3FdwError::ColumnTypeNotMatch(tgt_col.name.clone()))?;
                        if arr.is_null(self.batch_idx) {
                            None
                        } else {
                            arr.value_as_date(self.batch_idx).map(|dt| {
                                let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)
                                    .expect("1/1/1970 is a valid NaiveDate");
                                let seconds_from_epoch =
                                    dt.signed_duration_since(epoch).num_seconds();
                                let ts = to_timestamp(seconds_from_epoch as f64);
                                Cell::Date(Date::from(ts))
                            })
                        }
                    }
                    pg_sys::TIMESTAMPOID => {
                        let arr = col
                            .as_any()
                            .downcast_ref::<array::TimestampNanosecondArray>()
                            .ok_or(S3FdwError::ColumnTypeNotMatch(tgt_col.name.clone()))?;
                        if arr.is_null(self.batch_idx) {
                            None
                        } else {
                            arr.value_as_datetime(self.batch_idx).map(|ts| {
                                let ts = to_timestamp(ts.and_utc().timestamp() as f64);
                                Cell::Timestamp(ts.to_utc())
                            })
                        }
                    }
                    pg_sys::TIMESTAMPTZOID => {
                        let arr = col
                            .as_any()
                            .downcast_ref::<array::TimestampNanosecondArray>()
                            .ok_or(S3FdwError::ColumnTypeNotMatch(tgt_col.name.clone()))?;
                        if arr.is_null(self.batch_idx) {
                            None
                        } else {
                            arr.value_as_datetime(self.batch_idx).map(|ts| {
                                let ts = to_timestamp(ts.and_utc().timestamp() as f64);
                                Cell::Timestamptz(ts)
                            })
                        }
                    }
                    _ => return Err(S3FdwError::UnsupportedColumnType(tgt_col.name.clone())),
                };
                row.push(&tgt_col.name, cell);
            }
            self.batch_idx += 1;
            return Ok(Some(()));
        }
        Ok(None)
    }
}
