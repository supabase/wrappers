use crate::stats;
use arrow_array::{array, Array, RecordBatch};
use aws_sdk_s3 as s3;
use chrono::NaiveDate;
use futures::TryStreamExt;
use parquet::arrow::async_reader::{
    AsyncFileReader, ParquetRecordBatchStream, ParquetRecordBatchStreamBuilder,
};
use parquet::arrow::ProjectionMask;
use pgrx::datum::datetime_support::to_timestamp;
use pgrx::pg_sys;
use pgrx::prelude::Date;
use std::cmp::min;
use std::io::{Cursor, Error as IoError, ErrorKind, Result as IoResult, SeekFrom};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncSeek, ReadBuf};
use tokio::runtime::Handle;

use supabase_wrappers::prelude::*;

use super::{S3FdwError, S3FdwResult};

// convert an error to IO error
#[inline]
fn to_io_error(err: impl std::error::Error) -> IoError {
    IoError::new(ErrorKind::Other, err.to_string())
}

// async reader for a single S3 parquet file
pub(super) struct S3ParquetReader {
    client: s3::Client,
    bucket: String,
    object: String,
    object_size: Option<u64>,
    pos: u64,
}

impl S3ParquetReader {
    fn new(client: &s3::Client, bucket: &str, object: &str) -> Self {
        S3ParquetReader {
            client: client.clone(),
            bucket: bucket.to_owned(),
            object: object.to_owned(),
            object_size: None,
            pos: 0,
        }
    }

    fn get_object_size(&mut self) -> IoResult<u64> {
        if let Some(object_size) = self.object_size {
            return Ok(object_size);
        }

        // wait on current thread to get object metadata
        futures::executor::block_on(
            self.client
                .get_object_attributes()
                .bucket(&self.bucket)
                .key(&self.object)
                .object_attributes(s3::types::ObjectAttributes::ObjectSize)
                .send(),
        )
        .map_err(to_io_error)
        .and_then(|output| {
            let object_size = output.object_size().ok_or::<IoError>(IoError::new(
                ErrorKind::Other,
                "object has no size attribute",
            ))? as u64;
            self.object_size = Some(object_size);
            Ok(object_size)
        })
    }
}

impl AsyncRead for S3ParquetReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<IoResult<()>> {
        // ensure object size is already available
        let object_size = match self.get_object_size() {
            Ok(size) => size,
            Err(err) => {
                return Poll::Ready(Err(err));
            }
        };

        // calculate request range
        let object_remaining = object_size - self.pos;
        let remaining = min(buf.remaining() as u64, object_remaining);
        let range = format!("bytes={}-{}", self.pos, self.pos + remaining - 1);

        // wait on current thread to get object contents
        match futures::executor::block_on(
            self.client
                .get_object()
                .bucket(&self.bucket)
                .key(&self.object)
                .range(range)
                .send(),
        ) {
            Ok(output) => {
                let mut rdr = output.body.into_async_read();
                AsyncRead::poll_read(Pin::new(&mut rdr), cx, buf)
            }
            Err(err) => Poll::Ready(Err(to_io_error(err))),
        }
    }
}

impl AsyncSeek for S3ParquetReader {
    fn start_seek(mut self: Pin<&mut Self>, position: SeekFrom) -> IoResult<()> {
        let object_size = self.get_object_size()?;
        match position {
            SeekFrom::Start(pos) => {
                self.pos = pos;
            }
            SeekFrom::End(pos) => {
                self.pos = (object_size as i64 + pos) as u64;
            }
            SeekFrom::Current(pos) => {
                self.pos = (self.pos as i64 + pos) as u64;
            }
        }

        Ok(())
    }

    fn poll_complete(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<IoResult<u64>> {
        Poll::Ready(Ok(self.pos))
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

    // open async record batch stream
    //
    // This is done by spawning a thread to create builder and then get read stream
    // Note: this function should be called on a tokio runtime executor thread
    pub(super) async fn open_async_stream(
        &mut self,
        client: &s3::Client,
        bucket: &str,
        object: &str,
        tgt_cols: &[Column],
    ) -> S3FdwResult<()> {
        let handle = Handle::current();
        let rdr = S3ParquetReader::new(client, bucket, object);

        let task = handle
            .spawn_blocking(move || {
                // we need to create another thread and wait on it to create builder
                let handle = Handle::current();
                let task = handle.spawn_blocking(move || {
                    let boxed_rdr: Box<dyn AsyncFileReader> = Box::new(rdr);
                    Handle::current().block_on(ParquetRecordBatchStreamBuilder::new(boxed_rdr))
                });
                handle.block_on(task)
            })
            .await;

        let stream = task
            .expect("create parquet batch stream builder failed")
            .expect("create parquet batch stream builder failed")
            .and_then(|builder| {
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
                builder.with_projection(mask).build()
            })
            .map_err(|err| parquet::errors::ParquetError::General(err.to_string()))?;

        self.stream = Some(stream);
        self.batch = None;
        self.batch_idx = 0;

        Ok(())
    }

    // refill record batch
    pub(super) async fn refill(&mut self) -> S3FdwResult<Option<()>> {
        // if there are still records in the batch
        if let Some(batch) = &self.batch {
            if self.batch_idx < batch.num_rows() {
                return Ok(Some(()));
            }
        }

        // otherwise, read one moe batch
        if let Some(ref mut stream) = &mut self.stream {
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
                        let arr = col
                            .as_any()
                            .downcast_ref::<array::BinaryArray>()
                            .ok_or(S3FdwError::ColumnTypeNotMatch(tgt_col.name.clone()))?;
                        if arr.is_null(self.batch_idx) {
                            None
                        } else {
                            let s = String::from_utf8_lossy(arr.value(self.batch_idx));
                            Some(Cell::String(s.to_string()))
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
                                let ts = to_timestamp(ts.timestamp() as f64);
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
                                let ts = to_timestamp(ts.timestamp() as f64);
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
