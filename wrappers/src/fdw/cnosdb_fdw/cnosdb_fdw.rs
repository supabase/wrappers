use crate::fdw::cnosdb_fdw::CnosdbFdwError;
use crate::stats;
use arrow::array::{Array, ArrayRef};
use arrow::buffer::Buffer;
use arrow::datatypes::{DataType, Schema, TimeUnit};
use arrow::ipc;
use arrow::ipc::{reader, root_as_message};
use arrow::record_batch::RecordBatch;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::sql::Any;
use arrow_flight::sql::CommandStatementQuery;
use arrow_flight::utils::flight_data_to_arrow_batch;
use arrow_flight::{FlightDescriptor, HandshakeRequest, IpcMessage};
use futures::StreamExt;
use pgrx::{warning, Timestamp};
use prost::Message;
use std::collections::HashMap;
use std::iter;
use std::sync::Arc;
use supabase_wrappers::interface::{Cell, Column, ForeignDataWrapper, Limit, Qual, Row, Sort};
use supabase_wrappers::prelude;
use supabase_wrappers::wrappers_fdw;
use tokio::runtime::Runtime;
use tonic::codegen::http::header::AUTHORIZATION;
use tonic::metadata::AsciiMetadataValue;
use tonic::transport::Channel;
use tonic::Request;

const DEFAULT_URL: &str = "https://127.0.0.1:8904";
const DEFAULT_USER: &str = "root";
const DEFAULT_PASSWORD: &str = "";
const DEFAULT_TENANT: &str = "cnosdb";
const DEFAULT_DATABASE: &str = "public";

const MICRO_30YEAR: i64 = 946_684_800_000_000;

fn field_to_cell(column: &ArrayRef, i: usize) -> Result<Option<Cell>, CnosdbFdwError> {
    return match column.data_type() {
        DataType::Boolean => {
            let array = column
                .as_any()
                .downcast_ref::<arrow::array::BooleanArray>()
                .ok_or(CnosdbFdwError::ConvertColumnError(format!(
                    "{:?}",
                    column.data_type()
                )))?;
            if array.is_null(i) {
                return Ok(None);
            }
            return Ok(Some(Cell::Bool(array.value(i))));
        }
        DataType::Int64 => {
            let array = column
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .ok_or(CnosdbFdwError::ConvertColumnError(format!(
                    "{:?}",
                    column.data_type()
                )))?;
            if array.is_null(i) {
                return Ok(None);
            }
            Ok(Some(Cell::I64(array.value(i))))
        }
        DataType::UInt64 => {
            let array = column
                .as_any()
                .downcast_ref::<arrow::array::UInt64Array>()
                .ok_or(CnosdbFdwError::ConvertColumnError(format!(
                    "{:?}",
                    column.data_type()
                )))?;
            if array.is_null(i) {
                return Ok(None);
            }
            Ok(Some(Cell::I64(array.value(i) as i64)))
        }
        DataType::Float64 => {
            let array = column
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .ok_or(CnosdbFdwError::ConvertColumnError(format!(
                    "{:?}",
                    column.data_type()
                )))?;
            if array.is_null(i) {
                return Ok(None);
            }
            Ok(Some(Cell::F64(array.value(i))))
        }
        DataType::Utf8 => {
            let array = column
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .ok_or(CnosdbFdwError::ConvertColumnError(format!(
                    "{:?}",
                    column.data_type()
                )))?;
            if array.is_null(i) {
                return Ok(None);
            }
            Ok(Some(Cell::String(array.value(i).to_string())))
        }
        DataType::Timestamp(unit, _) => match unit {
            TimeUnit::Second => {
                let array = column
                    .as_any()
                    .downcast_ref::<arrow::array::TimestampSecondArray>()
                    .ok_or(CnosdbFdwError::ConvertColumnError(format!(
                        "{:?}",
                        column.data_type()
                    )))?;
                if array.is_null(i) {
                    return Ok(None);
                }
                Ok(Some(Cell::Timestamp(Timestamp::from(
                    array.value(i) * 1_000_000 - MICRO_30YEAR,
                ))))
            }
            TimeUnit::Millisecond => {
                let array = column
                    .as_any()
                    .downcast_ref::<arrow::array::TimestampMillisecondArray>()
                    .ok_or(CnosdbFdwError::ConvertColumnError(format!(
                        "{:?}",
                        column.data_type()
                    )))?;
                if array.is_null(i) {
                    return Ok(None);
                }
                Ok(Some(Cell::Timestamp(Timestamp::from(
                    array.value(i) * 1_000 - MICRO_30YEAR,
                ))))
            }
            TimeUnit::Microsecond => {
                let array = column
                    .as_any()
                    .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
                    .ok_or(CnosdbFdwError::ConvertColumnError(format!(
                        "{:?}",
                        column.data_type()
                    )))?;
                if array.is_null(i) {
                    return Ok(None);
                }
                Ok(Some(Cell::Timestamp(Timestamp::from(
                    array.value(i) - MICRO_30YEAR,
                ))))
            }
            TimeUnit::Nanosecond => {
                let array = column
                    .as_any()
                    .downcast_ref::<arrow::array::TimestampNanosecondArray>()
                    .ok_or(CnosdbFdwError::ConvertColumnError(format!(
                        "{:?}",
                        column.data_type()
                    )))?;
                if array.is_null(i) {
                    return Ok(None);
                }
                Ok(Some(Cell::Timestamp(Timestamp::from(
                    array.value(i) / 1_000 - MICRO_30YEAR,
                ))))
            }
        },
        _ => Err(CnosdbFdwError::UnsupportedColumnTypeError(format!(
            "{:?}",
            column.data_type()
        ))),
    };
}

#[wrappers_fdw(
    version = "0.1.0",
    author = "Subsegment",
    website = "https://github.com/supabase/wrappers/tree/main/wrappers/src/fdw/cnosdb_fdw",
    error_type = "CnosdbFdwError"
)]
pub(crate) struct CnosdbFdw {
    rt: Runtime,
    client: FlightServiceClient<Channel>,
    user: String,
    password: String,
    tenant: String,
    db: String,

    table: String,
    tgt_cols: Vec<Column>,
    data: Vec<RecordBatch>,
    num_rows: usize,
    batch_idx: usize,
}

impl CnosdbFdw {
    const FDW_NAME: &str = "CnosdbFdw";
    fn deparse(
        &self,
        quals: &[Qual],
        columns: &[Column],
        sorts: &[Sort],
        limit: &Option<Limit>,
    ) -> Result<String, CnosdbFdwError> {
        let mut sql = String::new();

        sql.push_str("SELECT ");

        if columns.is_empty() {
            sql.push_str("*");
        } else {
            sql.push_str(
                &columns
                    .iter()
                    .map(|c| c.name.as_str())
                    .collect::<Vec<&str>>()
                    .join(", "),
            );
        }

        sql.push_str(" FROM ");
        sql.push_str(&self.table);

        if !quals.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(
                &quals
                    .iter()
                    .map(|q| q.deparse())
                    .collect::<Vec<String>>()
                    .join(" AND "),
            );
        }

        if !sorts.is_empty() {
            sql.push_str(" ORDER BY ");
            sql.push_str(
                &sorts
                    .iter()
                    .map(|s| s.deparse())
                    .collect::<Vec<String>>()
                    .join(", "),
            );
        }

        if let Some(limit) = limit {
            sql.push_str(" LIMIT ");
            sql.push_str(&limit.deparse());
        }

        Ok(sql)
    }
}

impl ForeignDataWrapper<CnosdbFdwError> for CnosdbFdw {
    fn new(options: &HashMap<String, String>) -> Result<Self, CnosdbFdwError>
    where
        Self: Sized,
    {
        let rt = Runtime::new().unwrap();
        let url = options
            .get("url")
            .cloned()
            .unwrap_or(DEFAULT_URL.to_string());

        let user = options
            .get("user")
            .cloned()
            .unwrap_or(DEFAULT_USER.to_string());

        let password = options
            .get("password")
            .cloned()
            .unwrap_or(DEFAULT_PASSWORD.to_string());

        let tenant = options
            .get("tenant")
            .cloned()
            .unwrap_or(DEFAULT_TENANT.to_string());

        let db = options
            .get("db")
            .cloned()
            .unwrap_or(DEFAULT_DATABASE.to_string());

        let mut client = rt
            .block_on(FlightServiceClient::connect(url))
            .map_err(|e| CnosdbFdwError::ConnectCnosdbError(e.to_string()))?;

        let mut hands_shake = Request::new(futures::stream::iter(iter::once(
            HandshakeRequest::default(),
        )));

        hands_shake.metadata_mut().insert(
            AUTHORIZATION.as_str(),
            AsciiMetadataValue::try_from(format!(
                "Basic {}",
                base64::encode(format!("{}:{}", user, password)),
            ))
            .map_err(|e| CnosdbFdwError::RequestMetaDataError(e.to_string()))?,
        );

        rt.block_on(client.handshake(hands_shake))
            .map_err(|e| CnosdbFdwError::SendRequestError(e.to_string()))?;

        Ok(Self {
            rt,
            client,
            user,
            password,
            tenant,
            db,
            table: String::default(),
            tgt_cols: Vec::default(),
            data: Vec::default(),
            num_rows: 0,
            batch_idx: 0,
        })
    }

    fn begin_scan(
        &mut self,
        quals: &[Qual],
        columns: &[Column],
        sorts: &[Sort],
        limit: &Option<Limit>,
        options: &HashMap<String, String>,
    ) -> Result<(), CnosdbFdwError> {
        self.table = prelude::require_option("table", options)?.to_string();
        self.tgt_cols = columns.to_vec();

        let sql = self.deparse(quals, columns, sorts, limit)?;
        let cmd = CommandStatementQuery {
            query: sql,
            transaction_id: None,
        };

        let pack = Any::pack(&cmd).expect("pack");
        let fd = FlightDescriptor::new_cmd(pack.encode_to_vec());

        let mut req = Request::new(fd);
        req.metadata_mut().insert(
            AUTHORIZATION.as_str(),
            AsciiMetadataValue::try_from(format!(
                "Basic {}",
                base64::encode(format!("{}:{}", self.user, self.password)),
            ))
            .map_err(|e| CnosdbFdwError::RequestMetaDataError(e.to_string()))?,
        );
        req.metadata_mut().insert(
            "tenant",
            AsciiMetadataValue::try_from(self.tenant.clone())
                .map_err(|e| CnosdbFdwError::RequestMetaDataError(e.to_string()))?,
        );
        req.metadata_mut().insert(
            "db",
            AsciiMetadataValue::try_from(self.db.clone())
                .map_err(|e| CnosdbFdwError::RequestMetaDataError(e.to_string()))?,
        );

        let flight_info = self
            .rt
            .block_on(self.client.get_flight_info(req))
            .map_err(|e| CnosdbFdwError::SendRequestError(e.to_string()))?
            .into_inner();
        let mut all_rows = 0;
        let mut all_bytes = 0;
        let schema_ref = Arc::new(
            Schema::try_from(IpcMessage(flight_info.schema))
                .map_err(|e| CnosdbFdwError::BuildSchemaError(e.to_string()))?,
        );
        for endpoint in flight_info.endpoint {
            if let Some(ticket) = endpoint.ticket {
                let resp = self
                    .rt
                    .block_on(self.client.do_get(ticket))
                    .map_err(|e| CnosdbFdwError::SendRequestError(e.to_string()))?;
                let mut stream = resp.into_inner();
                let mut dictionaries_by_id = HashMap::new();
                while let Some(Ok(flight_data)) = self.rt.block_on(stream.next()) {
                    let message = root_as_message(&flight_data.data_header[..])
                        .map_err(|e| CnosdbFdwError::DecodeFlightDataError(e.to_string()))?;
                    match message.header_type() {
                        ipc::MessageHeader::RecordBatch => {
                            let record_batch = flight_data_to_arrow_batch(
                                &flight_data,
                                schema_ref.clone(),
                                &dictionaries_by_id,
                            )
                            .map_err(|e| CnosdbFdwError::DecodeFlightDataError(e.to_string()))?;
                            all_rows += record_batch.num_rows();
                            all_bytes += record_batch.get_array_memory_size();
                            self.data.push(record_batch);
                        }
                        ipc::MessageHeader::DictionaryBatch => {
                            let ipc_batch = message.header_as_dictionary_batch().unwrap();

                            reader::read_dictionary(
                                &Buffer::from(flight_data.data_body),
                                ipc_batch,
                                &schema_ref,
                                &mut dictionaries_by_id,
                                &message.version(),
                            )
                            .map_err(|e| CnosdbFdwError::DecodeFlightDataError(e.to_string()))?;
                        }
                        ipc::MessageHeader::Schema => {
                            continue;
                        }
                        _ => {
                            warning!("Reading types other than record batches not yet supported");
                        }
                    }
                }
            }
        }

        stats::inc_stats(Self::FDW_NAME, stats::Metric::RowsIn, all_rows as i64);
        stats::inc_stats(Self::FDW_NAME, stats::Metric::RowsOut, all_rows as i64);
        stats::inc_stats(Self::FDW_NAME, stats::Metric::BytesIn, all_bytes as i64);
        Ok(())
    }

    fn iter_scan(&mut self, row: &mut Row) -> Result<Option<()>, CnosdbFdwError> {
        if self.batch_idx < self.data.len() {
            let batch = &self.data[self.batch_idx];
            for tgt_col in &self.tgt_cols {
                let column = batch
                    .column_by_name(tgt_col.name.as_str())
                    .ok_or(CnosdbFdwError::ColumnMissingError(tgt_col.name.clone()))?;
                let cell = field_to_cell(column, self.num_rows)?;
                row.push(tgt_col.name.as_str(), cell);
            }
            self.num_rows += 1;
            if self.num_rows == batch.num_rows() {
                self.num_rows = 0;
                self.batch_idx += 1;
            }
            return Ok(Some(()));
        }
        Ok(None)
    }

    fn end_scan(&mut self) -> Result<(), CnosdbFdwError> {
        self.num_rows = 0;
        self.batch_idx = 0;
        self.data.clear();
        Ok(())
    }
}
