use arrow_array::{array::ArrayRef, builder::ArrayBuilder, Array, RecordBatch};
use futures::StreamExt;
use iceberg::{
    expr::Predicate,
    scan::ArrowRecordBatchStream,
    spec::{DataFileFormat, NestedFieldRef, PrimitiveType, Type},
    table::Table,
    transaction::{ApplyTransactionAction, Transaction},
    writer::{
        base_writer::data_file_writer::DataFileWriterBuilder, file_writer::ParquetWriterBuilder,
        IcebergWriter, IcebergWriterBuilder,
    },
    Catalog, NamespaceIdent, TableIdent,
};
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};
use iceberg_catalog_s3tables::{S3TablesCatalog, S3TablesCatalogConfig};
use parquet::file::properties::WriterProperties;
use pgrx::pg_sys;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use supabase_wrappers::prelude::*;

use super::{
    mapper::Mapper,
    pushdown::try_pushdown,
    utils,
    writer::{FileNameGenerator, LocationGenerator},
    IcebergFdwError, IcebergFdwResult,
};
use crate::stats;

#[derive(Debug, Clone)]
struct InputRow {
    cells: Vec<Option<Cell>>,
}

#[wrappers_fdw(
    version = "0.1.3",
    author = "Supabase",
    website = "https://github.com/supabase/wrappers/tree/main/wrappers/src/fdw/iceberg_fdw",
    error_type = "IcebergFdwError"
)]
pub(crate) struct IcebergFdw {
    rt: Runtime,
    mapper: Mapper,
    catalog: Box<dyn Catalog>,
    table: Option<Table>,
    predicate: Option<Predicate>,
    batch_size: Option<usize>,

    // copy of target columns
    tgt_cols: Vec<Column>,

    // recrod batch stream
    stream: Option<ArrowRecordBatchStream>,

    // converted cells for the batch
    row_data: VecDeque<Vec<Option<Cell>>>,

    // cached source fields for the batch
    src_fields: Vec<NestedFieldRef>,

    // for stats: total number of records and bytes read
    num_rows: usize,
    bytes_in: usize,

    // for insertion: buffer for accumulating input rows before sorting
    input_rows: Vec<InputRow>,
}

impl IcebergFdw {
    const FDW_NAME: &'static str = "IcebergFdw";

    // fetch next record batch from Arrow record batch stream
    // and convert it local cached row data
    fn next_batch(&mut self) -> IcebergFdwResult<()> {
        if let Some(stream) = &mut self.stream {
            if let Some(result) = self.rt.block_on(stream.next()) {
                let batch = result?;
                self.record_batch_to_row_data(&batch)?;
                if batch.num_rows() > 0 {
                    self.num_rows += batch.num_rows();
                    self.bytes_in += batch.get_array_memory_size();
                }
            }

            self.mapper.reset();
        }
        Ok(())
    }

    // convert record batch to row data
    fn record_batch_to_row_data(&mut self, batch: &RecordBatch) -> IcebergFdwResult<()> {
        let mut cols: Vec<ArrayRef> = Vec::new();

        self.row_data = VecDeque::with_capacity(batch.num_rows());

        for tgt_col in &self.tgt_cols {
            let col_name = &tgt_col.name;
            let array = batch
                .column_by_name(col_name)
                .ok_or_else(|| IcebergFdwError::ColumnNotFound(col_name.into()))?;
            cols.push(array.clone());
        }

        for rec_offset in 0..batch.num_rows() {
            let mut cells = Vec::with_capacity(batch.num_columns());

            for (col_idx, tgt_col) in self.tgt_cols.iter().enumerate() {
                // get source data array
                let array = &cols[col_idx];
                if array.is_null(rec_offset) {
                    cells.push(None);
                    continue;
                }

                // get source field type
                let src_type = self.src_fields[col_idx].field_type.as_ref();

                // map source to target cell
                let cell = self
                    .mapper
                    .map_cell(batch, tgt_col, array, src_type, rec_offset)?;
                cells.push(Some(cell));
            }

            self.row_data.push_back(cells);
        }

        Ok(())
    }

    // make scan to Iceberg and save record stream locally
    fn do_iceberg_scan(&mut self) -> IcebergFdwResult<()> {
        self.reset();

        if let Some(table) = &self.table {
            let mut scan_builder = table
                .scan()
                .select(self.tgt_cols.iter().map(|c| c.name.clone()))
                .with_batch_size(self.batch_size);
            if let Some(predicate) = &self.predicate {
                scan_builder = scan_builder.with_filter(predicate.clone());
            }
            let scan = scan_builder.build()?;

            // debug the record count and data files has been scanned
            if cfg!(debug_assertions) {
                let mut scan_files = self.rt.block_on(scan.plan_files())?;
                while let Some(sf) = self.rt.block_on(scan_files.next()) {
                    let sf = sf.unwrap();
                    report_info(&format!(
                        "file scan: {:?}, {}",
                        sf.record_count, sf.data_file_path
                    ));
                }
            }

            // convert to record stream and cache it locally
            self.stream = self.rt.block_on(scan.to_arrow())?.into();
        }

        Ok(())
    }

    fn reset(&mut self) {
        self.stream = None;
        self.row_data.clear();
        self.mapper.reset();
    }

    // sort input rows by partition column values
    fn sort_rows_by_partition(
        &mut self,
        metadata: &iceberg::spec::TableMetadata,
        schema: &iceberg::spec::Schema,
    ) -> IcebergFdwResult<Vec<InputRow>> {
        let partition_spec = metadata.default_partition_spec();

        // if no partition spec, return original order
        if partition_spec.fields().is_empty() {
            return Ok(self.input_rows.clone());
        }

        let mut rows_with_keys = Vec::new();

        // compute partition key for each row
        for row in &self.input_rows {
            let partition_key = self.compute_partition_key_for_input_row(metadata, schema, row)?;
            rows_with_keys.push((row.clone(), partition_key));
        }

        // sort by partition key
        rows_with_keys.sort_by(|a, b| a.1.cmp(&b.1));

        // extract sorted rows
        let sorted_rows = rows_with_keys.into_iter().map(|(row, _)| row).collect();
        Ok(sorted_rows)
    }

    // compute partition key for an input row
    fn compute_partition_key_for_input_row(
        &self,
        metadata: &iceberg::spec::TableMetadata,
        schema: &iceberg::spec::Schema,
        row: &InputRow,
    ) -> IcebergFdwResult<String> {
        let partition_spec = metadata.default_partition_spec();
        let mut key_parts = Vec::new();

        for partition_field in partition_spec.fields() {
            let source_field_id = partition_field.source_id;
            let field_name = &partition_field.name;

            // find the column index for this field ID in the schema
            let mut source_column_index = None;
            for (idx, field) in schema.as_struct().fields().iter().enumerate() {
                if field.id == source_field_id {
                    source_column_index = Some(idx);
                    break;
                }
            }
            let column_index = source_column_index.ok_or_else(|| {
                IcebergFdwError::ColumnNotFound(format!(
                    "cannot find source column with ID {} for partition field",
                    source_field_id
                ))
            })?;

            // get the cell value for this column
            if let Some(Some(cell)) = row.cells.get(column_index) {
                // for now, just use string representation
                // in a full implementation, you'd handle different transforms
                key_parts.push(format!("{}={}", field_name, cell));
            } else {
                key_parts.push(format!("{}=null", field_name));
            }
        }

        Ok(key_parts.join("/"))
    }

    // build record batch from sorted input rows
    fn build_record_batch_from_rows(
        &mut self,
        schema: &iceberg::spec::Schema,
        sorted_rows: &[InputRow],
    ) -> IcebergFdwResult<RecordBatch> {
        let schema: arrow_schema::Schema = schema.try_into()?;
        let mut builders: Vec<Box<dyn ArrayBuilder>> = Vec::new();

        for field in &schema.fields {
            let builder = arrow_array::builder::make_builder(field.data_type(), sorted_rows.len());
            builders.push(builder);
        }

        // populate builders with sorted row data
        for row in sorted_rows {
            for (col_idx, cell) in row.cells.iter().enumerate() {
                let builder = &mut builders[col_idx];
                let field_type = &schema.fields[col_idx].data_type();
                self.mapper
                    .append_array_value(builder, field_type, cell.as_ref())?;
            }
        }

        // convert builders to arrays
        let mut arrays: Vec<ArrayRef> = Vec::new();
        for mut builder in builders.drain(..) {
            let array = builder.finish();
            arrays.push(array);
        }

        // create record batch
        let rb_option = arrow_array::RecordBatchOptions::new().with_match_field_names(false);
        let record_batch =
            RecordBatch::try_new_with_options(Arc::new(schema.clone()), arrays, &rb_option)?;

        Ok(record_batch)
    }
}

impl ForeignDataWrapper<IcebergFdwError> for IcebergFdw {
    fn new(server: ForeignServer) -> IcebergFdwResult<Self> {
        // transform server options into properties for catalog creation
        let mut props: HashMap<String, String> = server
            .options
            .iter()
            .map(|(k, v)| -> IcebergFdwResult<_> {
                // get decrypted text from options with 'vault_' prefix
                let value = if k.starts_with("vault_") {
                    if let Some(val) = get_vault_secret(v) {
                        val
                    } else {
                        return Err(IcebergFdwError::VaultError(format!(
                            "cannot decrypt for '{k}' from Vault"
                        )));
                    }
                } else {
                    v.clone()
                };
                let key = k.strip_prefix("vault_").unwrap_or(k).to_string();
                Ok((key, value))
            })
            .collect::<IcebergFdwResult<Vec<_>>>()?
            .into_iter()
            .collect();

        // copy AWS credentials if they're not set by user
        utils::copy_option(&mut props, "aws_access_key_id", "s3.access-key-id");
        utils::copy_option(&mut props, "aws_secret_access_key", "s3.secret-access-key");
        utils::copy_option(&mut props, "region_name", "s3.region");

        let batch_size = require_option_or("batch_size", &server.options, "4096")
            .parse::<usize>()
            .unwrap_or(4096);

        let rt = create_async_runtime()?;

        // create catalog
        // note: only below services are supported now:
        //   1. S3 tables
        //   2. REST catalog with S3 (or compatible) as backend storage
        let catalog: Box<dyn Catalog> =
            if let Some(aws_s3table_arn) = props.get("aws_s3table_bucket_arn") {
                let catalog_config = S3TablesCatalogConfig::builder()
                    .table_bucket_arn(aws_s3table_arn.into())
                    .properties(props)
                    .build();
                Box::new(rt.block_on(S3TablesCatalog::new(catalog_config))?)
            } else {
                let catalog_uri = require_option("catalog_uri", &props)?;
                let warehouse = require_option_or("warehouse", &props, "warehouse");
                let catalog_config = RestCatalogConfig::builder()
                    .warehouse(warehouse.into())
                    .uri(catalog_uri.into())
                    .props(props)
                    .build();
                Box::new(RestCatalog::new(catalog_config))
            };

        stats::inc_stats(Self::FDW_NAME, stats::Metric::CreateTimes, 1);

        Ok(IcebergFdw {
            rt,
            catalog,
            table: None,
            predicate: None,
            batch_size: batch_size.into(),
            tgt_cols: Vec::new(),
            stream: None,
            row_data: VecDeque::new(),
            src_fields: Vec::new(),
            mapper: Mapper::default(),
            num_rows: 0,
            bytes_in: 0,
            input_rows: Vec::new(),
        })
    }

    fn begin_scan(
        &mut self,
        quals: &[Qual],
        columns: &[Column],
        _sorts: &[Sort],
        _limit: &Option<Limit>,
        options: &HashMap<String, String>,
    ) -> IcebergFdwResult<()> {
        let tbl_ident = TableIdent::from_strs(require_option("table", options)?.split("."))?;
        let table = self.rt.block_on(self.catalog.load_table(&tbl_ident))?;

        let schema = table.metadata().current_schema();
        for tgt_col in columns {
            let col_name = &tgt_col.name;
            let field = schema
                .field_by_name_case_insensitive(col_name)
                .ok_or_else(|| IcebergFdwError::ColumnNotFound(col_name.into()))?;
            self.src_fields.push(field.clone());
        }

        self.predicate = try_pushdown(&table, quals)?;
        self.table = table.into();
        self.tgt_cols = columns.to_vec();

        self.do_iceberg_scan()
    }

    fn iter_scan(&mut self, row: &mut Row) -> IcebergFdwResult<Option<()>> {
        if self.row_data.is_empty() {
            self.next_batch()?;
        }

        if let Some(cells) = self.row_data.pop_front() {
            let src_row = Row {
                cols: self.tgt_cols.iter().map(|c| c.name.clone()).collect(),
                cells,
            };
            row.replace_with(src_row);
            return Ok(Some(()));
        }

        stats::inc_stats(Self::FDW_NAME, stats::Metric::RowsIn, self.num_rows as _);
        stats::inc_stats(Self::FDW_NAME, stats::Metric::BytesIn, self.bytes_in as _);

        Ok(None)
    }

    fn re_scan(&mut self) -> IcebergFdwResult<()> {
        self.do_iceberg_scan()
    }

    fn end_scan(&mut self) -> IcebergFdwResult<()> {
        self.reset();
        Ok(())
    }

    fn begin_modify(&mut self, options: &HashMap<String, String>) -> IcebergFdwResult<()> {
        let tbl_ident = TableIdent::from_strs(require_option("table", options)?.split("."))?;
        self.table = self
            .rt
            .block_on(self.catalog.load_table(&tbl_ident))?
            .into();
        self.input_rows.clear();

        Ok(())
    }

    fn insert(&mut self, src: &Row) -> IcebergFdwResult<()> {
        // save the row in the input row buffer
        self.input_rows.push(InputRow {
            cells: src.cells.clone(),
        });

        Ok(())
    }

    fn end_modify(&mut self) -> IcebergFdwResult<()> {
        // only write if we have rows
        if self.input_rows.is_empty() {
            return Ok(());
        }

        // clone the table to avoid borrowing conflicts
        let table = match &self.table {
            Some(table) => table.clone(),
            None => return Ok(()),
        };

        let metadata = table.metadata();
        let schema = metadata.current_schema();

        // sort input_rows by partition column values
        let sorted_rows = self.sort_rows_by_partition(metadata, schema)?;

        // build record batch from sorted rows
        let record_batch = self.build_record_batch_from_rows(schema, &sorted_rows)?;

        // split the record batch by partition values
        let partition_batches = utils::split_record_batch_by_partition(metadata, record_batch)?;

        let mut data_files = Vec::new();

        // write each partition batch separately
        for partition_batch in partition_batches.iter() {
            let location_generator = LocationGenerator::new(metadata, partition_batch)?;
            let file_name_generator = FileNameGenerator::new(DataFileFormat::Parquet);

            // get partition value from location generator
            let partition_value = location_generator.partition_value();

            let parquet_writer_builder = ParquetWriterBuilder::new(
                WriterProperties::default(),
                schema.clone(),
                table.file_io().clone(),
                location_generator,
                file_name_generator,
            );
            let data_file_writer_builder = DataFileWriterBuilder::new(
                parquet_writer_builder,
                partition_value,
                metadata.default_partition_spec().spec_id(),
            );
            let mut data_file_writer = self.rt.block_on(data_file_writer_builder.build())?;

            // write the record batch to Iceberg and close the writer and get
            // the data file
            self.rt
                .block_on(data_file_writer.write(partition_batch.clone()))?;
            let mut part_data_files = self.rt.block_on(data_file_writer.close())?;

            data_files.append(&mut part_data_files);
        }

        // create transaction and commit the changes to update table metadata
        let tx = Transaction::new(&table);
        let append_action = tx.fast_append().add_data_files(data_files.clone());
        let tx = append_action.apply(tx)?;
        let updated_table = self.rt.block_on(tx.commit(self.catalog.as_ref()))?;

        // update the cached table reference with the new metadata
        self.table = Some(updated_table);

        if cfg!(debug_assertions) {
            for data_file in &data_files {
                report_info(&format!(
                    "Data file: {}, records: {}, size: {} bytes",
                    data_file.file_path(),
                    data_file.record_count(),
                    data_file.file_size_in_bytes()
                ));
            }
        }

        Ok(())
    }

    fn import_foreign_schema(
        &mut self,
        stmt: ImportForeignSchemaStmt,
    ) -> IcebergFdwResult<Vec<String>> {
        let is_strict =
            require_option_or("strict", &stmt.options, "false").to_lowercase() == "true";

        // get table list under specified remote schema
        let ns = NamespaceIdent::from_strs(stmt.remote_schema.split('.'))?;
        let tbl_idents = self.rt.block_on(self.catalog.list_tables(&ns))?;

        // filter out selected table name list
        let all_tables: HashSet<&str> =
            HashSet::from_iter(tbl_idents.iter().map(|i| i.name.as_str()));
        let table_list = stmt.table_list.iter().map(|t| t.as_str()).collect();
        let selected = match stmt.list_type {
            ImportSchemaType::FdwImportSchemaAll => all_tables,
            ImportSchemaType::FdwImportSchemaLimitTo => {
                all_tables.intersection(&table_list).copied().collect()
            }
            ImportSchemaType::FdwImportSchemaExcept => {
                all_tables.difference(&table_list).copied().collect()
            }
        };

        // get selected table instances
        let tbls: Vec<Table> = tbl_idents
            .iter()
            .filter(|t| selected.contains(t.name.as_str()))
            .map(|t| self.rt.block_on(self.catalog.load_table(t)))
            .collect::<Result<Vec<_>, _>>()?;

        let mut ret: Vec<String> = Vec::new();

        // generate DDL for each table
        for tbl in tbls {
            let schema = tbl.metadata().current_schema();
            let mut fields: Vec<String> = Vec::new();

            for field in schema.as_struct().fields() {
                let field_name = pgrx::spi::quote_identifier(&field.name);
                let not_null = if field.required { "not null" } else { "" };

                match *field.field_type {
                    Type::Primitive(ref p) => {
                        let pg_type = match p {
                            PrimitiveType::Boolean => "bool",
                            PrimitiveType::Int => "integer",
                            PrimitiveType::Long => "bigint",
                            PrimitiveType::Float => "real",
                            PrimitiveType::Double => "double precision",
                            PrimitiveType::Decimal { precision, scale } => {
                                &format!("numeric({}, {})", precision, scale)
                            }
                            PrimitiveType::String => "text",
                            PrimitiveType::Date => "date",
                            PrimitiveType::Timestamp => "timestamp",
                            PrimitiveType::Timestamptz => "timestamp with time zone",
                            PrimitiveType::Uuid => "uuid",
                            PrimitiveType::Binary => "bytea",
                            _ => {
                                if is_strict {
                                    return Err(IcebergFdwError::ImportColumnError(
                                        format!("{}.{}", tbl.identifier(), field_name),
                                        (*field.field_type).to_string(),
                                    ));
                                }
                                continue;
                            }
                        };
                        fields.push(format!("{} {} {}", field_name, pg_type, not_null));
                    }
                    Type::Struct(_) | Type::List(_) | Type::Map(_) => {
                        fields.push(format!("{} jsonb {}", field_name, not_null));
                    }
                }
            }

            if !fields.is_empty() {
                let ident_field_ids: Vec<i32> = schema.identifier_field_ids().collect();
                let rowid_column = if ident_field_ids.len() == 1 {
                    schema
                        .field_by_id(ident_field_ids[0])
                        .map(|field| format!(", rowid_column '{}'", field.name))
                } else {
                    None
                };

                ret.push(format!(
                    r#"create foreign table if not exists {} (
                        {}
                    )
                    server {} options (table '{}'{})"#,
                    tbl.identifier().name,
                    fields.join(","),
                    stmt.server_name,
                    tbl.identifier(),
                    rowid_column.unwrap_or_default(),
                ));
            }
        }

        Ok(ret)
    }

    fn validator(
        options: Vec<Option<String>>,
        catalog: Option<pg_sys::Oid>,
    ) -> IcebergFdwResult<()> {
        if let Some(oid) = catalog {
            if oid == FOREIGN_TABLE_RELATION_ID {
                // check required option
                check_options_contain(&options, "table")?;
            }
        }

        Ok(())
    }
}
