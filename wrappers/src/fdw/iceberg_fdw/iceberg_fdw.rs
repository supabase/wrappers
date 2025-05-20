use arrow_array::RecordBatch;
use futures::StreamExt;
use iceberg::{
    expr::Predicate,
    scan::ArrowRecordBatchStream,
    spec::{PrimitiveType, Type},
    table::Table,
    Catalog, NamespaceIdent, TableIdent,
};
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};
use iceberg_catalog_s3tables::{S3TablesCatalog, S3TablesCatalogConfig};
use pgrx::pg_sys;
use std::collections::{HashMap, HashSet};

use supabase_wrappers::prelude::*;

use super::{mapper::Mapper, pushdown::try_pushdown, IcebergFdwError, IcebergFdwResult};
use crate::stats;

#[wrappers_fdw(
    version = "0.1.0",
    author = "Supabase",
    website = "https://github.com/supabase/wrappers/tree/main/wrappers/src/fdw/iceberg_fdw",
    error_type = "IcebergFdwError"
)]
pub(crate) struct IcebergFdw {
    rt: Runtime,
    catalog: Box<dyn Catalog>,
    table: Option<Table>,
    predicate: Predicate,
    tgt_cols: Vec<Column>,
    stream: Option<ArrowRecordBatchStream>,
    batch: Option<RecordBatch>,
    mapper: Mapper,
    rec_offset: usize,
}

impl IcebergFdw {
    const FDW_NAME: &'static str = "IcebergFdw";

    // fetch next record batch from Arrow record batch stream
    fn next_batch(&mut self) -> IcebergFdwResult<()> {
        if let Some(stream) = &mut self.stream {
            self.batch = if let Some(result) = self.rt.block_on(stream.next()) {
                let batch = result?;
                if batch.num_rows() > 0 {
                    stats::inc_stats(Self::FDW_NAME, stats::Metric::RowsIn, batch.num_rows() as _);
                    stats::inc_stats(
                        Self::FDW_NAME,
                        stats::Metric::BytesIn,
                        batch.get_array_memory_size() as _,
                    );
                    Some(batch)
                } else {
                    None
                }
            } else {
                None
            };

            self.mapper.reset();
            self.rec_offset = 0;
        }
        Ok(())
    }

    // convert a batch record to a row
    fn record_to_row(&self, batch: &RecordBatch, row: &mut Row) -> IcebergFdwResult<()> {
        if let Some(table) = &self.table {
            // get source table schema
            let schema = table.metadata().current_schema();

            for tgt_col in &self.tgt_cols {
                let col_name = &tgt_col.name;

                // get source data array
                let array = batch
                    .column_by_name(col_name)
                    .ok_or(IcebergFdwError::ColumnNotFound(col_name.into()))?;
                if array.is_null(self.rec_offset) {
                    row.push(col_name, None);
                    continue;
                }

                // get source field and type
                let field = schema
                    .field_by_name_case_insensitive(col_name)
                    .ok_or(IcebergFdwError::ColumnNotFound(col_name.into()))?;
                let src_type = field.field_type.as_ref();

                // map source to target cell
                let cell: Option<Cell> =
                    self.mapper
                        .map_cell(batch, tgt_col, array, src_type, self.rec_offset)?;
                if cell.is_none() {
                    return Err(IcebergFdwError::IncompatibleColumnType(
                        col_name.into(),
                        (*src_type).to_string(),
                    ));
                }
                row.push(col_name, cell);
            }
        }

        Ok(())
    }

    // make scan to Iceberg and save record stream locally
    fn do_iceberg_scan(&mut self) -> IcebergFdwResult<()> {
        self.reset();

        if let Some(table) = &self.table {
            let scan = table
                .scan()
                .select(self.tgt_cols.iter().map(|c| c.name.clone()))
                .with_filter(self.predicate.clone())
                .build()?;

            // debug the record cound and data files has been scanned
            if cfg!(debug_assertions) {
                let mut scan_files = self.rt.block_on(scan.plan_files())?;
                while let Some(sf) = self.rt.block_on(scan_files.next()) {
                    let sf = sf.unwrap();
                    log_debug1(&format!(
                        "file scan: {:?}, {}",
                        sf.record_count, sf.data_file_path
                    ));
                }
            }

            // save record stream
            self.stream = self.rt.block_on(scan.to_arrow())?.into();
        }

        Ok(())
    }

    fn reset(&mut self) {
        self.stream = None;
        self.batch = None;
        self.mapper.reset();
        self.rec_offset = 0;
    }
}

impl ForeignDataWrapper<IcebergFdwError> for IcebergFdw {
    fn new(server: ForeignServer) -> IcebergFdwResult<Self> {
        // get aws config from server options
        let (aws_key_id, aws_secret_key) = match server.options.get("vault_access_key_id") {
            Some(key_id) => {
                let secret_key = require_option("vault_secret_access_key", &server.options)?;
                get_vault_secret(key_id)
                    .zip(get_vault_secret(secret_key))
                    .ok_or(IcebergFdwError::VaultError(
                        "cannot get credentials from Vault".into(),
                    ))?
            }
            None => {
                let key_id = require_option("aws_access_key_id", &server.options)?.to_string();
                let secret_key =
                    require_option("aws_secret_access_key", &server.options)?.to_string();
                (key_id, secret_key)
            }
        };
        let aws_region = require_option_or("aws_region", &server.options, "us-east-1");
        let s3_endpoint_url = server.options.get("s3_endpoint_url");

        // set properties passed to catalog config
        let mut props: HashMap<String, String> = HashMap::new();
        props.insert("aws_access_key_id".into(), aws_key_id.clone());
        props.insert("aws_secret_access_key".into(), aws_secret_key.clone());
        props.insert("region_name".into(), aws_region.to_owned());
        props.insert("s3.access-key-id".into(), aws_key_id);
        props.insert("s3.secret-access-key".into(), aws_secret_key);
        props.insert("s3.region".into(), aws_region.into());
        if let Some(s3_endpoint_url) = s3_endpoint_url {
            props.insert("s3.endpoint".into(), s3_endpoint_url.into());
        }

        let rt = create_async_runtime()?;

        // create catalog
        // note: only below services are supported now:
        //   1. S3 tables
        //   2. REST catalog with S3 as backend storage
        let catalog: Box<dyn Catalog> =
            if let Some(aws_s3table_arn) = server.options.get("aws_s3table_bucket_arn") {
                let catalog_config = S3TablesCatalogConfig::builder()
                    .table_bucket_arn(aws_s3table_arn.into())
                    .properties(props)
                    .build();
                Box::new(rt.block_on(S3TablesCatalog::new(catalog_config))?)
            } else {
                let catalog_uri = require_option("catalog_uri", &server.options)?;
                let catalog_config = RestCatalogConfig::builder()
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
            predicate: Predicate::AlwaysTrue,
            tgt_cols: Vec::new(),
            stream: None,
            batch: None,
            mapper: Mapper::default(),
            rec_offset: 0,
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
        self.predicate = try_pushdown(&table, quals)?.unwrap_or(Predicate::AlwaysTrue);
        self.table = table.into();
        self.tgt_cols = columns.to_vec();

        self.do_iceberg_scan()
    }

    fn iter_scan(&mut self, row: &mut Row) -> IcebergFdwResult<Option<()>> {
        if self.stream.is_some() {
            if let Some(batch) = &self.batch {
                if self.rec_offset >= batch.num_rows() {
                    self.next_batch()?;
                }
            } else {
                self.next_batch()?;
            }

            if let Some(batch) = &self.batch {
                self.record_to_row(batch, row)?;
                self.rec_offset += 1;
                return Ok(Some(()));
            }
        }
        Ok(None)
    }

    fn re_scan(&mut self) -> IcebergFdwResult<()> {
        self.do_iceberg_scan()
    }

    fn end_scan(&mut self) -> IcebergFdwResult<()> {
        self.reset();
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
                ret.push(format!(
                    r#"create foreign table if not exists {} (
                        {}
                    )
                    server {} options (table '{}')"#,
                    tbl.identifier().name,
                    fields.join(","),
                    stmt.server_name,
                    tbl.identifier(),
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
            if oid == FOREIGN_SERVER_RELATION_ID {
                // AWS credential pair must be specified together
                let a = check_options_contain(&options, "aws_access_key_id");
                let b = check_options_contain(&options, "aws_secret_access_key");
                match (a, b) {
                    (a @ Err(_), Ok(_)) => a.map(|_| ())?,
                    (Ok(_), b @ Err(_)) => b.map(|_| ())?,
                    _ => (),
                }
            } else if oid == FOREIGN_TABLE_RELATION_ID {
                // check required option
                check_options_contain(&options, "table")?;
            }
        }

        Ok(())
    }
}
