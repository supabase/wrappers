use crate::stats;
use iceberg::{
    Catalog, TableIdent,
    io::{FileIO, FileIOBuilder, S3_REGION},
    scan::ArrowRecordBatchStream,
};
use iceberg_catalog_memory::MemoryCatalog;
use futures::StreamExt;
use std::collections::HashMap;

use supabase_wrappers::prelude::*;

use super::{IcebergFdwError, IcebergFdwResult};

#[wrappers_fdw(
    version = "0.1.0",
    author = "Supabase",
    website = "https://github.com/supabase/wrappers/tree/main/wrappers/src/fdw/iceberg_fdw",
    error_type = "IcebergFdwError"
)]
pub(crate) struct IcebergFdw {
    rt: Runtime,
    stream: Option<ArrowRecordBatchStream>,
}

impl IcebergFdw {
    const FDW_NAME: &'static str = "IcebergFdw";
}

impl ForeignDataWrapper<IcebergFdwError> for IcebergFdw {
    fn new(server: ForeignServer) -> IcebergFdwResult<Self> {
        let rt = create_async_runtime()?;

        stats::inc_stats(Self::FDW_NAME, stats::Metric::CreateTimes, 1);

        Ok(IcebergFdw {
            rt,
            stream: None,
        })
    }

    fn begin_scan(
        &mut self,
        quals: &[Qual],
        columns: &[Column],
        sorts: &[Sort],
        limit: &Option<Limit>,
        options: &HashMap<String, String>,
    ) -> IcebergFdwResult<()> {
        let file_io = FileIOBuilder::new("s3")
            .with_prop(S3_REGION, "ap-southeast-2")
            .build()?;
        let catalog = MemoryCatalog::new(file_io, None);
        let table = self.rt.block_on(catalog.load_table(&TableIdent::from_strs(["hello", "world"])?))?;
        let stream = self.rt.block_on(table
            .scan()
            .select(["name", "id"])
            .build()?
            .to_arrow())?;
        self.stream = stream.into();
        // let data: Vec<_> = self.rt.block_on(stream.try_collect())?;
        // report_warning(&format!("===data: {:?}", data));
        Ok(())
    }

    fn iter_scan(&mut self, row: &mut Row) -> IcebergFdwResult<Option<()>> {
        if let Some(ref mut stream) = &mut self.stream {
            let a = self.rt.block_on(stream.next()).unwrap()?;
            report_warning(&format!("===data: {:?}", a));
        }
        Ok(None)
    }

    fn re_scan(&mut self) -> IcebergFdwResult<()> {
        Ok(())
    }

    fn end_scan(&mut self) -> IcebergFdwResult<()> {
        Ok(())
    }
}
