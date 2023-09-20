use pgrx::pg_sys::panic::ErrorReport;
use pgrx::PgSqlErrorCode;
use std::collections::HashMap;
use supabase_wrappers::prelude::*;

// A simple demo FDW
#[wrappers_fdw(
    version = "0.1.1",
    author = "Supabase",
    website = "https://github.com/supabase/wrappers/tree/main/wrappers/src/fdw/helloworld_fdw",
    error_type = "HelloWorldFdwError"
)]
pub(crate) struct HelloWorldFdw {
    // row counter
    row_cnt: i64,

    // target column list
    tgt_cols: Vec<Column>,
}

enum HelloWorldFdwError {}

impl From<HelloWorldFdwError> for ErrorReport {
    fn from(_value: HelloWorldFdwError) -> Self {
        ErrorReport::new(PgSqlErrorCode::ERRCODE_FDW_ERROR, "", "")
    }
}

type HelloWorldFdwResult<T> = Result<T, HelloWorldFdwError>;

impl ForeignDataWrapper<HelloWorldFdwError> for HelloWorldFdw {
    // 'options' is the key-value pairs defined in `CREATE SERVER` SQL, for example,
    //
    // create server my_helloworld_server
    //   foreign data wrapper wrappers_helloworld
    //   options (
    //     foo 'bar'
    // );
    //
    // 'options' passed here will be a hashmap { 'foo' -> 'bar' }.
    //
    // You can do any initalization in this new() function, like saving connection
    // info or API url in an variable, but don't do any heavy works like making a
    // database connection or API call.
    fn new(_options: &HashMap<String, String>) -> HelloWorldFdwResult<Self> {
        Ok(Self {
            row_cnt: 0,
            tgt_cols: Vec::new(),
        })
    }

    fn begin_scan(
        &mut self,
        _quals: &[Qual],
        columns: &[Column],
        _sorts: &[Sort],
        _limit: &Option<Limit>,
        _options: &HashMap<String, String>,
    ) -> HelloWorldFdwResult<()> {
        // reset row counter
        self.row_cnt = 0;

        // save a copy of target columns
        self.tgt_cols = columns.to_vec();

        Ok(())
    }

    fn iter_scan(&mut self, row: &mut Row) -> HelloWorldFdwResult<Option<()>> {
        // this is called on each row and we only return one row here
        if self.row_cnt < 1 {
            // add values to row if they are in target column list
            for tgt_col in &self.tgt_cols {
                match tgt_col.name.as_str() {
                    "id" => row.push("id", Some(Cell::I64(self.row_cnt))),
                    "col" => row.push("col", Some(Cell::String("Hello world".to_string()))),
                    _ => {}
                }
            }

            self.row_cnt += 1;

            // return Some(()) to Postgres and continue data scan
            return Ok(Some(()));
        }

        // return 'None' to stop data scan
        Ok(None)
    }

    fn end_scan(&mut self) -> HelloWorldFdwResult<()> {
        // we do nothing here, but you can do things like resource cleanup and etc.
        Ok(())
    }
}
