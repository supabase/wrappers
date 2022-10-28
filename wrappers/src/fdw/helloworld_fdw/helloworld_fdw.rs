use std::collections::HashMap;
use supabase_wrappers::{Cell, ForeignDataWrapper, Limit, Qual, Row, Sort};

// A simple demo FDW
pub(crate) struct HelloWorldFdw {
    row_cnt: i64,
}

impl HelloWorldFdw {
    // 'options' is the key-value pairs defined in 'create server` SQL, for example,
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
    pub fn new(_options: &HashMap<String, String>) -> Self {
        Self { row_cnt: 0 }
    }
}

impl ForeignDataWrapper for HelloWorldFdw {
    fn begin_scan(
        &mut self,
        _quals: &Vec<Qual>,
        _columns: &Vec<String>,
        _sorts: &Vec<Sort>,
        _limit: &Option<Limit>,
        _options: &HashMap<String, String>,
    ) {
        // reset row count
        self.row_cnt = 0;
    }

    fn iter_scan(&mut self) -> Option<Row> {
        // this is called on each row and we only return one row here
        if self.row_cnt < 1 {
            // create an empty row
            let mut row = Row::new();

            // add value to 'id' column
            row.push("id", Some(Cell::I64(self.row_cnt)));

            // add value to 'col' column
            row.push("col", Some(Cell::String("Hello world".to_string())));

            self.row_cnt += 1;

            // return the Some(row) to Postgres and continue data scan
            return Some(row);
        }

        // return 'None' to stop data scan
        None
    }

    fn end_scan(&mut self) {
        // we do nothing here, but you can do things like resource cleanup and etc.
    }
}
