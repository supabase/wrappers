use std::collections::HashMap;
use supabase_wrappers::{Cell, ForeignDataWrapper, Limit, Qual, Row, Sort};

pub(crate) struct HelloWorldFdw {
    row_cnt: i64,
}

impl HelloWorldFdw {
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
        self.row_cnt = 0;
    }

    fn iter_scan(&mut self) -> Option<Row> {
        if self.row_cnt < 1 {
            let mut row = Row::new();
            row.push("id", Some(Cell::I64(self.row_cnt)));
            row.push("col", Some(Cell::String("Hello world".to_string())));
            self.row_cnt += 1;
            return Some(row);
        }
        None
    }

    fn end_scan(&mut self) {}
}
