use crate::fdw::qdrant_fdw::qdrant_client::{QdrantClient, QdrantClientError};
use std::collections::VecDeque;
use supabase_wrappers::prelude::{Column, Row};

pub(crate) struct RowsIterator {
    collection_name: String,
    fetch_payload: bool,
    fetch_vector: bool,
    columns: Vec<Column>,
    qdrant_client: QdrantClient,
    batch_size: u64,
    rows: VecDeque<Row>,
    have_more_rows: bool,
    next_page_offset: Option<u64>,
}

impl RowsIterator {
    pub(crate) fn new(
        collection_name: String,
        columns: Vec<Column>,
        batch_size: u64,
        qdrant_client: QdrantClient,
    ) -> Self {
        let fetch_payload = columns.iter().any(|col| col.name == "payload");
        let fetch_vector = columns.iter().any(|col| col.name == "vector");
        Self {
            collection_name,
            fetch_payload,
            fetch_vector,
            columns,
            qdrant_client,
            batch_size,
            rows: VecDeque::new(),
            have_more_rows: true,
            next_page_offset: None,
        }
    }

    fn get_limit(&self) -> Option<u64> {
        Some(self.batch_size)
    }

    fn get_offset(&self) -> Option<u64> {
        self.next_page_offset
    }

    fn fetch_rows_batch(&mut self) -> Result<Option<Row>, QdrantClientError> {
        let points_result = self.qdrant_client.fetch_points(
            &self.collection_name,
            self.fetch_payload,
            self.fetch_vector,
            self.get_limit(),
            self.get_offset(),
        )?;
        self.rows = points_result
            .points
            .into_iter()
            .map(|p| p.into_row(&self.columns))
            .collect();
        self.next_page_offset = points_result.next_page_offset;
        self.have_more_rows = self.next_page_offset.is_some();
        Ok(self.get_next_row())
    }

    fn get_next_row(&mut self) -> Option<Row> {
        self.rows.pop_front()
    }
}

impl Iterator for RowsIterator {
    type Item = Result<Row, QdrantClientError>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(row) = self.get_next_row() {
            Some(Ok(row))
        } else {
            if self.have_more_rows {
                self.fetch_rows_batch().transpose()
            } else {
                None
            }
        }
    }
}
