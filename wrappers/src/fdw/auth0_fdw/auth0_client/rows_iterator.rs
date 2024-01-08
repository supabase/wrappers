use crate::fdw::auth0_fdw::auth0_client::{Auth0Client, Auth0ClientError};
use std::collections::VecDeque;
use supabase_wrappers::prelude::{Column, Row};

pub(crate) struct RowsIterator {
    auth0_client: Auth0Client,
    per_page: u64,
    columns: Vec<Column>,
    rows: VecDeque<Row>,
    have_more_rows: bool,
    page_offset: u64,
}

impl RowsIterator {
    pub(crate) fn new(columns: Vec<Column>, per_page: u64, auth0_client: Auth0Client) -> Self {
        Self {
            columns,
            auth0_client,
            per_page,
            rows: VecDeque::new(),
            have_more_rows: true,
            page_offset: 0,
        }
    }

    fn get_page_offset(&self) -> u64 {
        self.page_offset
    }

    fn get_per_page(&self) -> Option<u64> {
        Some(self.per_page)
    }

    fn fetch_rows_batch(&mut self) -> Result<Option<Row>, Auth0ClientError> {
        let result_payload = self
            .auth0_client
            .fetch_users(self.get_page_offset(), self.get_per_page())?;
        let total = result_payload.get_total().unwrap_or(0);
        self.rows = result_payload
            .into_users()
            .into_iter()
            .map(|u| u.into_row(&self.columns))
            .collect();
        self.page_offset += 1;
        self.have_more_rows = total > self.per_page * self.page_offset;
        Ok(self.get_next_row())
    }

    fn get_next_row(&mut self) -> Option<Row> {
        self.rows.pop_front()
    }
}

impl Iterator for RowsIterator {
    type Item = Result<Row, Auth0ClientError>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(row) = self.get_next_row() {
            Some(Ok(row))
        } else if self.have_more_rows {
            self.fetch_rows_batch().transpose()
        } else {
            None
        }
    }
}
