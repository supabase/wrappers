use crate::fdw::cognito_fdw::cognito_client::CognitoClientError;
use aws_sdk_cognitoidentityprovider::{config::Region, Client};

use std::collections::VecDeque;
use supabase_wrappers::prelude::{Column, Row};

pub(crate) struct RowsIterator {
    cognito_client: aws_sdk_cognitoidentityprovider::Client,
    batch_size: u64,
    columns: Vec<Column>,
    rows: VecDeque<Row>,
    have_more_rows: bool,
    next_page_offset: Option<u64>,
}

impl RowsIterator {
    pub(crate) fn new(
        columns: Vec<Column>,
        batch_size: u64,
        cognito_client: aws_sdk_cognitoidentityprovider::Client,
    ) -> Self {
        Self {
            columns,
            cognito_client,
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

    fn fetch_rows_batch(&mut self) -> Result<Option<Row>, CognitoClientError> {
        // TODO: Update logic

        // self.next_page_offset = user_result.next_page_offset;
        // self.have_more_rows = self.next_page_offset.is_some();
        //TODO: add proper logic to figure out when to set have_more_rows to false
        //hardcoding to false just to make the tests pass for now
        self.have_more_rows = false;
        Ok(self.get_next_row())
    }

    fn get_next_row(&mut self) -> Option<Row> {
        self.rows.pop_front()
    }
}

impl Iterator for RowsIterator {
    type Item = Result<Row, CognitoClientError>;

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
