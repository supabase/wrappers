#![allow(clippy::result_large_err)]
use aws_sdk_cognitoidentityprovider::Client;
use std::collections::VecDeque;
use std::sync::Arc;

use supabase_wrappers::prelude::{Column, Row, Runtime};

use super::super::CognitoFdwResult;
use super::row::IntoRow;

pub(in super::super) struct RowsIterator {
    rt: Arc<Runtime>,
    cognito_client: Client,
    columns: Vec<Column>,
    rows: VecDeque<Row>,
    user_pool_id: String,
    have_more_rows: bool,
    pagination_token: Option<String>,
}

impl RowsIterator {
    pub(in super::super) fn new(
        rt: Arc<Runtime>,
        columns: Vec<Column>,
        user_pool_id: String,
        cognito_client: Client,
    ) -> Self {
        Self {
            rt,
            columns,
            cognito_client,
            user_pool_id,
            rows: VecDeque::new(),
            have_more_rows: true,
            pagination_token: None,
        }
    }

    fn fetch_rows_batch(&mut self) -> CognitoFdwResult<Option<Row>> {
        self.have_more_rows = false;

        let mut request = self
            .cognito_client
            .list_users()
            .user_pool_id(self.user_pool_id.clone());

        if let Some(ref token) = self.pagination_token {
            request = request.pagination_token(token.clone());
        }

        let resp = self
            .rt
            .block_on(request.send())
            .map_err(aws_sdk_cognitoidentityprovider::Error::from)?;
        self.pagination_token.clone_from(&resp.pagination_token);
        self.rows = resp
            .users
            .unwrap_or_default()
            .into_iter()
            .map(|u| u.into_row(&self.columns))
            .collect::<Result<VecDeque<Row>, _>>()?;

        self.have_more_rows = self.pagination_token.is_some();

        Ok(self.get_next_row())
    }

    fn get_next_row(&mut self) -> Option<Row> {
        self.rows.pop_front()
    }
}

impl Iterator for RowsIterator {
    type Item = CognitoFdwResult<Row>;

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
