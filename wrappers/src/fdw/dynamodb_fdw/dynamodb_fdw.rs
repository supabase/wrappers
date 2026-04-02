use std::collections::{HashMap, VecDeque};
use std::env;

use aws_config::BehaviorVersion;
use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::types::AttributeValue;
use pgrx::pg_sys;
use std::collections::HashMap as AttrMap;
use supabase_wrappers::prelude::*;

use crate::stats;

use super::conv::{attr_to_cell, attr_to_json, cell_to_attr};
use super::{DynamoDbFdwError, DynamoDbFdwResult};

type Expressions = (
    Option<String>,
    Option<String>,
    HashMap<String, String>,
    HashMap<String, AttributeValue>,
    bool,
);

#[wrappers_fdw(
    version = "0.1.0",
    author = "Supabase",
    website = "https://github.com/supabase/wrappers/tree/main/wrappers/src/fdw/dynamodb_fdw",
    error_type = "DynamoDbFdwError"
)]
pub(crate) struct DynamoDbFdw {
    rt: Runtime,
    client: Client,

    // populated from table options in begin_scan / begin_modify
    table_name: String,
    // discovered via DescribeTable
    partition_key: String,
    sort_key: Option<String>,

    // scan state
    tgt_cols: Vec<Column>,
    quals: Vec<Qual>,
    limit: Option<Limit>,

    // lazy pagination
    scan_started: bool,
    rows: VecDeque<AttrMap<String, AttributeValue>>,
    last_key: Option<AttrMap<String, AttributeValue>>,
    exhausted: bool,
    row_cnt: i64,
}

impl DynamoDbFdw {
    const FDW_NAME: &'static str = "DynamoDbFdw";

    fn build_client(server: &ForeignServer) -> DynamoDbFdwResult<(Runtime, Client)> {
        let rt = create_async_runtime()?;

        // get AWS credentials from server options
        let creds = {
            match server.options.get("vault_access_key_id") {
                Some(vault_access_key_id) => {
                    // if using credentials stored in Vault
                    let vault_secret_access_key =
                        require_option("vault_secret_access_key", &server.options)?;
                    get_vault_secret(vault_access_key_id)
                        .zip(get_vault_secret(vault_secret_access_key))
                }
                None => {
                    // if using credentials directly specified
                    let aws_access_key_id =
                        require_option("aws_access_key_id", &server.options)?.to_string();
                    let aws_secret_access_key =
                        require_option("aws_secret_access_key", &server.options)?.to_string();
                    Some((aws_access_key_id, aws_secret_access_key))
                }
            }
            .expect("AWS credentials should be provided in server options")
        };

        let region = require_option_or("region", &server.options, "us-east-1").to_string();

        unsafe {
            env::set_var("AWS_ACCESS_KEY_ID", &creds.0);
            env::set_var("AWS_SECRET_ACCESS_KEY", &creds.1);
            env::set_var("AWS_REGION", &region);
        }

        let endpoint_url = server.options.get("endpoint_url").cloned();
        if let Some(ref url) = endpoint_url {
            unsafe {
                env::set_var("AWS_ENDPOINT_URL", url);
            }
        }

        let config = rt.block_on(aws_config::defaults(BehaviorVersion::latest()).load());
        let client = Client::new(&config);

        Ok((rt, client))
    }

    /// Call DescribeTable and extract partition key + optional sort key names.
    fn describe_table(&self, table_name: &str) -> DynamoDbFdwResult<(String, Option<String>)> {
        let resp = self
            .rt
            .block_on(self.client.describe_table().table_name(table_name).send())
            .map_err(DynamoDbFdwError::from)?;

        let table = resp
            .table
            .ok_or_else(|| DynamoDbFdwError::TableNotFound(table_name.to_string()))?;

        let key_schema = table.key_schema.unwrap_or_default();
        let mut partition_key = String::new();
        let mut sort_key: Option<String> = None;

        for key_el in &key_schema {
            match key_el.key_type.as_str() {
                "HASH" => partition_key = key_el.attribute_name.clone(),
                "RANGE" => sort_key = Some(key_el.attribute_name.clone()),
                _ => {}
            }
        }

        Ok((partition_key, sort_key))
    }

    /// Build key condition expression and filter expression from quals.
    /// Returns (key_condition, filter_expression, expr_attr_names, expr_attr_values, use_query).
    fn build_expressions(&self) -> Expressions {
        let mut names: HashMap<String, String> = HashMap::new();
        let mut values: HashMap<String, AttributeValue> = HashMap::new();
        let mut key_conds: Vec<String> = Vec::new();
        let mut filter_conds: Vec<String> = Vec::new();

        let mut has_pk_equality = false;

        for (val_idx, qual) in self.quals.iter().enumerate() {
            let name_placeholder = format!("#n{val_idx}");
            let val_placeholder = format!(":v{val_idx}");

            names.insert(name_placeholder.clone(), qual.field.clone());

            // Extract the attribute value from the qual
            let attr_val = match &qual.value {
                Value::Cell(cell) => match cell_to_attr(&Some(cell.clone())) {
                    Ok(v) => v,
                    Err(_) => continue,
                },
                _ => continue, // skip array/complex quals
            };
            values.insert(val_placeholder.clone(), attr_val);

            let expr = match qual.operator.as_str() {
                "=" => {
                    if qual.field == self.partition_key {
                        has_pk_equality = true;
                        key_conds.push(format!("{name_placeholder} = {val_placeholder}"));
                        continue;
                    } else if self.sort_key.as_deref() == Some(qual.field.as_str()) {
                        key_conds.push(format!("{name_placeholder} = {val_placeholder}"));
                        continue;
                    }
                    format!("{name_placeholder} = {val_placeholder}")
                }
                "<" => format!("{name_placeholder} < {val_placeholder}"),
                "<=" => format!("{name_placeholder} <= {val_placeholder}"),
                ">" => format!("{name_placeholder} > {val_placeholder}"),
                ">=" => format!("{name_placeholder} >= {val_placeholder}"),
                "<>" => format!("{name_placeholder} <> {val_placeholder}"),
                _ => continue, // skip unsupported operators (LIKE etc.)
            };

            // Sort key range conditions belong in key condition expression when using Query
            if has_pk_equality && self.sort_key.as_deref() == Some(qual.field.as_str()) {
                key_conds.push(expr);
            } else {
                filter_conds.push(expr);
            }
        }

        let key_cond = if has_pk_equality && !key_conds.is_empty() {
            Some(key_conds.join(" AND "))
        } else {
            None
        };

        let filter_expr = if !filter_conds.is_empty() {
            Some(filter_conds.join(" AND "))
        } else {
            None
        };

        (key_cond, filter_expr, names, values, has_pk_equality)
    }

    /// Fetch next page using Scan API.
    fn fetch_scan_page(
        &mut self,
        filter_expr: &Option<String>,
        names: &HashMap<String, String>,
        values: &HashMap<String, AttributeValue>,
    ) -> DynamoDbFdwResult<()> {
        let mut req = self.client.scan().table_name(&self.table_name);

        if let Some(expr) = filter_expr {
            req = req.filter_expression(expr);
        }
        for (k, v) in names {
            req = req.expression_attribute_names(k, v);
        }
        for (k, v) in values {
            req = req.expression_attribute_values(k, v.clone());
        }
        if let Some(ref last) = self.last_key {
            for (k, v) in last {
                req = req.exclusive_start_key(k, v.clone());
            }
        }
        if let Some(ref lim) = self.limit {
            // Limit hint: request up to count+offset items to avoid over-fetching
            let remaining = lim.count + lim.offset - self.row_cnt;
            if remaining > 0 {
                req = req.limit(remaining as i32);
            }
        }

        let resp = self
            .rt
            .block_on(req.send())
            .map_err(DynamoDbFdwError::from)?;

        for item in resp.items.unwrap_or_default() {
            self.rows.push_back(item);
        }

        self.last_key = resp.last_evaluated_key;
        if self.last_key.is_none() {
            self.exhausted = true;
        }

        Ok(())
    }

    /// Fetch next page using Query API (partition key equality required).
    fn fetch_query_page(
        &mut self,
        key_cond: &str,
        filter_expr: &Option<String>,
        names: &HashMap<String, String>,
        values: &HashMap<String, AttributeValue>,
    ) -> DynamoDbFdwResult<()> {
        let mut req = self
            .client
            .query()
            .table_name(&self.table_name)
            .key_condition_expression(key_cond);

        if let Some(expr) = filter_expr {
            req = req.filter_expression(expr);
        }
        for (k, v) in names {
            req = req.expression_attribute_names(k, v);
        }
        for (k, v) in values {
            req = req.expression_attribute_values(k, v.clone());
        }
        if let Some(ref last) = self.last_key {
            for (k, v) in last {
                req = req.exclusive_start_key(k, v.clone());
            }
        }

        let resp = self
            .rt
            .block_on(req.send())
            .map_err(DynamoDbFdwError::from)?;

        for item in resp.items.unwrap_or_default() {
            self.rows.push_back(item);
        }

        self.last_key = resp.last_evaluated_key;
        if self.last_key.is_none() {
            self.exhausted = true;
        }

        Ok(())
    }

    /// Convert a DynamoDB item (attribute map) into a Row using target columns.
    ///
    /// For jsonb columns with no direct attribute name match, all DynamoDB attributes
    /// not covered by any other named column are collected into that column as a JSON object.
    /// This makes a catch-all `_attrs jsonb` column work as expected after
    /// `import_foreign_schema`.
    fn item_to_row(&self, item: &AttrMap<String, AttributeValue>) -> DynamoDbFdwResult<Row> {
        // Names of every explicitly-declared column — used to exclude them from the catch-all.
        let mapped_names: std::collections::HashSet<&str> =
            self.tgt_cols.iter().map(|c| c.name.as_str()).collect();

        let mut row = Row::new();
        for col in &self.tgt_cols {
            let cell = match item.get(&col.name) {
                // Direct attribute name match — normal conversion.
                Some(attr) => attr_to_cell(attr, col)?,

                // No direct match and column is jsonb → catch-all: collect every
                // DynamoDB attribute whose name isn't declared as its own column.
                None if col.type_oid == pg_sys::JSONBOID => {
                    let obj: serde_json::Map<String, serde_json::Value> = item
                        .iter()
                        .filter(|(k, _)| !mapped_names.contains(k.as_str()))
                        .map(|(k, v)| (k.clone(), attr_to_json(v)))
                        .collect();
                    if obj.is_empty() {
                        None
                    } else {
                        Some(Cell::Json(pgrx::JsonB(serde_json::Value::Object(obj))))
                    }
                }

                // No match and not jsonb → NULL.
                None => None,
            };
            row.push(&col.name, cell);
        }
        Ok(row)
    }
}

impl ForeignDataWrapper<DynamoDbFdwError> for DynamoDbFdw {
    fn new(server: ForeignServer) -> DynamoDbFdwResult<Self> {
        let (rt, client) = Self::build_client(&server)?;

        stats::inc_stats(Self::FDW_NAME, stats::Metric::CreateTimes, 1);

        Ok(DynamoDbFdw {
            rt,
            client,
            table_name: String::new(),
            partition_key: String::new(),
            sort_key: None,
            tgt_cols: Vec::new(),
            quals: Vec::new(),
            limit: None,
            scan_started: false,
            rows: VecDeque::new(),
            last_key: None,
            exhausted: false,
            row_cnt: 0,
        })
    }

    fn begin_scan(
        &mut self,
        quals: &[Qual],
        columns: &[Column],
        _sorts: &[Sort],
        limit: &Option<Limit>,
        options: &HashMap<String, String>,
    ) -> DynamoDbFdwResult<()> {
        self.table_name = require_option("table", options)?.to_string();
        self.tgt_cols = columns.to_vec();
        self.quals = quals.to_vec();
        self.limit = limit.clone();

        let (pk, sk) = self.describe_table(&self.table_name.clone())?;
        self.partition_key = pk;
        self.sort_key = sk;

        // Reset scan state
        self.scan_started = false;
        self.rows.clear();
        self.last_key = None;
        self.exhausted = false;
        self.row_cnt = 0;

        Ok(())
    }

    fn iter_scan(&mut self, row: &mut Row) -> DynamoDbFdwResult<Option<()>> {
        // Check limit
        if let Some(ref lim) = self.limit.clone() {
            if self.row_cnt >= lim.count + lim.offset {
                return Ok(None);
            }
        }

        loop {
            // If we have buffered rows, emit next one
            if let Some(item) = self.rows.pop_front() {
                let new_row = self.item_to_row(&item)?;

                // Skip offset rows
                self.row_cnt += 1;
                if let Some(ref lim) = self.limit.clone() {
                    if self.row_cnt <= lim.offset {
                        continue;
                    }
                }

                row.replace_with(new_row);
                stats::inc_stats(Self::FDW_NAME, stats::Metric::RowsIn, 1);
                return Ok(Some(()));
            }

            // No buffered rows; if exhausted, we're done
            if self.exhausted {
                return Ok(None);
            }

            // Fetch next page
            let (key_cond, filter_expr, names, values, use_query) = self.build_expressions();
            self.scan_started = true;

            if use_query {
                let key_cond_str = key_cond.unwrap_or_default();
                self.fetch_query_page(&key_cond_str, &filter_expr, &names, &values)?;
            } else {
                self.fetch_scan_page(&filter_expr, &names, &values)?;
            }
        }
    }

    fn re_scan(&mut self) -> DynamoDbFdwResult<()> {
        self.scan_started = false;
        self.rows.clear();
        self.last_key = None;
        self.exhausted = false;
        self.row_cnt = 0;
        Ok(())
    }

    fn end_scan(&mut self) -> DynamoDbFdwResult<()> {
        self.rows.clear();
        Ok(())
    }

    fn begin_modify(&mut self, options: &HashMap<String, String>) -> DynamoDbFdwResult<()> {
        self.table_name = require_option("table", options)?.to_string();
        let (pk, sk) = self.describe_table(&self.table_name.clone())?;
        self.partition_key = pk;
        self.sort_key = sk;
        Ok(())
    }

    fn insert(&mut self, src: &Row) -> DynamoDbFdwResult<()> {
        let mut item: HashMap<String, AttributeValue> = HashMap::new();
        for (col_name, cell) in src.iter() {
            item.insert(col_name.clone(), cell_to_attr(cell)?);
        }

        self.rt
            .block_on(
                self.client
                    .put_item()
                    .table_name(&self.table_name)
                    .set_item(Some(item))
                    .send(),
            )
            .map_err(DynamoDbFdwError::from)?;

        stats::inc_stats(Self::FDW_NAME, stats::Metric::RowsOut, 1);
        Ok(())
    }

    fn update(&mut self, rowid: &Cell, new_row: &Row) -> DynamoDbFdwResult<()> {
        // Build key map (partition key from rowid)
        let mut key: HashMap<String, AttributeValue> = HashMap::new();
        key.insert(
            self.partition_key.clone(),
            cell_to_attr(&Some(rowid.clone()))?,
        );

        // Build UpdateExpression for all non-key columns
        let mut names: HashMap<String, String> = HashMap::new();
        let mut values: HashMap<String, AttributeValue> = HashMap::new();
        let mut set_clauses: Vec<String> = Vec::new();
        let mut idx: usize = 0;

        for (col_name, cell) in new_row.iter() {
            // Skip key columns — they cannot be updated
            if *col_name == self.partition_key {
                continue;
            }
            if self.sort_key.as_deref() == Some(col_name.as_str()) {
                continue;
            }

            let name_ph = format!("#n{idx}");
            let val_ph = format!(":v{idx}");
            names.insert(name_ph.clone(), col_name.clone());
            values.insert(val_ph.clone(), cell_to_attr(cell)?);
            set_clauses.push(format!("{name_ph} = {val_ph}"));
            idx += 1;
        }

        if set_clauses.is_empty() {
            return Ok(());
        }

        let update_expr = format!("SET {}", set_clauses.join(", "));

        self.rt
            .block_on(
                self.client
                    .update_item()
                    .table_name(&self.table_name)
                    .set_key(Some(key))
                    .update_expression(&update_expr)
                    .set_expression_attribute_names(Some(names))
                    .set_expression_attribute_values(Some(values))
                    .send(),
            )
            .map_err(DynamoDbFdwError::from)?;

        stats::inc_stats(Self::FDW_NAME, stats::Metric::RowsOut, 1);
        Ok(())
    }

    fn delete(&mut self, rowid: &Cell) -> DynamoDbFdwResult<()> {
        let mut key: HashMap<String, AttributeValue> = HashMap::new();
        key.insert(
            self.partition_key.clone(),
            cell_to_attr(&Some(rowid.clone()))?,
        );

        self.rt
            .block_on(
                self.client
                    .delete_item()
                    .table_name(&self.table_name)
                    .set_key(Some(key))
                    .send(),
            )
            .map_err(DynamoDbFdwError::from)?;

        stats::inc_stats(Self::FDW_NAME, stats::Metric::RowsOut, 1);
        Ok(())
    }

    fn end_modify(&mut self) -> DynamoDbFdwResult<()> {
        Ok(())
    }

    fn import_foreign_schema(
        &mut self,
        stmt: ImportForeignSchemaStmt,
    ) -> DynamoDbFdwResult<Vec<String>> {
        // List all DynamoDB tables (paginated)
        let mut table_names: Vec<String> = Vec::new();
        let mut last_name: Option<String> = None;

        loop {
            let mut req = self.client.list_tables();
            if let Some(ref name) = last_name {
                req = req.exclusive_start_table_name(name);
            }

            let resp = self
                .rt
                .block_on(req.send())
                .map_err(DynamoDbFdwError::from)?;

            table_names.extend(resp.table_names.unwrap_or_default());
            last_name = resp.last_evaluated_table_name;
            if last_name.is_none() {
                break;
            }
        }

        // Apply ImportSchemaType filter
        let filtered: Vec<String> = match stmt.list_type {
            ImportSchemaType::FdwImportSchemaLimitTo => table_names
                .into_iter()
                .filter(|t| stmt.table_list.contains(t))
                .collect(),
            ImportSchemaType::FdwImportSchemaExcept => table_names
                .into_iter()
                .filter(|t| !stmt.table_list.contains(t))
                .collect(),
            ImportSchemaType::FdwImportSchemaAll => table_names,
        };

        let mut ddls: Vec<String> = Vec::new();

        for table_name in filtered {
            let (pk, sk) = self.describe_table(&table_name)?;

            // PostgreSQL identifier safe name
            let pg_table_name = table_name.replace('-', "_");

            // Build column list: key columns + _attrs jsonb catch-all
            let mut cols = vec![format!("{pk} text not null")];
            if let Some(ref sk_name) = sk {
                cols.push(format!("{sk_name} text"));
            }
            cols.push("_attrs jsonb".to_string());

            let ddl = format!(
                r#"create foreign table if not exists {pg_table_name} ({})
                server {} options (
                    table '{table_name}',
                    rowid_column '{pk}'
                )"#,
                cols.join(",\n"),
                stmt.server_name
            );
            ddls.push(ddl);
        }

        Ok(ddls)
    }

    fn validator(
        options: Vec<Option<String>>,
        catalog: Option<pg_sys::Oid>,
    ) -> DynamoDbFdwResult<()> {
        if let Some(oid) = catalog {
            if oid == FOREIGN_SERVER_RELATION_ID {
                // Must have either (aws_access_key_id + aws_secret_access_key)
                // or (vault_access_key_id + vault_secret_access_key).
                let has = |key: &str| check_options_contain(&options, key).is_ok();

                let direct_id = has("aws_access_key_id");
                let direct_secret = has("aws_secret_access_key");
                let vault_id = has("vault_access_key_id");
                let vault_secret = has("vault_secret_access_key");

                // Reject partial direct credentials with a precise missing-key error.
                match (direct_id, direct_secret) {
                    (true, false) => check_options_contain(&options, "aws_secret_access_key")?,
                    (false, true) => check_options_contain(&options, "aws_access_key_id")?,
                    _ => {}
                }

                // Reject partial vault credentials with a precise missing-key error.
                match (vault_id, vault_secret) {
                    (true, false) => check_options_contain(&options, "vault_secret_access_key")?,
                    (false, true) => check_options_contain(&options, "vault_access_key_id")?,
                    _ => {}
                }

                let has_direct = direct_id && direct_secret;
                let has_vault = vault_id && vault_secret;
                if !has_direct && !has_vault {
                    return Err(DynamoDbFdwError::InvalidServerOptions(
                        "must provide either (aws_access_key_id + aws_secret_access_key) or (vault_access_key_id + vault_secret_access_key)".to_string(),
                    ));
                }
            } else if oid == FOREIGN_TABLE_RELATION_ID {
                check_options_contain(&options, "table")?;
            }
        }
        Ok(())
    }
}
