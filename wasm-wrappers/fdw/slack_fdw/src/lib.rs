#[allow(warnings)]
mod bindings;
use serde_json::Value as JsonValue;

// Slack FDW implementation modules
pub mod models;
mod api;

use bindings::{
    exports::supabase::wrappers::routines::Guest,
    supabase::wrappers::{
        http, stats, time,
        types::{Cell, Context, FdwError, FdwResult, OptionsType, Row, Value},
        utils,
    },
};

use models::User;

#[derive(Debug, Default)]
struct SlackFdw {
    // Connection state
    api_token: String,
    headers: Vec<(String, String)>,
    workspace: Option<String>,
    
    // Request state for pagination
    resource: String,
    has_more: bool,
    next_cursor: Option<String>,
    
    // Cache for API responses
    users: Vec<User>,
    
    // Current position in result set
    result_index: usize,
}

static mut INSTANCE: *mut SlackFdw = std::ptr::null_mut::<SlackFdw>();
static FDW_NAME: &str = "SlackFdw";

// max number of rows returned per request
static BATCH_SIZE: u32 = 100;

impl SlackFdw {
    fn init() {
        let instance = Self::default();
        unsafe {
            INSTANCE = Box::leak(Box::new(instance));
        }
    }

    fn this_mut() -> &'static mut Self {
        unsafe { &mut (*INSTANCE) }
    }
    
    // This was left as a placeholder - in production we don't use the SlackClient
    // but directly make requests via the HTTP interface
    
    // Map Slack User to PostgreSQL Row
    fn user_to_row(&self, user: &User, row: &Row) -> Result<(), FdwError> {
        // id
        row.push(Some(&Cell::String(user.id.clone())));
        
        // name
        row.push(Some(&Cell::String(user.name.clone())));
        
        // real_name
        if let Some(real_name) = &user.real_name {
            row.push(Some(&Cell::String(real_name.clone())));
        } else {
            row.push(None);
        }
        
        // email
        if let Some(email) = &user.profile.email {
            row.push(Some(&Cell::String(email.clone())));
        } else {
            row.push(None);
        }
        
        // is_admin
        if let Some(is_admin) = user.is_admin {
            row.push(Some(&Cell::Bool(is_admin)));
        } else {
            row.push(None);
        }
        
        // is_bot
        row.push(Some(&Cell::Bool(user.is_bot)));
        
        Ok(())
    }
    
    // Create a request for the Slack API
    fn create_request(&self, endpoint: &str, params: &[(String, String)]) -> Result<http::Request, FdwError> {
        let mut url = format!("https://slack.com/api/{}", endpoint);
        
        // Add query parameters if any
        if !params.is_empty() {
            let query_string = params.iter()
                .map(|(k, v)| format!("{}={}", k, v))
                .collect::<Vec<String>>()
                .join("&");
            url = format!("{}?{}", url, query_string);
        }
        
        Ok(http::Request {
            method: http::Method::Get,
            url,
            headers: self.headers.clone(),
            body: String::default(),
        })
    }
    
    // Make a request to the Slack API with retries for rate limiting
    fn make_request(&self, req: &http::Request) -> Result<JsonValue, FdwError> {
        loop {
            let resp = match req.method {
                http::Method::Get => http::get(req)?,
                _ => unreachable!("invalid request method"),
            };
            
            // Handle rate limiting
            if resp.status_code == 429 {
                if let Some(retry) = resp.headers.iter().find(|h| h.0 == "retry-after") {
                    let delay_secs = retry.1.parse::<u64>().map_err(|e| e.to_string())?;
                    time::sleep(delay_secs * 1000);
                    continue;
                }
            }
            
            // Check for errors
            http::error_for_status(&resp).map_err(|err| format!("{}: {}", err, resp.body))?;
            
            // Transform response to JSON
            let resp_json: JsonValue = serde_json::from_str(&resp.body).map_err(|e| e.to_string())?;
            
            stats::inc_stats(FDW_NAME, stats::Metric::BytesIn, resp.body.len() as i64);
            
            // Check for Slack API errors
            if let Some(ok) = resp_json.get("ok") {
                if !ok.as_bool().unwrap_or(false) {
                    let error = resp_json.get("error").and_then(|e| e.as_str()).unwrap_or("Unknown error");
                    return Err(format!("Slack API error: {}", error));
                }
            }
            
            return Ok(resp_json);
        }
    }
    
    // Fetch users
    fn fetch_users(&mut self, ctx: &Context) -> FdwResult {
        // Create request parameters
        let mut params = vec![
            ("limit".to_string(), BATCH_SIZE.to_string()),
        ];
        
        // Add cursor if available
        if let Some(cursor) = &self.next_cursor {
            params.push(("cursor".to_string(), cursor.clone()));
        }
        
        // Create request and send it
        let req = self.create_request("users.list", &params)?;
        let resp_json = self.make_request(&req)?;
        
        // Extract users and pagination info
        if let Some(members) = resp_json.get("members").and_then(|m| m.as_array()) {
            // Convert JSON users to our model
            let mut users = members.iter()
                .filter_map(|m| serde_json::from_value(m.clone()).ok())
                .collect::<Vec<User>>();
            
            // Filter users based on quals if needed
            let quals = ctx.get_quals();
            if !quals.is_empty() {
                users.retain(|user| {
                    for qual in quals.iter() {
                        match (qual.field().as_str(), qual.operator().as_str()) {
                            ("is_admin", "=") => {
                                if let Value::Cell(Cell::Bool(is_admin)) = qual.value() {
                                    if user.is_admin != Some(is_admin) {
                                        return false;
                                    }
                                }
                            },
                            ("is_bot", "=") => {
                                if let Value::Cell(Cell::Bool(is_bot)) = qual.value() {
                                    if user.is_bot != is_bot {
                                        return false;
                                    }
                                }
                            },
                            ("name", "=") => {
                                if let Value::Cell(Cell::String(name)) = qual.value() {
                                    if user.name != name {
                                        return false;
                                    }
                                }
                            },
                            _ => {}
                        }
                    }
                    true
                });
            }
            
            // Save filtered users
            self.users = users;
            
            // Get pagination info
            self.next_cursor = resp_json
                .get("response_metadata")
                .and_then(|m| m.get("next_cursor"))
                .and_then(|c| c.as_str())
                .map(|s| s.to_string());
            
            // Reset position
            self.result_index = 0;
            
            Ok(())
        } else {
            Err("Failed to parse users from response".to_string())
        }
    }
}

impl Guest for SlackFdw {
    fn host_version_requirement() -> String {
        // semver ref: https://docs.rs/semver/latest/semver/enum.Op.html
        "^0.1.0".to_string()
    }

    fn init(ctx: &Context) -> FdwResult {
        Self::init();
        let this = Self::this_mut();

        // Get foreign server options
        let opts = ctx.get_options(OptionsType::Server);
        
        // Get API token from options or vault
        let api_token = match opts.get("api_token") {
            Some(token) => token,
            None => {
                let token_id = opts.require("api_token_id")?;
                utils::get_vault_secret(&token_id).unwrap_or_default()
            }
        };
        
        // Validate token format
        if !api_token.starts_with("xoxp-") && !api_token.starts_with("xoxb-") {
            return Err("Invalid api_token format. Expected an OAuth token starting with 'xoxp-' or 'xoxb-'".to_string());
        }
        
        // Get optional parameters
        let workspace = opts.get("workspace");
        
        // Set up authorization headers
        this.headers.push(("user-agent".to_owned(), "Wrappers Slack FDW".to_string()));
        this.headers.push(("content-type".to_owned(), "application/json".to_string()));
        this.headers.push(("authorization".to_owned(), format!("Bearer {}", api_token)));
        
        // Store options in the instance
        this.api_token = api_token;
        this.workspace = workspace;
        
        stats::inc_stats(FDW_NAME, stats::Metric::CreateTimes, 1);
        
        Ok(())
    }

    fn begin_scan(ctx: &Context) -> FdwResult {
        let this = Self::this_mut();
        
        // Get resource from table options
        let opts = ctx.get_options(OptionsType::Table);
        let resource = opts.require("resource")?;
        
        // Reset pagination state
        this.resource = resource.clone();
        this.has_more = false;
        this.next_cursor = None;
        this.result_index = 0;
        
        // Verify that the resource is 'users'
        if resource.as_str() != "users" {
            return Err(format!("Unsupported resource type: {}. Currently only 'users' is supported.", resource));
        }
        
        // Fetch users data
        this.fetch_users(ctx)
    }

    fn iter_scan(ctx: &Context, row: &Row) -> Result<Option<u32>, FdwError> {
        let this = Self::this_mut();
        
        // If we've reached the end of our current batch
        if this.result_index >= this.users.len() {
            // Record metrics
            stats::inc_stats(FDW_NAME, stats::Metric::RowsIn, this.users.len() as i64);
            stats::inc_stats(FDW_NAME, stats::Metric::RowsOut, this.users.len() as i64);
            
            // If there's a next cursor, fetch the next batch
            if this.next_cursor.is_some() {
                this.fetch_users(ctx)?;
                
                // If the new batch is empty, we're done
                if this.users.is_empty() {
                    return Ok(None);
                }
            } else {
                // No more results
                return Ok(None);
            }
        }
        
        // Get the user from the current batch
        let user = &this.users[this.result_index];
        
        // Convert user to row
        this.user_to_row(user, row)?;
        
        this.result_index += 1;
        Ok(Some(0))
    }

    fn re_scan(ctx: &Context) -> FdwResult {
        let this = Self::this_mut();
        
        // Reset pagination state
        this.has_more = false;
        this.next_cursor = None;
        this.result_index = 0;
        
        // Re-fetch users data
        this.fetch_users(ctx)
    }

    fn end_scan(_ctx: &Context) -> FdwResult {
        let this = Self::this_mut();
        
        // Clear cached users data
        this.users.clear();
        
        Ok(())
    }

    fn begin_modify(_ctx: &Context) -> FdwResult {
        Err("Slack FDW is read-only".to_string())
    }

    fn insert(_ctx: &Context, _row: &Row) -> FdwResult {
        Err("Slack FDW is read-only".to_string())
    }

    fn update(_ctx: &Context, _rowid: Cell, _row: &Row) -> FdwResult {
        Err("Slack FDW is read-only".to_string())
    }

    fn delete(_ctx: &Context, _rowid: Cell) -> FdwResult {
        Err("Slack FDW is read-only".to_string())
    }

    fn end_modify(_ctx: &Context) -> FdwResult {
        Err("Slack FDW is read-only".to_string())
    }
}

bindings::export!(SlackFdw with_types_in bindings);