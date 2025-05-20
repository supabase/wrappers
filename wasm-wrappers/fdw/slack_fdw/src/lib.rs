//! Slack Foreign Data Wrapper for PostgreSQL
//!
//! This module implements a Slack FDW that allows querying Slack workspaces directly from PostgreSQL.
//! It supports various resource types including users, user groups, messages, channels, files, and team info.
//! The FDW leverages the Slack API to fetch data and supports query pushdown for filtering and sorting.
//!
//! Resources supported:
//! - users: Query workspace users with filtering and sorting
//! - usergroups: Query user groups with filtering and sorting
//! - usergroup_members: Query membership relationships between users and groups
//! - messages: Query messages from channels (requires channel_id)
//! - channels: Query public and private channels
//! - files: Query files shared in the workspace
//! - team-info: Query information about the workspace

#[allow(warnings)]
mod bindings;
use serde_json::Value as JsonValue;

// Slack FDW implementation modules
#[cfg(test)]
mod api;
pub mod models;

use bindings::{
    exports::supabase::wrappers::routines::Guest,
    supabase::wrappers::{
        http, stats, time,
        types::{
            Cell, Context, FdwError, FdwResult, ImportForeignSchemaStmt, Limit, OptionsType, Row,
            Sort, Value,
        },
        utils,
    },
};

// Import all model types needed for the FDW
use models::{Channel, File, Message, TeamInfo, User, UserGroup, UserGroupMembership};

/// Slack Foreign Data Wrapper implementation
///
/// This struct encapsulates all the functionality to interact with the Slack API,
/// translate Slack data to PostgreSQL rows, and handle query pushdown.
#[derive(Debug, Default)]
struct SlackFdw {
    // Connection state
    /// Slack API token (xoxb-* or xoxp-*)
    api_token: String,
    /// HTTP headers to send with all requests
    headers: Vec<(String, String)>,
    /// Optional workspace name for logging
    workspace: Option<String>,

    // Request state for pagination
    /// Current resource type being accessed (users, channels, messages, etc.)
    resource: String,
    /// Whether there are more results available in the API
    has_more: bool,
    /// Pagination cursor for fetching the next batch of results
    next_cursor: Option<String>,

    // Cache for API responses
    /// Cached users from the workspace
    users: Vec<User>,
    /// Cached user groups from the workspace
    user_groups: Vec<UserGroup>,
    /// Cached user group memberships
    user_group_memberships: Vec<UserGroupMembership>,
    /// Cached messages from channels
    messages: Vec<Message>,
    /// Cached channels from the workspace
    channels: Vec<Channel>,
    /// Cached files from the workspace
    files: Vec<File>,
    /// Cached team info (workspace info)
    team_info: Option<TeamInfo>,

    /// Current channel ID being queried (for messages)
    current_channel_id: Option<String>,

    /// Current position in the result set for iteration
    result_index: usize,

    // Query pushdown support
    /// Sorting criteria for query pushdown
    sorts: Vec<Sort>,
    /// Limit and offset for query pushdown
    limit: Option<Limit>,
}

/// Global instance of the FDW as required by the PostgreSQL FDW API
static mut INSTANCE: *mut SlackFdw = std::ptr::null_mut::<SlackFdw>();

/// Name of the FDW for logging and metrics
static FDW_NAME: &str = "SlackFdw";

/// Maximum number of rows returned per API request for pagination
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

    // Map Slack UserGroup to PostgreSQL Row
    fn usergroup_to_row(&self, usergroup: &UserGroup, row: &Row) -> Result<(), FdwError> {
        // Basic information
        row.push(Some(&Cell::String(usergroup.id.clone())));
        row.push(Some(&Cell::String(usergroup.team_id.clone())));
        row.push(Some(&Cell::String(usergroup.name.clone())));
        row.push(Some(&Cell::String(usergroup.handle.clone())));

        // Optional fields
        if let Some(description) = &usergroup.description {
            row.push(Some(&Cell::String(description.clone())));
        } else {
            row.push(None);
        }

        if let Some(is_external) = usergroup.is_external {
            row.push(Some(&Cell::Bool(is_external)));
        } else {
            row.push(None);
        }

        // Timestamps
        row.push(Some(&Cell::I64(usergroup.date_create)));
        row.push(Some(&Cell::I64(usergroup.date_update)));

        if let Some(date_delete) = usergroup.date_delete {
            row.push(Some(&Cell::I64(date_delete)));
        } else {
            row.push(None);
        }

        // Auto type
        if let Some(auto_type) = &usergroup.auto_type {
            row.push(Some(&Cell::String(auto_type.clone())));
        } else {
            row.push(None);
        }

        // User information
        row.push(Some(&Cell::String(usergroup.created_by.clone())));

        if let Some(updated_by) = &usergroup.updated_by {
            row.push(Some(&Cell::String(updated_by.clone())));
        } else {
            row.push(None);
        }

        if let Some(deleted_by) = &usergroup.deleted_by {
            row.push(Some(&Cell::String(deleted_by.clone())));
        } else {
            row.push(None);
        }

        // Counts
        if let Some(user_count) = usergroup.user_count {
            row.push(Some(&Cell::I32(user_count)));
        } else {
            row.push(None);
        }

        if let Some(channel_count) = usergroup.channel_count {
            row.push(Some(&Cell::I32(channel_count)));
        } else {
            row.push(None);
        }

        Ok(())
    }

    // Map UserGroupMembership to PostgreSQL Row
    fn usergroup_membership_to_row(
        &self,
        membership: &UserGroupMembership,
        row: &Row,
    ) -> Result<(), FdwError> {
        row.push(Some(&Cell::String(membership.usergroup_id.clone())));
        row.push(Some(&Cell::String(membership.usergroup_name.clone())));
        row.push(Some(&Cell::String(membership.usergroup_handle.clone())));
        row.push(Some(&Cell::String(membership.user_id.clone())));
        Ok(())
    }

    // Map Slack Message to PostgreSQL Row
    fn message_to_row(&self, message: &Message, row: &Row) -> Result<(), FdwError> {
        // Get channel_id
        let channel_id = self.current_channel_id.clone().unwrap_or_default();

        // Basic information
        row.push(Some(&Cell::String(message.ts.clone())));

        if let Some(user_id) = &message.user {
            row.push(Some(&Cell::String(user_id.clone())));
        } else {
            row.push(None);
        }

        row.push(Some(&Cell::String(channel_id)));
        row.push(Some(&Cell::String(message.text.clone())));

        if let Some(thread_ts) = &message.thread_ts {
            row.push(Some(&Cell::String(thread_ts.clone())));
        } else {
            row.push(None);
        }

        if let Some(reply_count) = message.reply_count {
            row.push(Some(&Cell::I32(reply_count)));
        } else {
            row.push(None);
        }

        Ok(())
    }

    // Map Slack Channel to PostgreSQL Row
    fn channel_to_row(&self, channel: &Channel, row: &Row) -> Result<(), FdwError> {
        row.push(Some(&Cell::String(channel.id.clone())));
        row.push(Some(&Cell::String(channel.name.clone())));
        row.push(Some(&Cell::Bool(channel.is_private)));

        // Convert unix timestamp to PostgreSQL timestamp
        row.push(Some(&Cell::I64(channel.created)));

        row.push(Some(&Cell::String(channel.creator.clone())));

        Ok(())
    }

    // Map Slack File to PostgreSQL Row
    fn file_to_row(&self, file: &File, row: &Row) -> Result<(), FdwError> {
        row.push(Some(&Cell::String(file.id.clone())));
        row.push(Some(&Cell::String(file.name.clone())));
        row.push(Some(&Cell::String(file.title.clone())));
        row.push(Some(&Cell::String(file.mimetype.clone())));
        row.push(Some(&Cell::I64(file.size)));

        if let Some(url) = &file.url_private {
            row.push(Some(&Cell::String(url.clone())));
        } else {
            row.push(None);
        }

        row.push(Some(&Cell::String(file.user.clone())));

        // Convert unix timestamp to PostgreSQL timestamp
        row.push(Some(&Cell::I64(file.created)));

        Ok(())
    }

    // Map Slack TeamInfo to PostgreSQL Row
    fn team_info_to_row(&self, team_info: &TeamInfo, row: &Row) -> Result<(), FdwError> {
        row.push(Some(&Cell::String(team_info.id.clone())));
        row.push(Some(&Cell::String(team_info.name.clone())));
        row.push(Some(&Cell::String(team_info.domain.clone())));
        row.push(Some(&Cell::String(team_info.email_domain.clone())));

        Ok(())
    }

    // Map Slack User to PostgreSQL Row
    fn user_to_row(&self, user: &User, row: &Row) -> Result<(), FdwError> {
        // Basic information
        row.push(Some(&Cell::String(user.id.clone()))); // id
        row.push(Some(&Cell::String(user.name.clone()))); // name

        // Name and profile fields
        if let Some(real_name) = &user.real_name {
            row.push(Some(&Cell::String(real_name.clone())));
        } else {
            row.push(None);
        }

        if let Some(display_name) = &user.profile.display_name {
            row.push(Some(&Cell::String(display_name.clone())));
        } else {
            row.push(None);
        }

        if let Some(display_name_normalized) = &user.profile.display_name_normalized {
            row.push(Some(&Cell::String(display_name_normalized.clone())));
        } else {
            row.push(None);
        }

        if let Some(real_name_normalized) = &user.profile.real_name_normalized {
            row.push(Some(&Cell::String(real_name_normalized.clone())));
        } else {
            row.push(None);
        }

        // Contact information
        if let Some(email) = &user.profile.email {
            row.push(Some(&Cell::String(email.clone())));
        } else {
            row.push(None);
        }

        if let Some(phone) = &user.profile.phone {
            row.push(Some(&Cell::String(phone.clone())));
        } else {
            row.push(None);
        }

        if let Some(skype) = &user.profile.skype {
            row.push(Some(&Cell::String(skype.clone())));
        } else {
            row.push(None);
        }

        // Role information
        if let Some(is_admin) = user.is_admin {
            row.push(Some(&Cell::Bool(is_admin)));
        } else {
            row.push(None);
        }

        if let Some(is_owner) = user.is_owner {
            row.push(Some(&Cell::Bool(is_owner)));
        } else {
            row.push(None);
        }

        if let Some(is_primary_owner) = user.is_primary_owner {
            row.push(Some(&Cell::Bool(is_primary_owner)));
        } else {
            row.push(None);
        }

        row.push(Some(&Cell::Bool(user.is_bot))); // is_bot

        if let Some(is_app_user) = user.is_app_user {
            row.push(Some(&Cell::Bool(is_app_user)));
        } else {
            row.push(None);
        }

        if let Some(is_restricted) = user.is_restricted {
            row.push(Some(&Cell::Bool(is_restricted)));
        } else {
            row.push(None);
        }

        if let Some(is_ultra_restricted) = user.is_ultra_restricted {
            row.push(Some(&Cell::Bool(is_ultra_restricted)));
        } else {
            row.push(None);
        }

        row.push(Some(&Cell::Bool(user.deleted))); // deleted

        // Status information
        if let Some(status_text) = &user.profile.status_text {
            row.push(Some(&Cell::String(status_text.clone())));
        } else {
            row.push(None);
        }

        if let Some(status_emoji) = &user.profile.status_emoji {
            row.push(Some(&Cell::String(status_emoji.clone())));
        } else {
            row.push(None);
        }

        if let Some(status_expiration) = user.profile.status_expiration {
            row.push(Some(&Cell::I64(status_expiration)));
        } else {
            row.push(None);
        }

        if let Some(title) = &user.profile.title {
            row.push(Some(&Cell::String(title.clone())));
        } else {
            row.push(None);
        }

        // Team information
        if let Some(team_id) = &user.team_id {
            row.push(Some(&Cell::String(team_id.clone())));
        } else {
            row.push(None);
        }

        if let Some(team) = &user.profile.team {
            row.push(Some(&Cell::String(team.clone())));
        } else {
            row.push(None);
        }

        // Time zone information
        if let Some(tz) = &user.tz {
            row.push(Some(&Cell::String(tz.clone())));
        } else {
            row.push(None);
        }

        if let Some(tz_label) = &user.tz_label {
            row.push(Some(&Cell::String(tz_label.clone())));
        } else {
            row.push(None);
        }

        if let Some(tz_offset) = user.tz_offset {
            row.push(Some(&Cell::I32(tz_offset)));
        } else {
            row.push(None);
        }

        if let Some(locale) = &user.locale {
            row.push(Some(&Cell::String(locale.clone())));
        } else {
            row.push(None);
        }

        // Avatar/image URLs
        if let Some(image_24) = &user.profile.image_24 {
            row.push(Some(&Cell::String(image_24.clone())));
        } else {
            row.push(None);
        }

        if let Some(image_48) = &user.profile.image_48 {
            row.push(Some(&Cell::String(image_48.clone())));
        } else {
            row.push(None);
        }

        if let Some(image_72) = &user.profile.image_72 {
            row.push(Some(&Cell::String(image_72.clone())));
        } else {
            row.push(None);
        }

        if let Some(image_192) = &user.profile.image_192 {
            row.push(Some(&Cell::String(image_192.clone())));
        } else {
            row.push(None);
        }

        if let Some(image_512) = &user.profile.image_512 {
            row.push(Some(&Cell::String(image_512.clone())));
        } else {
            row.push(None);
        }

        // Miscellaneous
        if let Some(color) = &user.color {
            row.push(Some(&Cell::String(color.clone())));
        } else {
            row.push(None);
        }

        if let Some(updated) = user.updated {
            row.push(Some(&Cell::I64(updated)));
        } else {
            row.push(None);
        }

        Ok(())
    }

    // Create a request for the Slack API
    fn create_request(
        &self,
        endpoint: &str,
        params: &[(String, String)],
    ) -> Result<http::Request, FdwError> {
        let mut url = format!("https://slack.com/api/{}", endpoint);

        // Add query parameters if any
        if !params.is_empty() {
            let query_string = params
                .iter()
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
            let resp_json: JsonValue =
                serde_json::from_str(&resp.body).map_err(|e| e.to_string())?;

            stats::inc_stats(FDW_NAME, stats::Metric::BytesIn, resp.body.len() as i64);

            // Check for Slack API errors
            if let Some(ok) = resp_json.get("ok") {
                if !ok.as_bool().unwrap_or(false) {
                    let error = resp_json
                        .get("error")
                        .and_then(|e| e.as_str())
                        .unwrap_or("Unknown error");
                    return Err(format!("Slack API error: {}", error));
                }
            }

            return Ok(resp_json);
        }
    }

    // Fetch user groups
    fn fetch_user_groups(&mut self, ctx: &Context) -> FdwResult {
        // Create request parameters
        let mut params = vec![("include_users".to_string(), "true".to_string())];

        // Push down WHERE filters if possible
        let quals = ctx.get_quals();
        if !quals.is_empty() {
            for qual in quals.iter() {
                if qual.operator().as_str() == "=" && !qual.use_or() {
                    match qual.field().as_str() {
                        "team_id" => {
                            if let Value::Cell(Cell::String(team_id)) = qual.value() {
                                params.push(("team_id".to_string(), team_id.clone()));
                            }
                        }
                        "include_disabled" => {
                            if let Value::Cell(Cell::Bool(include_disabled)) = qual.value() {
                                params.push((
                                    "include_disabled".to_string(),
                                    include_disabled.to_string(),
                                ));
                            }
                        }
                        _ => {}
                    }
                }
            }
        }

        // Create request and send it
        let req = self.create_request("usergroups.list", &params)?;
        let resp_json = self.make_request(&req)?;

        // Extract user groups
        if let Some(usergroups) = resp_json.get("usergroups").and_then(|g| g.as_array()) {
            // Convert JSON user groups to our model
            self.user_groups = usergroups
                .iter()
                .filter_map(|g| serde_json::from_value(g.clone()).ok())
                .collect::<Vec<UserGroup>>();

            // Apply sorting if requested
            if !self.sorts.is_empty() {
                self.user_groups.sort_by(|a, b| {
                    for sort in &self.sorts {
                        match sort.field().as_str() {
                            "name" => {
                                let ordering = a.name.cmp(&b.name);
                                if sort.reversed() {
                                    return ordering.reverse();
                                }
                                if ordering != std::cmp::Ordering::Equal {
                                    return ordering;
                                }
                            }
                            "handle" => {
                                let ordering = a.handle.cmp(&b.handle);
                                if sort.reversed() {
                                    return ordering.reverse();
                                }
                                if ordering != std::cmp::Ordering::Equal {
                                    return ordering;
                                }
                            }
                            "date_create" => {
                                let ordering = a.date_create.cmp(&b.date_create);
                                if sort.reversed() {
                                    return ordering.reverse();
                                }
                                if ordering != std::cmp::Ordering::Equal {
                                    return ordering;
                                }
                            }
                            "date_update" => {
                                let ordering = a.date_update.cmp(&b.date_update);
                                if sort.reversed() {
                                    return ordering.reverse();
                                }
                                if ordering != std::cmp::Ordering::Equal {
                                    return ordering;
                                }
                            }
                            _ => {}
                        }
                    }
                    std::cmp::Ordering::Equal
                });
            }

            // Apply LIMIT and OFFSET if specified
            if let Some(limit) = &self.limit {
                let start = limit.offset() as usize;
                let end = (limit.offset() + limit.count()) as usize;

                // Handle offset - trim the beginning of the results
                if start < self.user_groups.len() {
                    self.user_groups = self.user_groups[start..].to_vec();
                } else {
                    self.user_groups.clear();
                }

                // Handle count - trim the end of the results if needed
                if self.user_groups.len() > end - start {
                    self.user_groups.truncate(end - start);
                }
            }

            // Reset position
            self.result_index = 0;

            Ok(())
        } else {
            Err("Failed to parse user groups from response".to_string())
        }
    }

    // Fetch user group memberships
    fn fetch_user_group_memberships(&mut self, ctx: &Context) -> FdwResult {
        self.user_group_memberships.clear();

        // Get all user groups first
        self.fetch_user_groups(ctx)?;

        // For each group that doesn't include users, fetch its members
        for group in &self.user_groups {
            if let Some(users) = &group.users {
                // If users are already included in the group response, use those
                for user_id in users {
                    self.user_group_memberships.push(UserGroupMembership {
                        usergroup_id: group.id.clone(),
                        usergroup_name: group.name.clone(),
                        usergroup_handle: group.handle.clone(),
                        user_id: user_id.clone(),
                    });
                }
            } else {
                // Otherwise, fetch the members
                let params = vec![("usergroup".to_string(), group.id.clone())];

                let req = self.create_request("usergroups.users.list", &params)?;
                let resp_json = self.make_request(&req)?;

                if let Some(users) = resp_json.get("users").and_then(|u| u.as_array()) {
                    for user_id in users {
                        if let Some(user_id) = user_id.as_str() {
                            self.user_group_memberships.push(UserGroupMembership {
                                usergroup_id: group.id.clone(),
                                usergroup_name: group.name.clone(),
                                usergroup_handle: group.handle.clone(),
                                user_id: user_id.to_string(),
                            });
                        }
                    }
                }
            }
        }

        // Apply LIMIT and OFFSET if specified
        if let Some(limit) = &self.limit {
            let start = limit.offset() as usize;
            let end = (limit.offset() + limit.count()) as usize;

            // Handle offset - trim the beginning of the results
            if start < self.user_group_memberships.len() {
                self.user_group_memberships = self.user_group_memberships[start..].to_vec();
            } else {
                self.user_group_memberships.clear();
            }

            // Handle count - trim the end of the results if needed
            if self.user_group_memberships.len() > end - start {
                self.user_group_memberships.truncate(end - start);
            }
        }

        // Reset position
        self.result_index = 0;

        Ok(())
    }

    // Fetch messages from a channel
    fn fetch_messages(&mut self, ctx: &Context) -> FdwResult {
        self.messages.clear();

        // Create request parameters
        let mut params = Vec::new();

        // Push down WHERE filters if possible
        let quals = ctx.get_quals();
        if !quals.is_empty() {
            for qual in quals.iter() {
                if qual.operator().as_str() == "=" && !qual.use_or() {
                    match qual.field().as_str() {
                        "channel_id" => {
                            if let Value::Cell(Cell::String(channel_id)) = qual.value() {
                                params.push(("channel".to_string(), channel_id.clone()));
                                self.current_channel_id = Some(channel_id.clone());
                            }
                        }
                        "oldest" => {
                            if let Value::Cell(Cell::String(oldest)) = qual.value() {
                                params.push(("oldest".to_string(), oldest.clone()));
                            }
                        }
                        "latest" => {
                            if let Value::Cell(Cell::String(latest)) = qual.value() {
                                params.push(("latest".to_string(), latest.clone()));
                            }
                        }
                        _ => {}
                    }
                }
            }
        }

        // If no channel_id is specified, we can't fetch messages
        if !params.iter().any(|(k, _)| k == "channel") {
            return Err("channel_id is required for querying messages".to_string());
        }

        // Create request and send it
        let req = self.create_request("conversations.history", &params)?;
        let resp_json = self.make_request(&req)?;

        if let Some(messages) = resp_json.get("messages").and_then(|m| m.as_array()) {
            // Convert JSON messages to our model
            self.messages = messages
                .iter()
                .filter_map(|m| serde_json::from_value(m.clone()).ok())
                .collect::<Vec<Message>>();

            // Get pagination info
            self.has_more = resp_json
                .get("has_more")
                .and_then(|h| h.as_bool())
                .unwrap_or(false);
            self.next_cursor = resp_json
                .get("response_metadata")
                .and_then(|m| m.get("next_cursor"))
                .and_then(|c| c.as_str())
                .map(|s| s.to_string());

            // Reset position
            self.result_index = 0;

            Ok(())
        } else {
            Err("Failed to parse messages from response".to_string())
        }
    }

    // Fetch channels
    fn fetch_channels(&mut self, ctx: &Context) -> FdwResult {
        self.channels.clear();

        // Create request parameters
        let mut params = Vec::new();

        // Push down WHERE filters if possible
        let quals = ctx.get_quals();
        if !quals.is_empty() {
            for qual in quals.iter() {
                if qual.operator().as_str() == "="
                    && !qual.use_or()
                    && qual.field().as_str() == "types"
                {
                    if let Value::Cell(Cell::String(types)) = qual.value() {
                        params.push(("types".to_string(), types.clone()));
                    }
                }
            }
        }

        // Add types if not specified (default to public channels)
        if !params.iter().any(|(k, _)| k == "types") {
            params.push(("types".to_string(), "public_channel".to_string()));
        }

        // Create request and send it
        let req = self.create_request("conversations.list", &params)?;
        let resp_json = self.make_request(&req)?;

        if let Some(channels) = resp_json.get("channels").and_then(|c| c.as_array()) {
            // Convert JSON channels to our model
            self.channels = channels
                .iter()
                .filter_map(|c| serde_json::from_value(c.clone()).ok())
                .collect::<Vec<Channel>>();

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
            Err("Failed to parse channels from response".to_string())
        }
    }

    // Fetch files
    fn fetch_files(&mut self, ctx: &Context) -> FdwResult {
        self.files.clear();

        // Create request parameters
        let mut params = Vec::new();

        // Push down WHERE filters if possible
        let quals = ctx.get_quals();
        if !quals.is_empty() {
            for qual in quals.iter() {
                if qual.operator().as_str() == "=" && !qual.use_or() {
                    match qual.field().as_str() {
                        "channel_id" => {
                            if let Value::Cell(Cell::String(channel_id)) = qual.value() {
                                params.push(("channel".to_string(), channel_id.clone()));
                            }
                        }
                        "user_id" => {
                            if let Value::Cell(Cell::String(user_id)) = qual.value() {
                                params.push(("user".to_string(), user_id.clone()));
                            }
                        }
                        "ts_from" => {
                            if let Value::Cell(Cell::String(ts_from)) = qual.value() {
                                params.push(("ts_from".to_string(), ts_from.clone()));
                            }
                        }
                        "ts_to" => {
                            if let Value::Cell(Cell::String(ts_to)) = qual.value() {
                                params.push(("ts_to".to_string(), ts_to.clone()));
                            }
                        }
                        _ => {}
                    }
                }
            }
        }

        // Add default count
        params.push(("count".to_string(), BATCH_SIZE.to_string()));

        // Create request and send it
        let req = self.create_request("files.list", &params)?;
        let resp_json = self.make_request(&req)?;

        if let Some(files) = resp_json.get("files").and_then(|f| f.as_array()) {
            // Convert JSON files to our model
            self.files = files
                .iter()
                .filter_map(|f| serde_json::from_value(f.clone()).ok())
                .collect::<Vec<File>>();

            // Reset position
            self.result_index = 0;

            Ok(())
        } else {
            Err("Failed to parse files from response".to_string())
        }
    }

    // Fetch team info
    fn fetch_team_info(&mut self, _ctx: &Context) -> FdwResult {
        // Create request and send it
        let req = self.create_request("team.info", &[])?;
        let resp_json = self.make_request(&req)?;

        if let Some(team) = resp_json.get("team") {
            // Convert JSON team info to our model
            self.team_info = serde_json::from_value(team.clone()).ok();

            // Reset position
            self.result_index = 0;

            Ok(())
        } else {
            Err("Failed to parse team info from response".to_string())
        }
    }

    // Fetch users
    fn fetch_users(&mut self, ctx: &Context) -> FdwResult {
        // Determine batch size based on limit if provided
        let batch_size = if let Some(limit) = &self.limit {
            // If there's a limit and it's smaller than our default batch size, use it
            if limit.count() > 0 && limit.count() < BATCH_SIZE as i64 {
                limit.count() as u32
            } else {
                BATCH_SIZE
            }
        } else {
            BATCH_SIZE
        };

        // Create request parameters
        let mut params = vec![("limit".to_string(), batch_size.to_string())];

        // Add cursor if available
        if let Some(cursor) = &self.next_cursor {
            params.push(("cursor".to_string(), cursor.clone()));
        }

        // Push down WHERE filters if possible
        let quals = ctx.get_quals();
        if !quals.is_empty() {
            for qual in quals.iter() {
                if qual.operator().as_str() == "=" && !qual.use_or() {
                    match qual.field().as_str() {
                        // Server-side filtering via the Slack API
                        "team_id" => {
                            if let Value::Cell(Cell::String(team_id)) = qual.value() {
                                params.push(("team_id".to_string(), team_id.clone()));
                            }
                        }
                        // Search by display name or real name
                        "name" => {
                            if let Value::Cell(Cell::String(name)) = qual.value() {
                                params.push(("query".to_string(), name.clone()));
                            }
                        }
                        // Search by email address
                        "email" => {
                            if let Value::Cell(Cell::String(email)) = qual.value() {
                                params.push(("email".to_string(), email.clone()));
                            }
                        }
                        _ => {}
                    }
                }
            }
        }

        // Create request and send it
        let req = self.create_request("users.list", &params)?;
        let resp_json = self.make_request(&req)?;

        // Extract users and pagination info
        if let Some(members) = resp_json.get("members").and_then(|m| m.as_array()) {
            // Convert JSON users to our model
            let mut users = members
                .iter()
                .filter_map(|m| serde_json::from_value(m.clone()).ok())
                .collect::<Vec<User>>();

            // No client-side filtering - rely solely on the API's filtering

            // Apply sorting if requested
            if !self.sorts.is_empty() {
                users.sort_by(|a, b| {
                    for sort in &self.sorts {
                        match sort.field().as_str() {
                            "name" => {
                                let ordering = a.name.cmp(&b.name);
                                if sort.reversed() {
                                    return ordering.reverse();
                                }
                                if ordering != std::cmp::Ordering::Equal {
                                    return ordering;
                                }
                            }
                            "real_name" => {
                                let empty_string = String::new();
                                let a_real_name = a.real_name.as_ref().unwrap_or(&empty_string);
                                let b_real_name = b.real_name.as_ref().unwrap_or(&empty_string);
                                let ordering = a_real_name.cmp(b_real_name);
                                if sort.reversed() {
                                    return ordering.reverse();
                                }
                                if ordering != std::cmp::Ordering::Equal {
                                    return ordering;
                                }
                            }
                            "email" => {
                                let empty_string = String::new();
                                let a_email = a.profile.email.as_ref().unwrap_or(&empty_string);
                                let b_email = b.profile.email.as_ref().unwrap_or(&empty_string);
                                let ordering = a_email.cmp(b_email);
                                if sort.reversed() {
                                    return ordering.reverse();
                                }
                                if ordering != std::cmp::Ordering::Equal {
                                    return ordering;
                                }
                            }
                            _ => {}
                        }
                    }
                    std::cmp::Ordering::Equal
                });
            }

            // Apply LIMIT and OFFSET if specified
            if let Some(limit) = &self.limit {
                let start = limit.offset() as usize;
                let end = (limit.offset() + limit.count()) as usize;

                // Handle offset - trim the beginning of the results
                if start < users.len() {
                    users = users[start..].to_vec();
                } else {
                    users.clear();
                }

                // Handle count - trim the end of the results if needed
                if users.len() > end - start {
                    users.truncate(end - start);
                }

                // If we've reached the requested limit, don't fetch more pages
                if users.len() >= (limit.count() as usize) {
                    self.next_cursor = None;
                }
            }

            // Save filtered and sorted users
            self.users = users;

            // Get pagination info (if we haven't already cleared it due to limit)
            if self.next_cursor.is_some() {
                self.next_cursor = resp_json
                    .get("response_metadata")
                    .and_then(|m| m.get("next_cursor"))
                    .and_then(|c| c.as_str())
                    .map(|s| s.to_string());
            }

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
        let opts = ctx.get_options(&OptionsType::Server);

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
        this.headers
            .push(("user-agent".to_owned(), "Wrappers Slack FDW".to_string()));
        this.headers
            .push(("content-type".to_owned(), "application/json".to_string()));
        this.headers
            .push(("authorization".to_owned(), format!("Bearer {}", api_token)));

        // Store options in the instance
        this.api_token = api_token;
        this.workspace = workspace;

        stats::inc_stats(FDW_NAME, stats::Metric::CreateTimes, 1);

        Ok(())
    }

    fn begin_scan(ctx: &Context) -> FdwResult {
        let this = Self::this_mut();

        // Get resource from table options
        let opts = ctx.get_options(&OptionsType::Table);
        let resource = opts.require("resource")?;

        // Reset pagination state
        this.resource = resource.clone();
        this.has_more = false;
        this.next_cursor = None;
        this.result_index = 0;
        this.current_channel_id = None;

        // Store the sort and limit information for query pushdown
        this.sorts = ctx.get_sorts();
        this.limit = ctx.get_limit();

        // Fetch the appropriate resource data
        match resource.as_str() {
            "users" => this.fetch_users(ctx),
            "usergroups" => this.fetch_user_groups(ctx),
            "usergroup_members" => this.fetch_user_group_memberships(ctx),
            "messages" => this.fetch_messages(ctx),
            "channels" => this.fetch_channels(ctx),
            "files" => this.fetch_files(ctx),
            "team-info" => this.fetch_team_info(ctx),
            _ => Err(format!(
                "Unsupported resource type: {}. Supported resources are 'users', 'usergroups', 'usergroup_members', 'messages', 'channels', 'files', and 'team-info'.",
                resource
            )),
        }
    }

    fn iter_scan(ctx: &Context, row: &Row) -> Result<Option<u32>, FdwError> {
        let this = Self::this_mut();

        match this.resource.as_str() {
            "users" => {
                // If we've reached the end of our current batch of users
                if this.result_index >= this.users.len() {
                    // Record metrics
                    stats::inc_stats(FDW_NAME, stats::Metric::RowsIn, this.users.len() as i64);
                    stats::inc_stats(FDW_NAME, stats::Metric::RowsOut, this.users.len() as i64);

                    // If there's a next cursor and we don't have a limit or haven't reached it yet, fetch the next batch
                    if this.next_cursor.is_some() {
                        // If we have a limit, check if we've already reached it
                        if let Some(limit) = &this.limit {
                            if this.users.len() >= limit.count() as usize {
                                // We've already met our limit, don't fetch more
                                return Ok(None);
                            }
                        }

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
            "usergroups" => {
                // If we've reached the end of our user groups
                if this.result_index >= this.user_groups.len() {
                    // Record metrics
                    stats::inc_stats(
                        FDW_NAME,
                        stats::Metric::RowsIn,
                        this.user_groups.len() as i64,
                    );
                    stats::inc_stats(
                        FDW_NAME,
                        stats::Metric::RowsOut,
                        this.user_groups.len() as i64,
                    );
                    return Ok(None);
                }

                // Get the user group from the current position
                let usergroup = &this.user_groups[this.result_index];

                // Convert user group to row
                this.usergroup_to_row(usergroup, row)?;

                this.result_index += 1;
                Ok(Some(0))
            }
            "usergroup_members" => {
                // If we've reached the end of our user group memberships
                if this.result_index >= this.user_group_memberships.len() {
                    // Record metrics
                    stats::inc_stats(
                        FDW_NAME,
                        stats::Metric::RowsIn,
                        this.user_group_memberships.len() as i64,
                    );
                    stats::inc_stats(
                        FDW_NAME,
                        stats::Metric::RowsOut,
                        this.user_group_memberships.len() as i64,
                    );
                    return Ok(None);
                }

                // Get the membership from the current position
                let membership = &this.user_group_memberships[this.result_index];

                // Convert membership to row
                this.usergroup_membership_to_row(membership, row)?;

                this.result_index += 1;
                Ok(Some(0))
            }
            "messages" => {
                // If we've reached the end of our messages
                if this.result_index >= this.messages.len() {
                    // Record metrics
                    stats::inc_stats(FDW_NAME, stats::Metric::RowsIn, this.messages.len() as i64);
                    stats::inc_stats(FDW_NAME, stats::Metric::RowsOut, this.messages.len() as i64);

                    // If there's more messages and we have a pagination cursor, fetch the next batch
                    if this.has_more && this.next_cursor.is_some() {
                        this.fetch_messages(ctx)?;

                        // If the new batch is empty, we're done
                        if this.messages.is_empty() {
                            return Ok(None);
                        }
                    } else {
                        // No more results
                        return Ok(None);
                    }
                }

                // Get the message from the current batch
                let message = &this.messages[this.result_index];

                // Convert message to row
                this.message_to_row(message, row)?;

                this.result_index += 1;
                Ok(Some(0))
            }
            "channels" => {
                // If we've reached the end of our channels
                if this.result_index >= this.channels.len() {
                    // Record metrics
                    stats::inc_stats(FDW_NAME, stats::Metric::RowsIn, this.channels.len() as i64);
                    stats::inc_stats(FDW_NAME, stats::Metric::RowsOut, this.channels.len() as i64);

                    // If there's a next cursor, fetch the next batch
                    if this.next_cursor.is_some() {
                        this.fetch_channels(ctx)?;

                        // If the new batch is empty, we're done
                        if this.channels.is_empty() {
                            return Ok(None);
                        }
                    } else {
                        // No more results
                        return Ok(None);
                    }
                }

                // Get the channel from the current batch
                let channel = &this.channels[this.result_index];

                // Convert channel to row
                this.channel_to_row(channel, row)?;

                this.result_index += 1;
                Ok(Some(0))
            }
            "files" => {
                // If we've reached the end of our files
                if this.result_index >= this.files.len() {
                    // Record metrics
                    stats::inc_stats(FDW_NAME, stats::Metric::RowsIn, this.files.len() as i64);
                    stats::inc_stats(FDW_NAME, stats::Metric::RowsOut, this.files.len() as i64);
                    return Ok(None);
                }

                // Get the file from the current batch
                let file = &this.files[this.result_index];

                // Convert file to row
                this.file_to_row(file, row)?;

                this.result_index += 1;
                Ok(Some(0))
            }
            "team-info" => {
                // Team info has only one row
                if this.result_index > 0 || this.team_info.is_none() {
                    return Ok(None);
                }

                // Get the team info
                let team_info = this.team_info.as_ref().unwrap();

                // Convert team info to row
                this.team_info_to_row(team_info, row)?;

                this.result_index += 1;
                Ok(Some(0))
            }
            _ => Err(format!("Unsupported resource type: {}", this.resource)),
        }
    }

    fn re_scan(ctx: &Context) -> FdwResult {
        let this = Self::this_mut();

        // Reset pagination state
        this.has_more = false;
        this.next_cursor = None;
        this.result_index = 0;

        // Update sort and limit info in case they changed
        this.sorts = ctx.get_sorts();
        this.limit = ctx.get_limit();

        // Re-fetch the appropriate resource data
        match this.resource.as_str() {
            "users" => this.fetch_users(ctx),
            "usergroups" => this.fetch_user_groups(ctx),
            "usergroup_members" => this.fetch_user_group_memberships(ctx),
            "messages" => this.fetch_messages(ctx),
            "channels" => this.fetch_channels(ctx),
            "files" => this.fetch_files(ctx),
            "team-info" => this.fetch_team_info(ctx),
            _ => Err(format!("Unsupported resource type: {}", this.resource)),
        }
    }

    fn end_scan(_ctx: &Context) -> FdwResult {
        let this = Self::this_mut();

        // Clear cached data based on resource type
        match this.resource.as_str() {
            "users" => this.users.clear(),
            "usergroups" => this.user_groups.clear(),
            "usergroup_members" => this.user_group_memberships.clear(),
            "messages" => this.messages.clear(),
            "channels" => this.channels.clear(),
            "files" => this.files.clear(),
            "team-info" => this.team_info = None,
            _ => {} // No action for unknown resource
        }

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

    fn import_foreign_schema(
        _ctx: &Context,
        stmt: ImportForeignSchemaStmt,
    ) -> Result<Vec<String>, FdwError> {
        let ret = vec![
            format!(
                r#"create foreign table if not exists messages (
                    ts text,
                    user_id text,
                    channel_id text,
                    text text,
                    thread_ts text,
                    reply_count integer
                )
                server {} options (
                    resource 'messages'
                )"#,
                stmt.server_name,
            ),
            format!(
                r#"create foreign table if not exists channels (
                    id text,
                    name text,
                    is_private boolean,
                    created timestamp,
                    creator text
                )
                server {} options (
                    resource 'channels'
                )"#,
                stmt.server_name,
            ),
            format!(
                r#"create foreign table if not exists users (
                    id text,
                    name text,
                    real_name text,
                    display_name text,
                    display_name_normalized text,
                    real_name_normalized text,
                    email text,
                    phone text,
                    skype text,
                    is_admin boolean,
                    is_owner boolean,
                    is_primary_owner boolean,
                    is_bot boolean,
                    is_app_user boolean,
                    is_restricted boolean,
                    is_ultra_restricted boolean,
                    deleted boolean,
                    status_text text,
                    status_emoji text,
                    status_expiration bigint,
                    title text,
                    team_id text,
                    team text,
                    tz text,
                    tz_label text,
                    tz_offset integer,
                    locale text,
                    image_24 text,
                    image_48 text,
                    image_72 text,
                    image_192 text,
                    image_512 text,
                    color text,
                    updated bigint
                )
                server {} options (
                    resource 'users'
                )"#,
                stmt.server_name,
            ),
            format!(
                r#"create foreign table if not exists usergroups (
                    id text,
                    team_id text,
                    name text,
                    handle text,
                    description text,
                    is_external boolean,
                    date_create bigint,
                    date_update bigint,
                    date_delete bigint,
                    auto_type text,
                    created_by text,
                    updated_by text,
                    deleted_by text,
                    user_count integer,
                    channel_count integer
                )
                server {} options (
                    resource 'usergroups'
                )"#,
                stmt.server_name,
            ),
            format!(
                r#"create foreign table if not exists usergroup_members (
                    usergroup_id text,
                    usergroup_name text,
                    usergroup_handle text,
                    user_id text
                )
                server {} options (
                    resource 'usergroup_members'
                )"#,
                stmt.server_name,
            ),
            format!(
                r#"create foreign table if not exists files (
                    id text,
                    name text,
                    title text,
                    mimetype text,
                    size bigint,
                    url_private text,
                    user_id text,
                    created timestamp
                )
                server {} options (
                    resource 'files'
                )"#,
                stmt.server_name,
            ),
            format!(
                r#"create foreign table if not exists team_info (
                    id text,
                    name text,
                    domain text,
                    email_domain text
                )
                server {} options (
                    resource 'team-info'
                )"#,
                stmt.server_name,
            ),
        ];
        Ok(ret)
    }
}

bindings::export!(SlackFdw with_types_in bindings);
