use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// Slack API Response models

/// General Slack API response structure
#[derive(Debug, Deserialize)]
pub struct SlackResponse<T> {
    pub ok: bool,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(flatten)]
    pub data: Option<T>,
}

/// Message resource models
#[derive(Debug, Deserialize)]
pub struct MessageResponse {
    pub messages: Vec<Message>,
    pub has_more: bool,
    #[serde(default)]
    pub response_metadata: Option<ResponseMetadata>,
}

#[derive(Debug, Deserialize)]
pub struct ResponseMetadata {
    #[serde(default)]
    pub next_cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct Message {
    pub ts: String,
    #[serde(default)]
    pub user: Option<String>,
    #[serde(default)]
    pub bot_id: Option<String>,
    pub text: String,
    #[serde(default)]
    pub thread_ts: Option<String>,
    #[serde(default)]
    pub reply_count: Option<i32>,
    #[serde(default)]
    pub reactions: Option<Vec<Reaction>>,
    #[serde(default)]
    pub files: Option<Vec<File>>,
}

/// Channel resource models
#[derive(Debug, Deserialize)]
pub struct ChannelResponse {
    pub channels: Vec<Channel>,
    #[serde(default)]
    pub response_metadata: Option<ResponseMetadata>,
}

#[derive(Debug, Deserialize)]
pub struct Channel {
    pub id: String,
    pub name: String,
    pub is_private: bool,
    pub created: i64,
    pub creator: String,
    pub is_archived: bool,
    pub num_members: Option<i32>,
    pub topic: Option<Topic>,
    pub purpose: Option<Purpose>,
}

#[derive(Debug, Deserialize)]
pub struct Topic {
    pub value: String,
    pub creator: String,
    pub last_set: i64,
}

#[derive(Debug, Deserialize)]
pub struct Purpose {
    pub value: String,
    pub creator: String,
    pub last_set: i64,
}

/// User resource models
#[derive(Debug, Deserialize)]
pub struct UserResponse {
    pub members: Vec<User>,
    #[serde(default)]
    pub response_metadata: Option<ResponseMetadata>,
}

#[derive(Debug, Deserialize)]
pub struct User {
    pub id: String,
    pub name: String,
    pub real_name: Option<String>,
    pub profile: UserProfile,
    pub is_admin: Option<bool>,
    pub is_owner: Option<bool>,
    pub is_bot: bool,
    pub deleted: bool,
    pub updated: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct UserProfile {
    pub email: Option<String>,
    pub display_name: Option<String>,
    pub status_text: Option<String>,
    pub status_emoji: Option<String>,
    pub title: Option<String>,
    pub phone: Option<String>,
}

/// File resource models
#[derive(Debug, Deserialize)]
pub struct FileResponse {
    pub files: Vec<File>,
    pub paging: Paging,
}

#[derive(Debug, Deserialize)]
pub struct File {
    pub id: String,
    pub name: String,
    pub title: String,
    pub mimetype: String,
    pub filetype: String,
    pub size: i64,
    pub url_private: Option<String>,
    pub url_private_download: Option<String>,
    pub user: String,
    pub created: i64,
    pub timestamp: i64,
    pub channels: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
pub struct Paging {
    pub count: i32,
    pub total: i32,
    pub page: i32,
    pub pages: i32,
}

/// Reaction resource models
#[derive(Debug, Deserialize)]
pub struct Reaction {
    pub name: String,
    pub count: i32,
    pub users: Vec<String>,
}

/// Team Info resource models
#[derive(Debug, Deserialize)]
pub struct TeamInfoResponse {
    pub team: TeamInfo,
}

#[derive(Debug, Deserialize)]
pub struct TeamInfo {
    pub id: String,
    pub name: String,
    pub domain: String,
    pub email_domain: String,
    pub icon: TeamIcon,
}

#[derive(Debug, Deserialize)]
pub struct TeamIcon {
    pub image_34: String,
    pub image_44: String,
    pub image_68: String,
    pub image_88: String,
    pub image_102: String,
    pub image_132: String,
}

/// API Error models
#[derive(Debug, Serialize, Deserialize)]
pub struct ApiError {
    pub error: String,
    #[serde(default)]
    pub warning: Option<String>,
    #[serde(default)]
    pub response_metadata: Option<ErrorMetadata>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ErrorMetadata {
    #[serde(default)]
    pub messages: Option<Vec<String>>,
}

// Utility conversion functions for database rows
impl Message {
    pub fn to_row(&self, channel_id: &str) -> Vec<Option<String>> {
        vec![
            Some(self.ts.clone()),
            self.user.clone(),
            Some(channel_id.to_string()),
            Some(self.text.clone()),
            self.thread_ts.clone(),
            self.reply_count.map(|c| c.to_string()),
        ]
    }
}

impl Channel {
    pub fn to_row(&self) -> Vec<Option<String>> {
        vec![
            Some(self.id.clone()),
            Some(self.name.clone()),
            Some(self.is_private.to_string()),
            Some(self.created.to_string()),
            Some(self.creator.clone()),
        ]
    }
}

impl User {
    pub fn to_row(&self) -> Vec<Option<String>> {
        vec![
            Some(self.id.clone()),
            Some(self.name.clone()),
            self.real_name.clone(),
            self.profile.email.clone(),
            self.is_admin.map(|a| a.to_string()),
            Some(self.is_bot.to_string()),
        ]
    }
}

impl File {
    pub fn to_row(&self) -> Vec<Option<String>> {
        vec![
            Some(self.id.clone()),
            Some(self.name.clone()),
            Some(self.title.clone()),
            Some(self.mimetype.clone()),
            Some(self.size.to_string()),
            self.url_private.clone(),
            Some(self.user.clone()),
            Some(self.created.to_string()),
        ]
    }
}

impl TeamInfo {
    pub fn to_row(&self) -> Vec<Option<String>> {
        vec![
            Some(self.id.clone()),
            Some(self.name.clone()),
            Some(self.domain.clone()),
            Some(self.email_domain.clone()),
        ]
    }
}