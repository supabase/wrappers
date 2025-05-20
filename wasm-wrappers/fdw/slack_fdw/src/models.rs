use serde::{Deserialize, Serialize};

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

#[derive(Debug, Deserialize)]
pub struct ResponseMetadata {
    #[serde(default)]
    pub next_cursor: Option<String>,
}

/// Messages resource models
#[derive(Debug, Deserialize)]
pub struct ConversationHistoryResponse {
    pub messages: Vec<Message>,
    pub has_more: bool,
    #[serde(default)]
    pub response_metadata: Option<ResponseMetadata>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Message {
    pub ts: String,
    pub user: Option<String>,
    pub bot_id: Option<String>,
    pub text: String,
    pub thread_ts: Option<String>,
    pub reply_count: Option<i32>,
    #[serde(default)]
    pub reactions: Option<Vec<Reaction>>,
    #[serde(default)]
    pub files: Option<Vec<File>>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Reaction {
    pub name: String,
    pub users: Vec<String>,
    pub count: i32,
}

/// Channels resource models
#[derive(Debug, Deserialize)]
pub struct ConversationListResponse {
    pub channels: Vec<Channel>,
    #[serde(default)]
    pub response_metadata: Option<ResponseMetadata>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Channel {
    pub id: String,
    pub name: String,
    pub is_private: bool,
    pub created: i64,
    pub creator: String,
    pub is_archived: bool,
    pub num_members: Option<i32>,
    pub topic: Option<ChannelTopic>,
    pub purpose: Option<ChannelPurpose>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ChannelTopic {
    pub value: String,
    pub creator: String,
    pub last_set: i64,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ChannelPurpose {
    pub value: String,
    pub creator: String,
    pub last_set: i64,
}

/// Files resource models
#[derive(Debug, Deserialize)]
pub struct FilesListResponse {
    pub files: Vec<File>,
    pub paging: Option<Paging>,
}

#[derive(Debug, Deserialize, Clone)]
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

#[derive(Debug, Deserialize, Clone)]
pub struct Paging {
    pub count: i32,
    pub total: i32,
    pub page: i32,
    pub pages: i32,
}

/// Team info resource models
#[derive(Debug, Deserialize)]
pub struct TeamInfoResponse {
    pub team: TeamInfo,
}

#[derive(Debug, Deserialize, Clone)]
pub struct TeamInfo {
    pub id: String,
    pub name: String,
    pub domain: String,
    pub email_domain: String,
    pub icon: TeamIcon,
}

#[derive(Debug, Deserialize, Clone)]
pub struct TeamIcon {
    pub image_34: String,
    pub image_44: String,
    pub image_68: String,
    pub image_88: String,
    pub image_102: String,
    pub image_132: String,
}

/// User resource models
#[derive(Debug, Deserialize)]
pub struct UserResponse {
    pub members: Vec<User>,
    #[serde(default)]
    pub response_metadata: Option<ResponseMetadata>,
}

#[derive(Debug, Default, Deserialize, Clone)]
pub struct User {
    pub id: String,
    pub name: String,
    pub real_name: Option<String>,
    pub team_id: Option<String>,
    pub profile: UserProfile,
    pub is_admin: Option<bool>,
    pub is_owner: Option<bool>,
    pub is_primary_owner: Option<bool>,
    pub is_restricted: Option<bool>,
    pub is_ultra_restricted: Option<bool>,
    pub is_bot: bool,
    pub is_app_user: Option<bool>,
    pub deleted: bool,
    pub updated: Option<i64>,
    pub tz: Option<String>,
    pub tz_label: Option<String>,
    pub tz_offset: Option<i32>,
    pub locale: Option<String>,
    pub color: Option<String>,
}

#[derive(Debug, Default, Deserialize, Clone)]
pub struct UserProfile {
    pub email: Option<String>,
    pub display_name: Option<String>,
    pub real_name: Option<String>,
    pub real_name_normalized: Option<String>,
    pub display_name_normalized: Option<String>,
    pub status_text: Option<String>,
    pub status_emoji: Option<String>,
    pub status_expiration: Option<i64>,
    pub title: Option<String>,
    pub phone: Option<String>,
    pub skype: Option<String>,
    pub image_24: Option<String>,
    pub image_32: Option<String>,
    pub image_48: Option<String>,
    pub image_72: Option<String>,
    pub image_192: Option<String>,
    pub image_512: Option<String>,
    pub team: Option<String>,
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

// User Group models
#[derive(Debug, Deserialize)]
pub struct UserGroupsResponse {
    pub usergroups: Vec<UserGroup>,
    #[serde(default)]
    pub response_metadata: Option<ResponseMetadata>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct UserGroup {
    pub id: String,
    pub team_id: String,
    pub name: String,
    pub handle: String,
    pub description: Option<String>,
    pub is_external: Option<bool>,
    pub date_create: i64,
    pub date_update: i64,
    pub date_delete: Option<i64>,
    pub auto_type: Option<String>,
    pub created_by: String,
    pub updated_by: Option<String>,
    pub deleted_by: Option<String>,
    pub user_count: Option<i32>,
    pub users: Option<Vec<String>>,
    pub prefs: Option<UserGroupPrefs>,
    pub channel_count: Option<i32>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct UserGroupPrefs {
    pub channels: Option<Vec<String>>,
    pub groups: Option<Vec<String>>,
}

// User Group Members response
#[derive(Debug, Deserialize)]
pub struct UserGroupMembersResponse {
    pub users: Vec<String>,
    #[serde(default)]
    pub response_metadata: Option<ResponseMetadata>,
}

// Model for joining user groups with users
#[derive(Debug, Clone)]
pub struct UserGroupMembership {
    pub usergroup_id: String,
    pub usergroup_name: String,
    pub usergroup_handle: String,
    pub user_id: String,
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
            self.reply_count.map(|r| r.to_string()),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_to_row() {
        let msg = Message {
            ts: "1234567890.123456".to_string(),
            user: Some("U12345".to_string()),
            bot_id: None,
            text: "Test message".to_string(),
            thread_ts: Some("1234567890.123456".to_string()),
            reply_count: Some(5),
            reactions: None,
            files: None,
        };

        let row = msg.to_row("C12345");
        assert_eq!(row.len(), 6);
        assert_eq!(row[0], Some("1234567890.123456".to_string()));
        assert_eq!(row[1], Some("U12345".to_string()));
        assert_eq!(row[2], Some("C12345".to_string()));
        assert_eq!(row[3], Some("Test message".to_string()));
        assert_eq!(row[4], Some("1234567890.123456".to_string()));
        assert_eq!(row[5], Some("5".to_string()));
    }

    #[test]
    fn test_user_to_row() {
        let user = User {
            id: "U12345".to_string(),
            name: "testuser".to_string(),
            real_name: Some("Test User".to_string()),
            profile: UserProfile {
                email: Some("test@example.com".to_string()),
                display_name: Some("testuser".to_string()),
                status_text: None,
                status_emoji: None,
                title: None,
                phone: None,
                ..Default::default()
            },
            is_admin: Some(true),
            is_owner: None,
            is_bot: false,
            deleted: false,
            updated: None,
            ..Default::default()
        };

        let row = user.to_row();
        assert_eq!(row.len(), 6);
        assert_eq!(row[0], Some("U12345".to_string()));
        assert_eq!(row[1], Some("testuser".to_string()));
        assert_eq!(row[2], Some("Test User".to_string()));
        assert_eq!(row[3], Some("test@example.com".to_string()));
        assert_eq!(row[4], Some("true".to_string()));
        assert_eq!(row[5], Some("false".to_string()));
    }

    #[test]
    fn test_channel_to_row() {
        let channel = Channel {
            id: "C12345".to_string(),
            name: "general".to_string(),
            is_private: false,
            created: 1618247000,
            creator: "U12345".to_string(),
            is_archived: false,
            num_members: Some(42),
            topic: None,
            purpose: None,
        };

        let row = channel.to_row();
        assert_eq!(row.len(), 5);
        assert_eq!(row[0], Some("C12345".to_string()));
        assert_eq!(row[1], Some("general".to_string()));
        assert_eq!(row[2], Some("false".to_string()));
        assert_eq!(row[3], Some("1618247000".to_string()));
        assert_eq!(row[4], Some("U12345".to_string()));
    }

    #[test]
    fn test_file_to_row() {
        let file = File {
            id: "F12345".to_string(),
            name: "document.pdf".to_string(),
            title: "Important Document".to_string(),
            mimetype: "application/pdf".to_string(),
            filetype: "pdf".to_string(),
            size: 1024000,
            url_private: Some("https://files.slack.com/files-pri/T123456/document.pdf".to_string()),
            url_private_download: None,
            user: "U12345".to_string(),
            created: 1618247000,
            timestamp: 1618247000,
            channels: Some(vec!["C12345".to_string()]),
        };

        let row = file.to_row();
        assert_eq!(row.len(), 8);
        assert_eq!(row[0], Some("F12345".to_string()));
        assert_eq!(row[1], Some("document.pdf".to_string()));
        assert_eq!(row[2], Some("Important Document".to_string()));
        assert_eq!(row[3], Some("application/pdf".to_string()));
        assert_eq!(row[4], Some("1024000".to_string()));
        assert_eq!(
            row[5],
            Some("https://files.slack.com/files-pri/T123456/document.pdf".to_string())
        );
        assert_eq!(row[6], Some("U12345".to_string()));
        assert_eq!(row[7], Some("1618247000".to_string()));
    }

    #[test]
    fn test_team_info_to_row() {
        let team_info = TeamInfo {
            id: "T12345".to_string(),
            name: "Acme Inc".to_string(),
            domain: "acme".to_string(),
            email_domain: "acme.com".to_string(),
            icon: TeamIcon {
                image_34: "https://a.slack-edge.com/image_34.png".to_string(),
                image_44: "https://a.slack-edge.com/image_44.png".to_string(),
                image_68: "https://a.slack-edge.com/image_68.png".to_string(),
                image_88: "https://a.slack-edge.com/image_88.png".to_string(),
                image_102: "https://a.slack-edge.com/image_102.png".to_string(),
                image_132: "https://a.slack-edge.com/image_132.png".to_string(),
            },
        };

        let row = team_info.to_row();
        assert_eq!(row.len(), 4);
        assert_eq!(row[0], Some("T12345".to_string()));
        assert_eq!(row[1], Some("Acme Inc".to_string()));
        assert_eq!(row[2], Some("acme".to_string()));
        assert_eq!(row[3], Some("acme.com".to_string()));
    }
}
