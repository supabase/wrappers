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

/// User resource models
#[derive(Debug, Deserialize)]
pub struct UserResponse {
    pub members: Vec<User>,
    #[serde(default)]
    pub response_metadata: Option<ResponseMetadata>,
}

#[derive(Debug, Deserialize, Clone)]
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

#[derive(Debug, Deserialize, Clone)]
pub struct UserProfile {
    pub email: Option<String>,
    pub display_name: Option<String>,
    pub status_text: Option<String>,
    pub status_emoji: Option<String>,
    pub title: Option<String>,
    pub phone: Option<String>,
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
            },
            is_admin: Some(true),
            is_owner: None,
            is_bot: false,
            deleted: false,
            updated: None,
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
        assert_eq!(row[5], Some("https://files.slack.com/files-pri/T123456/document.pdf".to_string()));
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