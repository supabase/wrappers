use pgrx::JsonB;
use serde::Deserialize;
use supabase_wrappers::prelude::Cell;
use supabase_wrappers::prelude::Column;
use supabase_wrappers::prelude::Row;

#[derive(Debug, Deserialize, PartialEq)]
pub struct ResultPayload {
    users: Vec<Auth0User>,
    start: Option<u64>,
    limit: Option<u64>,
    length: Option<u64>,
    total: Option<u64>,
}

#[derive(Debug, Deserialize, PartialEq)]
pub(crate) struct Success {
    status: String,
    result: ResultPayload,
    time: f64,
}

// {
//     "picture": "https://data.nccr-catalysis.ch/api/avatar?sub=auth0|66437a7f85eb3d0c0ac1bec5",
//     "identities": [
//         {
//             "connection": "Username-Password-Authentication",
//             "user_id": "66437a7f85eb3d0c0ac1bec5",
//             "provider": "auth0",
//             "isSocial": false
//         }
//     ],
//     "user_metadata": {},
//     "user_id": "auth0|66437a7f85eb3d0c0ac1bec5",
//     "nickname": "NCCR Catalysis Admin",
//     "created_at": "2024-05-14T14:51:43.844Z",
//     "updated_at": "2024-05-14T14:55:08.716Z",
//     "email": "admin@nccr-catalysis.ch",
//     "email_verified": true,
//     "name": "NCCR Catalysis Admin",
//     "last_login": "2024-05-14T14:55:08.716Z",
//     "last_ip": "2a04:ee41:86:92f2:1d3d:14bd:1fa3:4c88",
//     "logins_count": 3,
//     "app_metadata": {}
// },

#[derive(Debug, Deserialize, PartialEq)]
pub struct Auth0User {
    pub user_id: String,
    pub email: String,
    pub email_verified: bool,
    pub username: Option<String>,
    pub phone_number: Option<String>,
    pub phone_verified: Option<bool>,
    pub created_at: String,
    pub updated_at: String,
    pub identities: Option<serde_json::Value>,
    pub app_metadata: Option<serde_json::Value>,
    pub user_metadata: Option<serde_json::Value>,
    pub picture: String,
    pub name: String,
    pub nickname: String,
    pub multifactor: Option<serde_json::Value>,
    pub last_ip: String,
    pub last_login: String,
    pub logins_count: i32,
    pub blocked: Option<bool>,
    pub given_name: Option<String>,
    pub family_name: Option<String>,
}

impl ResultPayload {
    pub fn into_users(self) -> Vec<Auth0User> {
        self.users
    }
    pub fn get_total(&self) -> Option<u64> {
        self.total
    }
}

impl Auth0User {
    pub(crate) fn into_row(mut self, columns: &[Column]) -> Row {
        let mut row = Row::new();
        for tgt_col in columns {
            let cell = match tgt_col.name.as_str() {
                "user_id" => Some(Cell::String(self.user_id.clone())),
                "email" => Some(Cell::String(self.email.clone())),
                "email_verified" => Some(Cell::Bool(self.email_verified)),
                "username" => self.username.take().map(Cell::String),
                "phone_number" => self.phone_number.take().map(Cell::String),
                "phone_verified" => self.phone_verified.take().map(Cell::Bool),
                "created_at" => Some(Cell::String(self.created_at.clone())),
                "updated_at" => Some(Cell::String(self.updated_at.clone())),
                "identities" => Some(Cell::Json(JsonB(
                    self.identities
                        .take()
                        .expect("Column identities is expected but missing"),
                ))),
                "app_metadata" => self.app_metadata.take().map(|data| Cell::Json(JsonB(data))),
                "user_metadata" => self
                    .user_metadata
                    .take()
                    .map(|data| Cell::Json(JsonB(data))),
                "picture" => Some(Cell::String(self.picture.clone())),
                "name" => Some(Cell::String(self.name.clone())),
                "nickname" => Some(Cell::String(self.nickname.clone())),
                "multifactor" => self.multifactor.take().map(|data| Cell::Json(JsonB(data))),
                "last_ip" => Some(Cell::String(self.last_ip.clone())),
                "last_login" => Some(Cell::String(self.last_login.clone())),
                "logins_count" => Some(Cell::I32(self.logins_count)),
                "blocked" => self.blocked.take().map(Cell::Bool),
                "given_name" => self.given_name.take().map(Cell::String),
                "family_name" => self.family_name.take().map(Cell::String),
                _ => None,
            };
            row.push(tgt_col.name.as_str(), cell);
        }
        row
    }
}
