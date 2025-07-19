#[allow(warnings)]
mod bindings;
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};

use bindings::{
    exports::supabase::wrappers::routines::Guest,
    supabase::wrappers::{
        http, stats, time,
        types::{
            Cell, Context, FdwError, FdwResult, ImportForeignSchemaStmt, OptionsType, Row, TypeOid,
            Value,
        },
        utils,
    },
};

#[derive(Debug, Default)]
struct GravatarFdw {
    base_url: String,
    headers: Vec<(String, String)>,
    scanned_profiles: Vec<JsonValue>,
    scan_index: usize,
}

static mut INSTANCE: *mut GravatarFdw = std::ptr::null_mut::<GravatarFdw>();
static FDW_NAME: &str = "GravatarFdw";

impl GravatarFdw {
    const PROFILES_OBJECT: &'static str = "profiles";

    fn init() {
        let instance = Self::default();
        unsafe {
            INSTANCE = Box::leak(Box::new(instance));
        }
    }

    fn this_mut() -> &'static mut Self {
        unsafe { &mut (*INSTANCE) }
    }

    // build URL for gravatar profile
    fn build_url(&self, email: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(email.trim().to_lowercase().as_bytes());
        let hash = format!("{:x}", hasher.finalize());
        format!("{}/profiles/{}", self.base_url, hash)
    }

    // fetch source data rows from Gravatar API
    fn fetch_source_data(&mut self, ctx: &Context) -> FdwResult {
        // clear previous results
        self.scanned_profiles.clear();
        self.scan_index = 0;

        let opts = ctx.get_options(&OptionsType::Table);
        let table = opts.require_or("table", Self::PROFILES_OBJECT);
        if table != Self::PROFILES_OBJECT {
            return Err(format!(
                "Unsupported table '{}'. Only 'profiles' is supported.",
                table
            ));
        }

        // look for email filters in quals
        let mut emails_to_fetch = Vec::new();
        let quals = ctx.get_quals();

        for qual in quals {
            if qual.field() == "email" {
                if qual.operator() == "=" {
                    if let Value::Cell(Cell::String(email)) = qual.value() {
                        emails_to_fetch.push(email);
                    }
                } else {
                    // handle unsupported operators like IN, LIKE, etc.
                    return Err(format!("Unsupported operator '{}' for email field. Only '=' (equality) is supported.", qual.operator()));
                }
            }
        }

        // if no email filter provided, we can't fetch profiles
        if emails_to_fetch.is_empty() {
            utils::report_warning("No email filters provided. Gravatar FDW requires email = 'email@example.com' in WHERE clause");
            return Ok(());
        }

        // only allow one email at a time
        if emails_to_fetch.len() > 1 {
            return Err(format!("Multiple email filters are not supported. Found {} email conditions. Use separate queries for each email.", emails_to_fetch.len()));
        }

        // fetch profiles for each email
        for email in emails_to_fetch {
            let url = self.build_url(&email);

            let req = http::Request {
                method: http::Method::Get,
                url,
                headers: self.headers.clone(),
                body: String::default(),
            };

            let resp = http::get(&req)?;

            // handle 429 rate limiting
            if resp.status_code == 429 {
                // check if we're using an API key
                let using_api_key = self
                    .headers
                    .iter()
                    .any(|(key, _)| key.to_lowercase() == "authorization");

                // build error message based on X-RateLimit-Reset header and API key usage
                let mut error_msg = "Rate limit exceeded (429).".to_string();
                if let Some(reset_header) = resp
                    .headers
                    .iter()
                    .find(|h| h.0.to_lowercase() == "x-ratelimit-reset")
                {
                    if let Ok(reset_timestamp) = reset_header.1.parse::<u64>() {
                        let current_time = time::epoch_secs() as u64;
                        let wait_seconds = reset_timestamp.saturating_sub(current_time);
                        error_msg.push_str(&format!(" Wait {} seconds for reset.", wait_seconds));
                    }
                }

                if using_api_key {
                    error_msg.push_str(" Please contact Gravatar to increase your usage limit.");
                } else {
                    error_msg.push_str(" Consider getting an API key at https://gravatar.com/developers/applications for higher rate limits.");
                }

                return Err(error_msg);
            }

            if resp.status_code == 200 {
                // parse successful response
                let mut profile: JsonValue = serde_json::from_str(&resp.body)
                    .map_err(|e| format!("Failed to parse JSON response: {}", e))?;

                // add email to the response since API doesn't return it
                if let JsonValue::Object(ref mut map) = profile {
                    map.insert("email".to_string(), JsonValue::String(email.clone()));
                }

                self.scanned_profiles.push(profile);
            } else {
                // handle 404 (expected for private or non-existing profiles) and generic API errors
                // by skipping this email - no row will be returned for failed lookups
                if resp.status_code == 404 {
                    utils::report_warning(&format!("Profile not found for email: {}", email));
                } else {
                    utils::report_warning(&format!(
                        "HTTP error {} for email {}: {}",
                        resp.status_code, email, resp.body
                    ));
                }
            }
        }

        Ok(())
    }
}

impl Guest for GravatarFdw {
    fn host_version_requirement() -> String {
        // semver ref: https://docs.rs/semver/latest/semver/enum.Op.html
        "^0.1.0".to_string()
    }

    fn init(ctx: &Context) -> FdwResult {
        Self::init();
        let this = Self::this_mut();

        // get foreign server options
        let opts = ctx.get_options(&OptionsType::Server);
        this.base_url = opts.require_or("api_url", "https://api.gravatar.com/v3");

        // initialize basic headers
        let user_agent = format!("{}/{}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"));
        this.headers.push(("user-agent".to_owned(), user_agent));
        this.headers
            .push(("accept".to_owned(), "application/json".to_owned()));

        // handle API key authentication
        // support three options: direct api_key, api_key_id (via vault UUID) or no api_key
        // ref: https://docs.gravatar.com/rest/authentication/
        if let Some(api_key) = opts.get("api_key") {
            this.headers
                .push(("authorization".to_owned(), format!("Bearer {}", api_key)));
        } else if let Some(api_key_id) = opts.get("api_key_id") {
            if let Some(vault_api_key) = utils::get_vault_secret(&api_key_id) {
                this.headers.push((
                    "authorization".to_owned(),
                    format!("Bearer {}", vault_api_key),
                ));
            } else {
                return Err(format!(
                    "Failed to retrieve API key from Vault using ID: {}",
                    api_key_id
                ));
            }
        } else {
            // no API key provided - will use public API endpoints only
            utils::report_warning("Gravatar FDW initialized without API key (public access only)");
        }

        stats::inc_stats(FDW_NAME, stats::Metric::CreateTimes, 1);

        Ok(())
    }

    fn begin_scan(ctx: &Context) -> FdwResult {
        let this = Self::this_mut();
        this.fetch_source_data(ctx)
    }

    fn iter_scan(ctx: &Context, row: &Row) -> Result<Option<u32>, FdwError> {
        let this = Self::this_mut();

        if this.scan_index >= this.scanned_profiles.len() {
            stats::inc_stats(FDW_NAME, stats::Metric::RowsIn, this.scan_index as i64);
            stats::inc_stats(FDW_NAME, stats::Metric::RowsOut, this.scan_index as i64);
            return Ok(None);
        }

        let profile = &this.scanned_profiles[this.scan_index];

        for tgt_col in ctx.get_columns() {
            let tgt_col_name = tgt_col.name();
            let cell = match tgt_col_name.as_str() {
                "hash" => profile
                    .get("hash")
                    .and_then(|v| v.as_str())
                    .map(|s| Cell::String(s.to_string())),
                "email" => profile
                    .get("email")
                    .and_then(|v| v.as_str())
                    .map(|s| Cell::String(s.to_string())),
                "display_name" => profile
                    .get("display_name")
                    .and_then(|v| v.as_str())
                    .map(|s| Cell::String(s.to_string())),
                "profile_url" => profile
                    .get("profile_url")
                    .and_then(|v| v.as_str())
                    .map(|s| Cell::String(s.to_string())),
                "avatar_url" => profile
                    .get("avatar_url")
                    .and_then(|v| v.as_str())
                    .map(|s| Cell::String(s.to_string())),
                "avatar_alt_text" => profile
                    .get("avatar_alt_text")
                    .and_then(|v| v.as_str())
                    .map(|s| Cell::String(s.to_string())),
                "location" => profile
                    .get("location")
                    .and_then(|v| v.as_str())
                    .map(|s| Cell::String(s.to_string())),
                "description" => profile
                    .get("description")
                    .and_then(|v| v.as_str())
                    .map(|s| Cell::String(s.to_string())),
                "job_title" => profile
                    .get("job_title")
                    .and_then(|v| v.as_str())
                    .map(|s| Cell::String(s.to_string())),
                "company" => profile
                    .get("company")
                    .and_then(|v| v.as_str())
                    .map(|s| Cell::String(s.to_string())),
                "verified_accounts" => profile
                    .get("verified_accounts")
                    .map(|v| Cell::Json(v.to_string())),
                "pronunciation" => profile
                    .get("pronunciation")
                    .and_then(|v| v.as_str())
                    .map(|s| Cell::String(s.to_string())),
                "pronouns" => profile
                    .get("pronouns")
                    .and_then(|v| v.as_str())
                    .map(|s| Cell::String(s.to_string())),
                "timezone" => profile
                    .get("timezone")
                    .and_then(|v| v.as_str())
                    .map(|s| Cell::String(s.to_string())),
                "languages" => profile.get("languages").map(|v| Cell::Json(v.to_string())),
                "first_name" => profile
                    .get("first_name")
                    .and_then(|v| v.as_str())
                    .map(|s| Cell::String(s.to_string())),
                "last_name" => profile
                    .get("last_name")
                    .and_then(|v| v.as_str())
                    .map(|s| Cell::String(s.to_string())),
                "is_organization" => profile
                    .get("is_organization")
                    .and_then(|v| v.as_bool())
                    .map(Cell::Bool),
                "links" => profile.get("links").map(|v| Cell::Json(v.to_string())),
                "interests" => profile.get("interests").map(|v| Cell::Json(v.to_string())),
                "payments" => profile.get("payments").map(|v| Cell::Json(v.to_string())),
                "contact_info" => profile
                    .get("contact_info")
                    .map(|v| Cell::Json(v.to_string())),
                "number_verified_accounts" => profile
                    .get("number_verified_accounts")
                    .and_then(|v| v.as_i64())
                    .map(Cell::I64),
                "last_profile_edit" => profile
                    .get("last_profile_edit")
                    .and_then(|v| v.as_str())
                    .map(|s| {
                        let ts = time::parse_from_rfc3339(s).unwrap_or_default();
                        Cell::Timestamp(ts)
                    }),
                "registration_date" => profile
                    .get("registration_date")
                    .and_then(|v| v.as_str())
                    .map(|s| {
                        let ts = time::parse_from_rfc3339(s).unwrap_or_default();
                        Cell::Timestamp(ts)
                    }),
                "attrs" => Some(Cell::Json(profile.to_string())),
                _ => {
                    // For unknown columns, try to get the value directly
                    match tgt_col.type_oid() {
                        TypeOid::Bool => profile
                            .get(&tgt_col_name)
                            .and_then(|v| v.as_bool())
                            .map(Cell::Bool),
                        TypeOid::String => profile
                            .get(&tgt_col_name)
                            .and_then(|v| v.as_str())
                            .map(|s| Cell::String(s.to_string())),
                        TypeOid::I32 => profile
                            .get(&tgt_col_name)
                            .and_then(|v| v.as_i64())
                            .map(|i| Cell::I32(i as i32)),
                        TypeOid::I64 => profile
                            .get(&tgt_col_name)
                            .and_then(|v| v.as_i64())
                            .map(Cell::I64),
                        TypeOid::Json => profile
                            .get(&tgt_col_name)
                            .map(|v| Cell::Json(v.to_string())),
                        _ => None,
                    }
                }
            };

            row.push(cell.as_ref());
        }

        this.scan_index += 1;

        Ok(Some(0))
    }

    fn re_scan(ctx: &Context) -> FdwResult {
        let this = Self::this_mut();
        this.fetch_source_data(ctx)
    }

    fn end_scan(_ctx: &Context) -> FdwResult {
        let this = Self::this_mut();
        this.scanned_profiles.clear();
        this.scan_index = 0;
        Ok(())
    }

    fn begin_modify(_ctx: &Context) -> FdwResult {
        Err("modify on foreign table is not supported".to_owned())
    }

    fn insert(_ctx: &Context, _row: &Row) -> FdwResult {
        Ok(())
    }

    fn update(_ctx: &Context, _rowid: Cell, _row: &Row) -> FdwResult {
        Ok(())
    }

    fn delete(_ctx: &Context, _rowid: Cell) -> FdwResult {
        Ok(())
    }

    fn end_modify(_ctx: &Context) -> FdwResult {
        Ok(())
    }

    fn import_foreign_schema(
        _ctx: &Context,
        stmt: ImportForeignSchemaStmt,
    ) -> Result<Vec<String>, FdwError> {
        let ret = vec![format!(
            r#"create foreign table if not exists profiles (
                    hash text,
                    email text,
                    display_name text,
                    profile_url text,
                    avatar_url text,
                    avatar_alt_text text,
                    location text,
                    description text,
                    job_title text,
                    company text,
                    verified_accounts jsonb,
                    pronunciation text,
                    pronouns text,
                    timezone text,
                    language jsonb,
                    first_name text,
                    last_name text,
                    is_organization boolean,
                    links jsonb,
                    interests jsonb,
                    payments jsonb,
                    contact_info jsonb,
                    number_verified_accounts bigint,
                    last_profile_edit timestamp,
                    registration_date timestamp,
                    attrs jsonb
                )
                server {} options (
                    table 'profiles'
                )"#,
            stmt.server_name,
        )];
        Ok(ret)
    }
}

bindings::export!(GravatarFdw with_types_in bindings);
