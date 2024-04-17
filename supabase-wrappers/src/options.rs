use pgrx::pg_sys::panic::ErrorReport;
use pgrx::{pg_sys, PgList, PgSqlErrorCode};
use std::collections::HashMap;
use std::ffi::CStr;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum OptionsError {
    #[error("required option `{0}` is not specified")]
    OptionNameNotFound(String),
    #[error("option name `{0}` is not a valid UTF-8 string")]
    OptionNameIsInvalidUtf8(String),
    #[error("option value `{0}` is not a valid UTF-8 string")]
    OptionValueIsInvalidUtf8(String),
}

impl From<OptionsError> for ErrorReport {
    fn from(value: OptionsError) -> Self {
        let error_message = format!("{value}");
        match value {
            OptionsError::OptionNameNotFound(_) => ErrorReport::new(
                PgSqlErrorCode::ERRCODE_FDW_OPTION_NAME_NOT_FOUND,
                error_message,
                "",
            ),
            OptionsError::OptionNameIsInvalidUtf8(_)
            | OptionsError::OptionValueIsInvalidUtf8(_) => ErrorReport::new(
                PgSqlErrorCode::ERRCODE_FDW_INVALID_STRING_FORMAT,
                error_message,
                "",
            ),
        }
    }
}

/// Get required option value from the `options` map
///
/// Get the required option's value from `options` map, return None and report
/// error and stop current transaction if it does not exist.
///
/// For example,
///
/// ```rust,no_run
/// # use supabase_wrappers::prelude::require_option;
/// # use std::collections::HashMap;
/// # use supabase_wrappers::options::OptionsError;
/// # fn main() -> Result<(), OptionsError> {
/// # let options = &HashMap::new();
/// require_option("my_option", options)?;
/// # Ok(())
/// # }
/// ```
pub fn require_option<'map>(
    opt_name: &str,
    options: &'map HashMap<String, String>,
) -> Result<&'map str, OptionsError> {
    options
        .get(opt_name)
        .map(|t| t.as_ref())
        .ok_or_else(|| OptionsError::OptionNameNotFound(opt_name.to_string()))
}

/// Get required option value from the `options` map or a provided default
///
/// Get the required option's value from `options` map, return default if it does not exist.
///
/// For example,
///
/// ```rust,no_run
/// # use supabase_wrappers::prelude::require_option_or;
/// # use std::collections::HashMap;
/// # let options = &HashMap::new();
/// require_option_or("my_option", options, "default value");
/// ```
pub fn require_option_or<'a>(
    opt_name: &str,
    options: &'a HashMap<String, String>,
    default: &'a str,
) -> &'a str {
    options.get(opt_name).map(|t| t.as_ref()).unwrap_or(default)
}

/// Check if the option list contains a specific option, used in [validator](crate::interface::ForeignDataWrapper::validator)
pub fn check_options_contain(opt_list: &[Option<String>], tgt: &str) -> Result<(), OptionsError> {
    let search_key = format!("{}=", tgt);
    let valid = opt_list
        .iter()
        .flatten()
        .any(|a| a.starts_with(&search_key));
    if valid {
        Ok(())
    } else {
        Err(OptionsError::OptionNameNotFound(tgt.to_string()))
    }
}

// convert options definition to hashmap
pub(super) unsafe fn options_to_hashmap(
    options: *mut pg_sys::List,
) -> Result<HashMap<String, String>, OptionsError> {
    let mut ret = HashMap::new();
    let options: PgList<pg_sys::DefElem> = PgList::from_pg(options);
    for option in options.iter_ptr() {
        let name = CStr::from_ptr((*option).defname);
        let value = CStr::from_ptr(pg_sys::defGetString(option));
        let name = name.to_str().map_err(|_| {
            OptionsError::OptionNameIsInvalidUtf8(
                String::from_utf8_lossy(name.to_bytes()).to_string(),
            )
        })?;
        let value = value.to_str().map_err(|_| {
            OptionsError::OptionValueIsInvalidUtf8(
                String::from_utf8_lossy(value.to_bytes()).to_string(),
            )
        })?;
        ret.insert(name.to_string(), value.to_string());
    }
    Ok(ret)
}
