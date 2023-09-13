use crate::utils::report_error;
use pgrx::pg_sys::panic::ErrorReport;
use pgrx::{pg_sys, PgList, PgSqlErrorCode};
use std::collections::HashMap;
use std::ffi::CStr;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum OptionsError {
    #[error("required option `{0}` is not specified")]
    OptionNameNotFound(String),
    #[error("an option name is not a valid UTF-8 string")]
    OptionNameIsInvalidUtf8,
    #[error("an option value is not a valid UTF-8 string")]
    OptionValueIsInvalidUtf8,
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
            OptionsError::OptionNameIsInvalidUtf8 | OptionsError::OptionValueIsInvalidUtf8 => {
                ErrorReport::new(
                    PgSqlErrorCode::ERRCODE_FDW_INVALID_STRING_FORMAT,
                    error_message,
                    "",
                )
            }
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
/// # let options = &HashMap::new();
/// require_option("my_option", options);
/// ```
pub fn require_option<'map>(
    opt_name: &str,
    options: &'map HashMap<String, String>,
) -> Result<&'map str, OptionsError> {
    options
        .get(opt_name)
        .map(|t| t.as_ref())
        .ok_or(OptionsError::OptionNameNotFound(opt_name.to_string()))
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
/// require_option_or("my_option", options, "default value".to_string());
/// ```
pub fn require_option_or(
    opt_name: &str,
    options: &HashMap<String, String>,
    default: String,
) -> String {
    options
        .get(opt_name)
        .map(|t| t.to_owned())
        .unwrap_or(default)
}

/// Check if the option list contains a specific option, used in [validator](crate::interface::ForeignDataWrapper::validator)
pub fn check_options_contain(opt_list: &[Option<String>], tgt: &str) {
    let search_key = tgt.to_owned() + "=";
    if !opt_list.iter().any(|opt| {
        if let Some(s) = opt {
            s.starts_with(&search_key)
        } else {
            false
        }
    }) {
        report_error(
            PgSqlErrorCode::ERRCODE_FDW_OPTION_NAME_NOT_FOUND,
            &format!("required option \"{}\" is not specified", tgt),
        );
    }
}

// convert options definition to hashmap
pub(super) unsafe fn options_to_hashmap(options: *mut pg_sys::List) -> HashMap<String, String> {
    let mut ret = HashMap::new();
    let options: PgList<pg_sys::DefElem> = PgList::from_pg(options);
    for option in options.iter_ptr() {
        let name = CStr::from_ptr((*option).defname);
        let value = CStr::from_ptr(pg_sys::defGetString(option));
        ret.insert(
            name.to_str().unwrap().to_owned(),
            value.to_str().unwrap().to_owned(),
        );
    }
    ret
}
