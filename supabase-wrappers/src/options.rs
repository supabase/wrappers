use pgrx::list::List;
use pgrx::pg_sys::panic::ErrorReport;
use pgrx::{PgSqlErrorCode, pg_sys};
use std::collections::HashMap;
use std::ffi::CStr;
use std::ffi::c_void;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum OptionsError {
    #[error("required option `{0}` is not specified")]
    OptionNameNotFound(String),
    #[error("option name `{0}` is not a valid UTF-8 string")]
    OptionNameIsInvalidUtf8(String),
    #[error("option value for `{option_name}` is not a valid UTF-8 string")]
    OptionValueIsInvalidUtf8 {
        option_name: String,
        // NOTE: We intentionally don't include the actual value here
        // to prevent credential leakage in error messages
    },
    #[error("option `{option_name}` cannot be parsed as {type_name}")]
    OptionParsingError {
        option_name: String,
        type_name: &'static str,
    },
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
            | OptionsError::OptionValueIsInvalidUtf8 { .. } => ErrorReport::new(
                PgSqlErrorCode::ERRCODE_FDW_INVALID_STRING_FORMAT,
                error_message,
                "",
            ),
            OptionsError::OptionParsingError { .. } => {
                ErrorReport::new(PgSqlErrorCode::ERRCODE_FDW_ERROR, error_message, "")
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
    let search_key = format!("{tgt}=");
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
    pgrx::memcx::current_context(|mcx| {
        let mut ret = HashMap::new();

        if let Some(options) = unsafe { List::<*mut c_void>::downcast_ptr_in_memcx(options, mcx) } {
            for option in options.iter() {
                let option = *option as *mut pg_sys::DefElem;
                let name = unsafe { CStr::from_ptr((*option).defname) };
                let value = unsafe { CStr::from_ptr(pg_sys::defGetString(option)) };
                let name = name.to_str().map_err(|_| {
                    OptionsError::OptionNameIsInvalidUtf8(
                        String::from_utf8_lossy(name.to_bytes()).to_string(),
                    )
                })?;
                // SECURITY: Don't include the actual value in error messages
                // to prevent leaking credentials for sensitive options
                let value = value
                    .to_str()
                    .map_err(|_| OptionsError::OptionValueIsInvalidUtf8 {
                        option_name: name_str.to_string(),
                    })?;
                let name = name_str;
                ret.insert(name.to_string(), value.to_string());
            }
        }

        Ok(ret)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    // ==========================================================================
    // Tests for OptionsError
    // ==========================================================================

    #[test]
    fn test_option_name_not_found_error_message() {
        let err = OptionsError::OptionNameNotFound("test_option".to_string());
        assert_eq!(
            format!("{err}"),
            "required option `test_option` is not specified"
        );
    }

    #[test]
    fn test_option_name_invalid_utf8_error_message() {
        let err = OptionsError::OptionNameIsInvalidUtf8("bad_name".to_string());
        assert_eq!(
            format!("{err}"),
            "option name `bad_name` is not a valid UTF-8 string"
        );
    }

    #[test]
    fn test_option_value_invalid_utf8_error_message() {
        let err = OptionsError::OptionValueIsInvalidUtf8 {
            option_name: "password".to_string(),
        };
        assert_eq!(
            format!("{err}"),
            "option value for `password` is not a valid UTF-8 string"
        );
    }

    #[test]
    fn test_option_parsing_error_message() {
        let err = OptionsError::OptionParsingError {
            option_name: "max_size".to_string(),
            type_name: "usize",
        };
        assert_eq!(
            format!("{err}"),
            "option `max_size` cannot be parsed as usize"
        );
    }

    // ==========================================================================
    // Tests for require_option
    // ==========================================================================

    #[test]
    fn test_require_option_found() {
        let mut options = HashMap::new();
        options.insert("api_key".to_string(), "my_key".to_string());

        let result = require_option("api_key", &options);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "my_key");
    }

    #[test]
    fn test_require_option_not_found() {
        let options = HashMap::new();

        let result = require_option("api_key", &options);
        assert!(result.is_err());
        match result {
            Err(OptionsError::OptionNameNotFound(name)) => {
                assert_eq!(name, "api_key");
            }
            _ => panic!("Expected OptionNameNotFound error"),
        }
    }

    // ==========================================================================
    // Tests for require_option_or
    // ==========================================================================

    #[test]
    fn test_require_option_or_found() {
        let mut options = HashMap::new();
        options.insert("region".to_string(), "us-west-2".to_string());

        let result = require_option_or("region", &options, "us-east-1");
        assert_eq!(result, "us-west-2");
    }

    #[test]
    fn test_require_option_or_not_found_uses_default() {
        let options = HashMap::new();

        let result = require_option_or("region", &options, "us-east-1");
        assert_eq!(result, "us-east-1");
    }

    // ==========================================================================
    // Tests for check_options_contain
    // ==========================================================================

    #[test]
    fn test_check_options_contain_found() {
        let opt_list = vec![
            Some("api_key=abc123".to_string()),
            Some("region=us-west-2".to_string()),
        ];

        let result = check_options_contain(&opt_list, "api_key");
        assert!(result.is_ok());
    }

    #[test]
    fn test_check_options_contain_not_found() {
        let opt_list = vec![
            Some("region=us-west-2".to_string()),
            Some("bucket=mybucket".to_string()),
        ];

        let result = check_options_contain(&opt_list, "api_key");
        assert!(result.is_err());
        match result {
            Err(OptionsError::OptionNameNotFound(name)) => {
                assert_eq!(name, "api_key");
            }
            _ => panic!("Expected OptionNameNotFound error"),
        }
    }

    #[test]
    fn test_check_options_contain_with_none() {
        let opt_list: Vec<Option<String>> = vec![None, Some("api_key=abc123".to_string()), None];

        let result = check_options_contain(&opt_list, "api_key");
        assert!(result.is_ok());
    }

    #[test]
    fn test_check_options_contain_partial_match_fails() {
        // "api_key_secondary" should not match "api_key"
        let opt_list = vec![Some("api_key_secondary=abc123".to_string())];

        // This will actually pass because starts_with("api_key=") won't match "api_key_secondary=abc123"
        let result = check_options_contain(&opt_list, "api_key");
        assert!(result.is_err());
    }
}
