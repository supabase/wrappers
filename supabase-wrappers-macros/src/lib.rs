//! A macro to set up facility functions for Postgres FDW development.
//!
//! This crate is NOT supposed to be used directly, please use [supabase-wrappers](https://github.com/supabase/wrappers/tree/main/supabase-wrappers) instead.
//!
//!  See more details about [Wrappers](https://github.com/supabase/wrappers).

extern crate proc_macro;
use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{quote, ToTokens, TokenStreamExt};
use syn::{
    parse::{Parse, ParseStream, Result},
    parse_macro_input, Attribute, Error, Ident, Meta, NestedMeta,
};

mod instance;
mod limit;
mod modify;
mod polyfill;
mod qual;
mod scan;
mod sort;
mod utils;

use crate::{instance::Instance, modify::Modify, polyfill::Polyfill, scan::Scan, utils::Utils};

// Parse a FDW type with optional feature config attributes
//
// #[cfg(feature = "your-feature")]
// MyFdwType
//
pub(crate) struct FdwType {
    attr: Option<Attribute>,
    fdw: Ident,
}

impl Parse for FdwType {
    fn parse(input: ParseStream) -> Result<Self> {
        let attrs = input.call(Attribute::parse_outer)?;
        let attr = if attrs.is_empty() {
            None
        } else if attrs.len() == 1 {
            let attr = attrs.first().unwrap();
            let meta = attr.parse_meta()?;
            let err = Error::new_spanned(attr, "expect #[cfg(feature = \"...\")] attribute");
            match meta {
                Meta::List(meta_list) => {
                    let segments = &meta_list.path.segments;
                    if segments.len() != 1 || segments.first().unwrap().ident != "cfg" {
                        return Err(err);
                    }
                    let nested = &meta_list.nested;
                    if nested.len() != 1 {
                        return Err(err);
                    }
                    match nested.first().unwrap() {
                        NestedMeta::Meta(meta) => match meta {
                            Meta::NameValue(nv) => {
                                let segments = &nv.path.segments;
                                if segments.len() != 1
                                    || segments.first().unwrap().ident != "feature"
                                {
                                    return Err(err);
                                }
                            }
                            _ => {
                                return Err(err);
                            }
                        },
                        _ => {
                            return Err(err);
                        }
                    }
                }
                _ => {
                    return Err(err);
                }
            }
            Some(attr.clone())
        } else {
            let last_attr = attrs.last().unwrap().clone();
            let err = Error::new_spanned(last_attr, "only one feature config allowd");
            return Err(err);
        };
        let fdw = input.parse()?;
        Ok(Self { attr, fdw })
    }
}

// All the magic come from here :-)
struct WrappersMagic {
    instance: Instance,
}

impl Parse for WrappersMagic {
    fn parse(input: ParseStream) -> Result<Self> {
        let fdw_types = input.parse_terminated(FdwType::parse)?;
        let instance = Instance::new(fdw_types);
        Ok(Self { instance })
    }
}

impl ToTokens for WrappersMagic {
    fn to_tokens(&self, tokens: &mut TokenStream2) {
        let instance = &self.instance;
        let polyfill = Polyfill {};
        let utils = Utils {};
        let scan = Scan {};
        let modify = Modify {};

        let combined = quote! {
            pgx::pg_module_magic!();

            mod _supabase_wrappers {
                use pgx::*;
                use pg_sys::*;
                use std::collections::HashMap;
                use ::supabase_wrappers::{Cell, ForeignDataWrapper, Row, Qual, Sort, Limit, report_error};

                mod polyfill {
                    #polyfill
                }

                mod utils {
                    #utils
                }

                mod instance {
                    #instance
                }

                mod scan {
                    #scan
                }

                mod modify {
                    #modify
                }

                #[pg_extern]
                fn wrappers_handler() -> PgBox<FdwRoutine> {
                    let mut fdw_routine = PgBox::<FdwRoutine>::alloc_node(NodeTag_T_FdwRoutine);

                    // plan phase
                    fdw_routine.GetForeignRelSize = Some(scan::get_foreign_rel_size);
                    fdw_routine.GetForeignPaths = Some(scan::get_foreign_paths);
                    fdw_routine.GetForeignPlan = Some(scan::get_foreign_plan);
                    fdw_routine.ExplainForeignScan = Some(scan::explain_foreign_scan);

                    // scan phase
                    fdw_routine.BeginForeignScan = Some(scan::begin_foreign_scan);
                    fdw_routine.IterateForeignScan = Some(scan::iterate_foreign_scan);
                    fdw_routine.ReScanForeignScan = Some(scan::re_scan_foreign_scan);
                    fdw_routine.EndForeignScan = Some(scan::end_foreign_scan);

                    // modify phase
                    fdw_routine.AddForeignUpdateTargets = Some(modify::add_foreign_update_targets);
                    fdw_routine.PlanForeignModify = Some(modify::plan_foreign_modify);
                    fdw_routine.BeginForeignModify = Some(modify::begin_foreign_modify);
                    fdw_routine.ExecForeignInsert = Some(modify::exec_foreign_insert);
                    fdw_routine.ExecForeignDelete = Some(modify::exec_foreign_delete);
                    fdw_routine.ExecForeignUpdate = Some(modify::exec_foreign_update);
                    fdw_routine.EndForeignModify = Some(modify::end_foreign_modify);

                    fdw_routine.into_pg_boxed()
                }

                #[pg_extern]
                fn wrappers_validator(opt_list: Vec<Option<String>>, catalog: Option<pg_sys::Oid>) {
                    // only check if mandatory options exist
                    if let Some(oid) = catalog {
                        match oid {
                            polyfill::FOREIGN_DATA_WRAPPER_RELATION_ID => {
                                utils::check_options_contain(&opt_list, "wrapper");
                            }
                            polyfill::FOREIGN_SERVER_RELATION_ID => {}
                            polyfill::FOREIGN_TABLE_RELATION_ID => {}
                            _ => {}
                        }
                    }
                }
            }
        };

        tokens.append_all(combined);
    }
}

/// Set up all required facilities for Postgres FDW development.
///
/// This macro will create all the required callback routines and set them up
/// with your Postgres FDW developed with `Wrappers`.
///
/// You don't need to call [`pgx::pg_module_magic!()`](https://docs.rs/pgx/latest/pgx/macro.pg_module_magic.html) as this macro will call it for you.
///
/// This macro should be called only once.
///
/// # Example
///
/// Suppose you developed two FDWs `FooFdw` and `BarFdw`, then you can use them
/// like below,
///
/// ```rust,no_run
/// use supabase_wrappers::wrappers_magic;
/// use crate::{FooFdw, BarFdw};
///
/// // use a single FDW
/// wrappers_magic!(FooFdw);
///
/// // or use multiple FDWs
/// wrappers_magic!(FooFdw, BarFdw);
/// ```
///
/// Feature based conditional compilation is also supported.
///
/// ```rust,no_run
/// #[cfg(feature = "foo_fdw")]
/// use crate::FooFdw;
///
/// #[cfg(feature = "bar_fdw")]
/// use crate::BarFdw;
///
/// wrappers_magic!(
///     #[cfg(feature = "foo_fdw")]
///     FooFdw,
///
///     #[cfg(feature = "bar_fdw")]
///     BarFdw,
/// );
/// ```
#[proc_macro]
pub fn wrappers_magic(input: TokenStream) -> TokenStream {
    let magic: WrappersMagic = parse_macro_input!(input as WrappersMagic);
    quote! {
        #magic
    }
    .into()
}
