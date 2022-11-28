//! A set of macros to set up facility functions for Postgres FDW development
//! framework `Wrappers`.
//!
//! This crate is NOT supposed to be used directly, please use [supabase-wrappers](https://github.com/supabase/wrappers/tree/main/supabase-wrappers) instead.
//!
//!  See more details about [Wrappers](https://github.com/supabase/wrappers).

extern crate proc_macro;
use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote, ToTokens, TokenStreamExt};
use syn::{
    parse::{Parse, ParseStream, Result},
    parse_macro_input,
    punctuated::Punctuated,
    ItemStruct, Lit, MetaNameValue, Token,
};

mod instance;
mod limit;
mod meta;
mod modify;
mod parser;
mod polyfill;
mod qual;
mod scan;
mod sort;
mod utils;

use crate::{
    instance::Instance, meta::Meta as WrappersMeta, modify::Modify, parser::FdwType,
    polyfill::Polyfill, scan::Scan, utils::Utils,
};

// All the magic come from here :-)
struct WrappersMagic {
    fdw_types: Punctuated<FdwType, Token![,]>,
}

impl Parse for WrappersMagic {
    fn parse(input: ParseStream) -> Result<Self> {
        let fdw_types = input.parse_terminated(FdwType::parse)?;
        Ok(Self { fdw_types })
    }
}

impl ToTokens for WrappersMagic {
    fn to_tokens(&self, tokens: &mut TokenStream2) {
        let instance = Instance::new(self.fdw_types.clone());
        let meta = WrappersMeta::new(self.fdw_types.clone());
        let polyfill = Polyfill {};
        let utils = Utils {};
        let scan = Scan {};
        let modify = Modify {};

        let combined = quote! {
            pgx::pg_module_magic!();

            mod _supabase_wrappers {
                use pgx::prelude::*;
                use std::collections::HashMap;
                use ::supabase_wrappers::prelude::*;

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

                mod meta {
                    #meta
                }

                #[pg_extern]
                fn wrappers_handler() -> PgBox<pg_sys::FdwRoutine> {
                    let mut fdw_routine = PgBox::<pg_sys::FdwRoutine>::alloc_node(pg_sys::NodeTag_T_FdwRoutine);

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

/// Set up metadata for Postgres FDW
///
/// This macro will set up metadata for your Postgres FDW. All metadata value
/// type should be `String`.
///
/// The FDW metadata can be queried once `Wrappers` extension is installed.
///
/// ```sql
/// create extension wrappers;
/// select * from wrappers_meta();
///
///      fdw      | version |  author  |             website
/// --------------+---------+----------+---------------------------------------
/// HelloWorldFdw | 0.1.0   | Supabase | https://github.com/supabase/wrappers/
/// ```
///
/// # Example
///
/// ```rust,no_run
/// use supabase_wrappers::wrappers_meta;
///
/// #[wrappers_meta(
///     version = "0.1.0",
///     author = "Supabase",
///     website = "https://github.com/supabase/wrappers/",
/// )]
/// pub(crate) struct HelloWorldFdw {...}
/// ```
#[proc_macro_attribute]
pub fn wrappers_meta(attr: TokenStream, item: TokenStream) -> TokenStream {
    let mut metas = TokenStream2::new();
    let meta_attrs: Punctuated<MetaNameValue, Token![,]> =
        parse_macro_input!(attr with Punctuated::parse_terminated);
    for attr in meta_attrs {
        let name = format!("{}", attr.path.segments.first().unwrap().ident);
        match attr.lit {
            Lit::Str(val) => {
                let value = val.value();
                let stmt = quote! {
                    ret.insert(#name.to_owned(), #value.to_owned());
                };
                metas.append_all(stmt);
            }
            _ => {}
        }
    }

    let item: ItemStruct = parse_macro_input!(item as ItemStruct);
    let item_name = format!("{}", item.ident);
    let item_ident = format_ident!("{}", item.ident);
    let item_tokens = item.to_token_stream();

    quote! {
        #item_tokens

        impl #item_ident {
            pub(crate) fn _wrappers_meta() -> std::collections::HashMap<String, String> {
                let mut ret = HashMap::new();
                ret.insert("fdw".to_owned(), #item_name.to_owned());
                #metas
                ret
            }
        }
    }
    .into()
}
