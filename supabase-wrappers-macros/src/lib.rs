extern crate proc_macro;
use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote, ToTokens, TokenStreamExt};
use syn::{parse_macro_input, punctuated::Punctuated, ItemStruct, Lit, MetaNameValue, Token};

/// Create necessary handler, validator and meta functions for foreign data wrapper
///
/// This macro will create three functions which can be used in Postgres.
///
/// 1. `<snake_case_fdw_name>_fdw_handler()` - foreign data wrapper handler function
/// 2. `<snake_case_fdw_name>_fdw_validator()` - foreign data wrapper validator function
/// 3. `<snake_case_fdw_name>_fdw_meta()` - function to return a table contains fdw metadata
///
/// # Example
///
/// ```rust,no_run
/// use supabase_wrappers::prelude::*;
///
/// #[wrappers_fdw(
///     version = "0.1.0",
///     author = "Supabase",
///     website = "https://github.com/supabase/wrappers/tree/main/wrappers/src/fdw/helloworld_fdw"
/// )]
/// pub struct HelloWorldFdw;
/// ```
///
/// then you can use those functions in Postgres,
///
/// ```sql
/// create extension wrappers;
///
/// create foreign data wrapper helloworld_wrapper
///   handler hello_world_fdw_handler
///   validator hello_world_fdw_validator;
///
/// select * from hello_world_fdw_meta();
/// ```
#[proc_macro_attribute]
pub fn wrappers_fdw(attr: TokenStream, item: TokenStream) -> TokenStream {
    let mut metas = TokenStream2::new();
    let meta_attrs: Punctuated<MetaNameValue, Token![,]> =
        parse_macro_input!(attr with Punctuated::parse_terminated);
    let mut error_type: Option<String> = None;
    for attr in meta_attrs {
        let name = format!("{}", attr.path.segments.first().unwrap().ident);
        if let Lit::Str(val) = attr.lit {
            let value = val.value();
            if name == "version" || name == "author" || name == "website" {
                metas.append_all(quote! {
                    meta.insert(#name.to_owned(), #value.to_owned());
                });
            } else if name == "error_type" {
                error_type = Some(value);
            }
        }
    }

    let error_type_ident = if let Some(error_type) = error_type {
        format_ident!("{}", error_type)
    } else {
        let quoted = quote! {
            compile_error!("Missing `error_type` in the `wrappers_fdw` attribute");
        };
        return quoted.into();
    };

    let item: ItemStruct = parse_macro_input!(item as ItemStruct);
    let item_tokens = item.to_token_stream();
    let ident = item.ident;
    let ident_str = ident.to_string();
    let ident_snake = to_snake_case(ident_str.as_str());

    let module_ident = format_ident!("__{}_pgrx", ident_snake);
    let fn_ident = format_ident!("{}_handler", ident_snake);
    let fn_validator_ident = format_ident!("{}_validator", ident_snake);
    let fn_meta_ident = format_ident!("{}_meta", ident_snake);

    let quoted = quote! {
        #item_tokens

        mod #module_ident {
            use super::#ident;
            use std::collections::HashMap;
            use pgrx::pg_sys::panic::{ErrorReport, ErrorReportable};
            use pgrx::prelude::*;
            use supabase_wrappers::prelude::*;

            #[pg_extern(create_or_replace)]
            fn #fn_ident() -> supabase_wrappers::FdwRoutine {
                #ident::fdw_routine()
            }

            #[pg_extern(create_or_replace)]
            fn #fn_validator_ident(options: Vec<Option<String>>, catalog: Option<pg_sys::Oid>) {
                #ident::validator(options, catalog)
                    .map_err(|e| <super::#error_type_ident as Into<ErrorReport>>::into(e))
                    .report();
            }

            #[pg_extern(create_or_replace)]
            fn #fn_meta_ident() -> TableIterator<'static, (
                name!(name, Option<String>),
                name!(version, Option<String>),
                name!(author, Option<String>),
                name!(website, Option<String>)
            )> {
                let mut meta: HashMap<String, String> = HashMap::new();

                #metas

                TableIterator::new(vec![(
                    Some(#ident_str.to_owned()),
                    meta.get("version").map(|s| s.to_owned()),
                    meta.get("author").map(|s| s.to_owned()),
                    meta.get("website").map(|s| s.to_owned()),
                )].into_iter())
            }
        }

    };

    quoted.into()
}

fn to_snake_case(s: &str) -> String {
    let mut acc = String::new();
    let mut prev = '_';
    for ch in s.chars() {
        if ch.is_uppercase() && prev != '_' {
            acc.push('_');
        }
        acc.push(ch);
        prev = ch;
    }
    acc.to_lowercase()
}
