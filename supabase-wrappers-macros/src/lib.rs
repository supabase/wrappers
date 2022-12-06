extern crate proc_macro;
use proc_macro::TokenStream;
use quote::{format_ident, quote, ToTokens};
use syn::{parse_macro_input, ItemStruct};

#[proc_macro_attribute]
pub fn wrappers_fdw(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let item: ItemStruct = parse_macro_input!(item as ItemStruct);
    let item_tokens = item.to_token_stream();
    let ident = item.ident;
    let ident_str = ident.to_string();
    let ident_snake = to_snake_case(ident_str.as_str());

    let module_ident = format_ident!("__{}_pgx", ident_snake);
    let fn_ident = format_ident!("{}_handler", ident_snake);
    let fn_validator_ident = format_ident!("{}_validator", ident_snake);

    let quoted = quote! {
        #item_tokens

        mod #module_ident {
            use super::#ident;
            use pgx::prelude::*;
            use supabase_wrappers::prelude::*;

            #[pg_extern]
            fn #fn_ident() -> supabase_wrappers::FdwRoutine {
                #ident::fdw_routine()
            }

            #[pg_extern]
            fn #fn_validator_ident(options: Vec<Option<String>>, catalog: Option<pg_sys::Oid>) {
                #ident::validator(options, catalog)
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
