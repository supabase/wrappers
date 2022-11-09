use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote, ToTokens, TokenStreamExt};
use syn::{punctuated::Punctuated, Token};

use crate::FdwType;

fn to_tokens(fdw_types: &Punctuated<FdwType, Token![,]>) -> TokenStream2 {
    let mut fdw_meta_tokens = TokenStream2::new();

    for fdw_type in fdw_types {
        let fdw = &fdw_type.fdw;
        let name = format_ident!("{}", fdw);
        let meta = match fdw_type.attr {
            Some(ref attr) => {
                quote! {
                    cfg_if::cfg_if! {
                        if #attr {
                            metas.push(crate::#name::_wrappers_meta());
                        }
                    }
                }
            }
            None => quote! {
                metas.push(#name);
            },
        };
        fdw_meta_tokens.append_all(meta);
    }

    quote! {
        use pgx::*;
        use pgx::iter::*;
        use pg_sys::*;

        // Wrappers' metadata
        #[derive(Copy, Clone, Default)]
        pub(super) struct WrappersMeta {
        }

        unsafe impl PGXSharedMemory for WrappersMeta {}

        pub(super) static WRAPPERS_META: PgLwLock<WrappersMeta> = PgLwLock::new();

        #[pg_guard]
        pub extern "C" fn _PG_init() {
            pg_shmem_init!(WRAPPERS_META);
        }

        #[pg_extern]
        fn wrappers_meta() -> TableIterator<'static, (
            name!(fdw, Option<String>),
            name!(version, Option<String>),
            name!(author, Option<String>),
            name!(website, Option<String>),
        )> {
            let mut metas = Vec::new();
            #fdw_meta_tokens

            TableIterator::new(
                metas
                    .into_iter()
                    .map(|value| (
                        value.get("fdw").map(|s| s.to_owned()),
                        value.get("version").map(|s| s.to_owned()),
                        value.get("author").map(|s| s.to_owned()),
                        value.get("website").map(|s| s.to_owned()),
                    )),
            )
        }
    }
}

pub(crate) struct Meta {
    fdw_types: Punctuated<FdwType, Token![,]>,
}

impl Meta {
    pub(crate) fn new(fdw_types: Punctuated<FdwType, Token![,]>) -> Self {
        Self { fdw_types }
    }
}

impl ToTokens for Meta {
    fn to_tokens(&self, tokens: &mut TokenStream2) {
        tokens.append_all(to_tokens(&self.fdw_types));
    }
}
