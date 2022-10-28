use proc_macro2::TokenStream as TokenStream2;
use quote::{quote, ToTokens, TokenStreamExt};
use syn::{punctuated::Punctuated, Token};

use crate::FdwType;

fn to_tokens(fdw_types: &Punctuated<FdwType, Token![,]>) -> TokenStream2 {
    let mut fdw_inst_tokens = TokenStream2::new();

    for fdw_type in fdw_types {
        let fdw = &fdw_type.fdw;
        let name = format!("{}", fdw);
        let inst = match fdw_type.attr {
            Some(ref attr) => {
                quote! {
                    #name => {
                        cfg_if::cfg_if! {
                            if #attr {
                                Box::new(<crate::#fdw>::new(&fserver_opts))
                            } else {
                                report_error(
                                    PgSqlErrorCode::ERRCODE_FDW_ERROR,
                                    &format!(
                                        "foreign data wrapper '{}' not implemented",
                                        wrapper.as_str()
                                    ),
                                );
                                Box::new(DummyFdw {})
                            }
                        }
                    }
                }
            }
            None => quote! {
                #name => Box::new(<crate::#fdw>::new(&fserver_opts)),
            },
        };
        fdw_inst_tokens.append_all(inst);
    }

    quote! {
        use pg_sys::*;
        use pgx::*;
        use std::collections::HashMap;
        use ::supabase_wrappers::{ForeignDataWrapper, Cell, Row, Qual, Value, Sort, Limit, report_error};

        use super::utils;

        // a dummy fdw does nothing
        struct DummyFdw;

        impl ForeignDataWrapper for DummyFdw {
            fn begin_scan(
                &mut self,
                _quals: &Vec<Qual>,
                _columns: &Vec<String>,
                _sorts: &Vec<Sort>,
                _limit: &Option<Limit>,
                _options: &HashMap<String, String>,
            ) {
            }

            fn iter_scan(&mut self) -> Option<Row> {
                None
            }

            fn end_scan(&mut self) {}
        }

        // create a fdw instance
        pub(super) unsafe fn create_fdw_instance(ftable_id: Oid) -> Box<dyn ForeignDataWrapper> {
            let ftable = GetForeignTable(ftable_id);
            let fserver = GetForeignServer((*ftable).serverid);
            let fserver_opts = utils::options_to_hashmap((*fserver).options);
            let fdw = GetForeignDataWrapper((*fserver).fdwid);
            let opts = utils::options_to_hashmap((*fdw).options);
            let wrapper = opts.get("wrapper").unwrap();

            match wrapper.as_str() {
                #fdw_inst_tokens
                _ => {
                    report_error(
                        PgSqlErrorCode::ERRCODE_FDW_ERROR,
                        &format!(
                            "foreign data wrapper '{}' not implemented",
                            wrapper.as_str()
                        ),
                    );
                    Box::new(DummyFdw {})
                }
            }
        }
    }
}

pub(crate) struct Instance {
    fdw_types: Punctuated<FdwType, Token![,]>,
}

impl Instance {
    pub(crate) fn new(fdw_types: Punctuated<FdwType, Token![,]>) -> Self {
        Self { fdw_types }
    }
}

impl ToTokens for Instance {
    fn to_tokens(&self, tokens: &mut TokenStream2) {
        tokens.append_all(to_tokens(&self.fdw_types));
    }
}
