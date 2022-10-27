use proc_macro2::TokenStream as TokenStream2;
use quote::{quote, ToTokens, TokenStreamExt};

fn to_tokens() -> TokenStream2 {
    quote! {
        unsafe fn create_sort(
            pathkey: *mut PathKey,
            var: *mut Var,
            baserel_id: Oid,
        ) -> Option<Sort> {
            let attno = (*var).varattno;
            let attname = get_attname(baserel_id, attno, true);
            if !attname.is_null() {
                let mut sort = Sort::default();
                sort.field = CStr::from_ptr(attname).to_str().unwrap().to_owned();
                sort.field_no = attno as usize;
                sort.reversed =
                    (*pathkey).pk_strategy as u32 == BTGreaterStrategyNumber;
                sort.nulls_first = (*pathkey).pk_nulls_first;
                return Some(sort);
            }
            None
        }

        // extract sorts
        unsafe fn extract_sorts(
            root: *mut PlannerInfo,
            baserel: *mut RelOptInfo,
            baserel_id: Oid,
        ) -> Vec<Sort> {
            let mut ret = Vec::new();
            let pathkeys: PgList<PathKey> = PgList::from_pg((*root).query_pathkeys);

            for pathkey in pathkeys.iter_ptr() {
                let ec = (*pathkey).pk_eclass;

                if (*ec).ec_has_volatile {
                    continue;
                }

                let ems: PgList<EquivalenceMember> = PgList::from_pg((*ec).ec_members);
                ems.iter_ptr()
                    .find(|em| bms_equal((*(*em)).em_relids, (*baserel).relids))
                    .and_then(|em| {
                        let expr = (*em).em_expr as *mut Node;

                        if is_a(expr, NodeTag_T_Var) {
                            let var = expr as *mut Var;
                            let sort = create_sort(pathkey, var, baserel_id);
                            if sort.is_some() {
                                ret.push(sort.unwrap());
                            }
                        } else if is_a(expr, NodeTag_T_RelabelType) {
                            // ORDER BY clauses having a COLLATE option will be RelabelType
                            let expr = expr as *mut RelabelType;
                            let var = (*expr).arg as *mut Var;
                            if is_a(var as *mut Node, NodeTag_T_Var) {
                                let sort = create_sort(pathkey, var, baserel_id);
                                if sort.is_some() {
                                    let mut sort = sort.unwrap();
                                    let coll_id = (*expr).resultcollid;
                                    sort.collate = Some(CStr::from_ptr(get_collation_name(coll_id)).to_str().unwrap().to_owned());
                                    ret.push(sort);
                                }
                            }
                        }

                        None::<()>
                    });
            }

            ret
        }
    }
}

pub(crate) struct Sort;

impl ToTokens for Sort {
    fn to_tokens(&self, tokens: &mut TokenStream2) {
        tokens.append_all(to_tokens());
    }
}
