use crate::interface::Sort;
use pgrx::list::List;
use pgrx::{is_a, pg_sys};
use std::ffi::CStr;
use std::ffi::c_void;

pub(crate) unsafe fn create_sort(
    pathkey: *mut pg_sys::PathKey,
    var: *mut pg_sys::Var,
    baserel_id: pg_sys::Oid,
) -> Option<Sort> {
    unsafe {
        let attno = (*var).varattno;
        let attname = pg_sys::get_attname(baserel_id, attno, true);
        if !attname.is_null() {
            let sort = Sort {
                field: CStr::from_ptr(attname).to_str().unwrap().to_owned(),
                field_no: attno as usize,
                #[cfg(feature = "pg18")]
                reversed: (*pathkey).pk_cmptype == pg_sys::BTGreaterStrategyNumber,
                #[cfg(not(feature = "pg18"))]
                reversed: (*pathkey).pk_strategy as u32 == pg_sys::BTGreaterStrategyNumber,
                nulls_first: (*pathkey).pk_nulls_first,
                ..Default::default()
            };
            return Some(sort);
        }
        None
    }
}

// extract sorts
pub(crate) unsafe fn extract_sorts(
    root: *mut pg_sys::PlannerInfo,
    baserel: *mut pg_sys::RelOptInfo,
    baserel_id: pg_sys::Oid,
) -> Vec<Sort> {
    unsafe {
        pgrx::memcx::current_context(|mcx| {
            let mut ret = Vec::new();

            if let Some(pathkeys) =
                List::<*mut c_void>::downcast_ptr_in_memcx((*root).query_pathkeys, mcx)
            {
                for pathkey in pathkeys.iter() {
                    let ec = (*(*pathkey as *mut pg_sys::PathKey)).pk_eclass;

                    if (*ec).ec_has_volatile {
                        continue;
                    }

                    if let Some(ems) =
                        List::<*mut c_void>::downcast_ptr_in_memcx((*ec).ec_members, mcx)
                    {
                        ems.iter()
                            .find(|em| {
                                pg_sys::bms_equal(
                                    (*(**em as *mut pg_sys::EquivalenceMember)).em_relids,
                                    (*baserel).relids,
                                )
                            })
                            .and_then(|em| {
                                let expr = (*(*em as *mut pg_sys::EquivalenceMember)).em_expr
                                    as *mut pg_sys::Node;

                                if is_a(expr, pg_sys::NodeTag::T_Var) {
                                    let var = expr as *mut pg_sys::Var;
                                    let sort = create_sort(*pathkey as _, var, baserel_id);
                                    if let Some(sort) = sort {
                                        ret.push(sort);
                                    }
                                } else if is_a(expr, pg_sys::NodeTag::T_RelabelType) {
                                    // ORDER BY clauses having a COLLATE option will be RelabelType
                                    let expr = expr as *mut pg_sys::RelabelType;
                                    let var = (*expr).arg as *mut pg_sys::Var;
                                    if is_a(var as *mut pg_sys::Node, pg_sys::NodeTag::T_Var) {
                                        let sort = create_sort(*pathkey as _, var, baserel_id);
                                        if let Some(mut sort) = sort {
                                            let coll_id = (*expr).resultcollid;
                                            sort.collate = Some(
                                                CStr::from_ptr(pg_sys::get_collation_name(coll_id))
                                                    .to_str()
                                                    .unwrap()
                                                    .to_owned(),
                                            );
                                            ret.push(sort);
                                        }
                                    }
                                }

                                None::<()>
                            });
                    }
                }
            }

            ret
        })
    }
}
