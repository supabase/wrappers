use crate::interface::Sort;
use pgrx::{is_a, list::PgList, pg_sys};
use std::ffi::CStr;

pub(crate) unsafe fn create_sort(
    pathkey: *mut pg_sys::PathKey,
    var: *mut pg_sys::Var,
    baserel_id: pg_sys::Oid,
) -> Option<Sort> {
    let attno = (*var).varattno;
    let attname = pg_sys::get_attname(baserel_id, attno, true);
    if !attname.is_null() {
        let sort = Sort {
            field: CStr::from_ptr(attname).to_str().unwrap().to_owned(),
            field_no: attno as usize,
            reversed: (*pathkey).pk_strategy as u32 == pg_sys::BTGreaterStrategyNumber,
            nulls_first: (*pathkey).pk_nulls_first,
            ..Default::default()
        };
        return Some(sort);
    }
    None
}

// extract sorts
pub(crate) unsafe fn extract_sorts(
    root: *mut pg_sys::PlannerInfo,
    baserel: *mut pg_sys::RelOptInfo,
    baserel_id: pg_sys::Oid,
) -> Vec<Sort> {
    let mut ret = Vec::new();
    let pathkeys: PgList<pg_sys::PathKey> = PgList::from_pg((*root).query_pathkeys);

    for pathkey in pathkeys.iter_ptr() {
        let ec = (*pathkey).pk_eclass;

        if (*ec).ec_has_volatile {
            continue;
        }

        let ems: PgList<pg_sys::EquivalenceMember> = PgList::from_pg((*ec).ec_members);
        ems.iter_ptr()
            .find(|em| pg_sys::bms_equal((*(*em)).em_relids, (*baserel).relids))
            .and_then(|em| {
                let expr = (*em).em_expr as *mut pg_sys::Node;

                if is_a(expr, pg_sys::NodeTag::T_Var) {
                    let var = expr as *mut pg_sys::Var;
                    let sort = create_sort(pathkey, var, baserel_id);
                    if let Some(sort) = sort {
                        ret.push(sort);
                    }
                } else if is_a(expr, pg_sys::NodeTag::T_RelabelType) {
                    // ORDER BY clauses having a COLLATE option will be RelabelType
                    let expr = expr as *mut pg_sys::RelabelType;
                    let var = (*expr).arg as *mut pg_sys::Var;
                    if is_a(var as *mut pg_sys::Node, pg_sys::NodeTag::T_Var) {
                        let sort = create_sort(pathkey, var, baserel_id);
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

    ret
}
