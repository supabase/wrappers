use crate::prelude::*;
use pgrx::pg_sys::Oid;
use pgrx::{
    FromDatum, PgBuiltInOids, PgOid,
    datum::{Array, Date, JsonB, Timestamp},
    is_a,
    list::List,
    pg_sys,
    pg_sys::Datum,
};
use std::ffi::CStr;
use std::ffi::c_void;
use std::os::raw::c_int;
use std::ptr;
use std::sync::Mutex;

use crate::interface::Param;

// create array of Cell from constant datum array
pub(crate) unsafe fn form_array_from_datum(
    datum: Datum,
    is_null: bool,
    typoid: pg_sys::Oid,
) -> Option<Vec<Cell>> {
    unsafe {
        let oid = PgOid::from(typoid);
        match oid {
            PgOid::BuiltIn(PgBuiltInOids::BOOLARRAYOID) => {
                Array::<bool>::from_polymorphic_datum(datum, is_null, pg_sys::BOOLOID).map(|arr| {
                    arr.iter()
                        .filter(|v| v.is_some())
                        .map(|v| Cell::Bool(v.expect("non-null array element")))
                        .collect::<Vec<_>>()
                })
            }
            PgOid::BuiltIn(PgBuiltInOids::CHARARRAYOID) => {
                Array::<i8>::from_polymorphic_datum(datum, is_null, pg_sys::CHAROID).map(|arr| {
                    arr.iter()
                        .filter(|v| v.is_some())
                        .map(|v| Cell::I8(v.expect("non-null array element")))
                        .collect::<Vec<_>>()
                })
            }
            PgOid::BuiltIn(PgBuiltInOids::INT2ARRAYOID) => {
                Array::<i16>::from_polymorphic_datum(datum, is_null, pg_sys::INT2OID).map(|arr| {
                    arr.iter()
                        .filter(|v| v.is_some())
                        .map(|v| Cell::I16(v.expect("non-null array element")))
                        .collect::<Vec<_>>()
                })
            }
            PgOid::BuiltIn(PgBuiltInOids::FLOAT4ARRAYOID) => {
                Array::<f32>::from_polymorphic_datum(datum, is_null, pg_sys::FLOAT4OID).map(|arr| {
                    arr.iter()
                        .filter(|v| v.is_some())
                        .map(|v| Cell::F32(v.expect("non-null array element")))
                        .collect::<Vec<_>>()
                })
            }
            PgOid::BuiltIn(PgBuiltInOids::INT4ARRAYOID) => {
                Array::<i32>::from_polymorphic_datum(datum, is_null, pg_sys::INT4OID).map(|arr| {
                    arr.iter()
                        .filter(|v| v.is_some())
                        .map(|v| Cell::I32(v.expect("non-null array element")))
                        .collect::<Vec<_>>()
                })
            }
            PgOid::BuiltIn(PgBuiltInOids::FLOAT8ARRAYOID) => {
                Array::<f64>::from_polymorphic_datum(datum, is_null, pg_sys::FLOAT8OID).map(|arr| {
                    arr.iter()
                        .filter(|v| v.is_some())
                        .map(|v| Cell::F64(v.expect("non-null array element")))
                        .collect::<Vec<_>>()
                })
            }
            PgOid::BuiltIn(PgBuiltInOids::INT8ARRAYOID) => {
                Array::<i64>::from_polymorphic_datum(datum, is_null, pg_sys::INT8OID).map(|arr| {
                    arr.iter()
                        .filter(|v| v.is_some())
                        .map(|v| Cell::I64(v.expect("non-null array element")))
                        .collect::<Vec<_>>()
                })
            }
            PgOid::BuiltIn(PgBuiltInOids::TEXTARRAYOID) => {
                Array::<String>::from_polymorphic_datum(datum, is_null, pg_sys::TEXTOID).map(
                    |arr| {
                        arr.iter()
                            .filter(|v| v.is_some())
                            .map(|v| Cell::String(v.expect("non-null array element")))
                            .collect::<Vec<_>>()
                    },
                )
            }
            PgOid::BuiltIn(PgBuiltInOids::DATEARRAYOID) => {
                Array::<Date>::from_polymorphic_datum(datum, is_null, pg_sys::DATEOID).map(|arr| {
                    arr.iter()
                        .filter(|v| v.is_some())
                        .map(|v| Cell::Date(v.expect("non-null array element")))
                        .collect::<Vec<_>>()
                })
            }
            PgOid::BuiltIn(PgBuiltInOids::TIMESTAMPARRAYOID) => {
                Array::<Timestamp>::from_polymorphic_datum(datum, is_null, pg_sys::TIMESTAMPOID)
                    .map(|arr| {
                        arr.iter()
                            .filter(|v| v.is_some())
                            .map(|v| Cell::Timestamp(v.expect("non-null array element")))
                            .collect::<Vec<_>>()
                    })
            }
            PgOid::BuiltIn(PgBuiltInOids::JSONBARRAYOID) => {
                Array::<JsonB>::from_polymorphic_datum(datum, is_null, pg_sys::JSONBOID).map(
                    |arr| {
                        arr.iter()
                            .filter(|v| v.is_some())
                            .map(|v| Cell::Json(v.expect("non-null array element")))
                            .collect::<Vec<_>>()
                    },
                )
            }
            _ => None,
        }
    }
}

pub(crate) unsafe fn get_operator(opno: pg_sys::Oid) -> pg_sys::Form_pg_operator {
    unsafe {
        let htup = pg_sys::SearchSysCache1(
            pg_sys::SysCacheIdentifier::OPEROID.try_into().unwrap(),
            opno.into(),
        );
        if htup.is_null() {
            pg_sys::ReleaseSysCache(htup);
            pgrx::error!("cache lookup operator {:?} failed", opno);
        }
        let op = pg_sys::GETSTRUCT(htup) as pg_sys::Form_pg_operator;
        pg_sys::ReleaseSysCache(htup);
        op
    }
}

pub(crate) unsafe fn unnest_clause(node: *mut pg_sys::Node) -> *mut pg_sys::Node {
    unsafe {
        if is_a(node, pg_sys::NodeTag::T_RelabelType) {
            (*(node as *mut pg_sys::RelabelType)).arg as _
        } else if is_a(node, pg_sys::NodeTag::T_ArrayCoerceExpr) {
            (*(node as *mut pg_sys::ArrayCoerceExpr)).arg as _
        } else {
            node
        }
    }
}

pub(crate) unsafe fn extract_from_op_expr(
    _root: *mut pg_sys::PlannerInfo,
    baserel_id: pg_sys::Oid,
    baserel_ids: pg_sys::Relids,
    expr: *mut pg_sys::OpExpr,
) -> Option<Qual> {
    unsafe {
        pgrx::memcx::current_context(|mcx| {
            if let Some(args) = List::<*mut c_void>::downcast_ptr_in_memcx((*expr).args, mcx) {
                // only deal with binary operator
                if args.len() != 2 {
                    report_warning("only support binary operator expression");
                    return None;
                }

                // get operator
                let opno = (*expr).opno;
                let opr = get_operator(opno);
                if opr.is_null() {
                    report_warning("operator is empty");
                    return None;
                }

                let mut left = unnest_clause(*args.get(0).unwrap() as _);
                let mut right = unnest_clause(*args.get(1).unwrap() as _);

                // swap operands if needed
                if is_a(right, pg_sys::NodeTag::T_Var)
                    && !is_a(left, pg_sys::NodeTag::T_Var)
                    && (*opr).oprcom != Oid::INVALID
                {
                    std::mem::swap(&mut left, &mut right);
                }

                if is_a(left, pg_sys::NodeTag::T_Var) {
                    let left = left as *mut pg_sys::Var;

                    if pg_sys::bms_is_member((*left).varno as c_int, baserel_ids)
                        && (*left).varattno >= 1
                    {
                        let field = pg_sys::get_attname(baserel_id, (*left).varattno, false);

                        let (value, param) = if is_a(right, pg_sys::NodeTag::T_Const) {
                            let right = right as *mut pg_sys::Const;
                            (
                                Cell::from_polymorphic_datum(
                                    (*right).constvalue,
                                    (*right).constisnull,
                                    (*right).consttype,
                                ),
                                None,
                            )
                        } else if is_a(right, pg_sys::NodeTag::T_Param) {
                            // add a dummy value if this is query parameter, the actual value
                            // will be extracted from execution state
                            let right = right as *mut pg_sys::Param;
                            let param = Param {
                                kind: (*right).paramkind,
                                id: (*right).paramid as _,
                                type_oid: (*right).paramtype,
                                eval_value: Mutex::new(None).into(),
                                expr_eval: ExprEval {
                                    expr: if (*right).paramkind == pg_sys::ParamKind::PARAM_EXEC {
                                        right as _
                                    } else {
                                        ptr::null_mut()
                                    },
                                    expr_state: ptr::null_mut(),
                                },
                            };
                            (Some(Cell::I64(0)), Some(param))
                        } else {
                            (None, None)
                        };

                        if let Some(value) = value {
                            let qual = Qual {
                                field: CStr::from_ptr(field).to_str().unwrap().to_string(),
                                operator: pgrx::name_data_to_str(&(*opr).oprname).to_string(),
                                value: Value::Cell(value),
                                use_or: false,
                                param,
                            };
                            return Some(qual);
                        }
                    }
                }

                if let Some(stm) = pgrx::nodes::node_to_string(expr as _) {
                    report_warning(&format!("unsupported operator expression in qual: {stm}",));
                }
            }

            None
        })
    }
}

pub(crate) unsafe fn extract_from_null_test(
    baserel_id: pg_sys::Oid,
    expr: *mut pg_sys::NullTest,
) -> Option<Qual> {
    unsafe {
        let var = (*expr).arg as *mut pg_sys::Var;
        if !is_a(var as _, pg_sys::NodeTag::T_Var) || (*var).varattno < 1 {
            return None;
        }

        let field = pg_sys::get_attname(baserel_id, (*var).varattno, false);

        let opname = if (*expr).nulltesttype == pg_sys::NullTestType::IS_NULL {
            "is".to_string()
        } else {
            "is not".to_string()
        };

        let qual = Qual {
            field: CStr::from_ptr(field).to_str().unwrap().to_string(),
            operator: opname,
            value: Value::Cell(Cell::String("null".to_string())),
            use_or: false,
            param: None,
        };

        Some(qual)
    }
}

pub(crate) unsafe fn extract_from_scalar_array_op_expr(
    _root: *mut pg_sys::PlannerInfo,
    baserel_id: pg_sys::Oid,
    baserel_ids: pg_sys::Relids,
    expr: *mut pg_sys::ScalarArrayOpExpr,
) -> Option<Qual> {
    unsafe {
        pgrx::memcx::current_context(|mcx| {
            if let Some(args) = List::<*mut c_void>::downcast_ptr_in_memcx((*expr).args, mcx) {
                // only deal with binary operator
                if args.len() != 2 {
                    return None;
                }

                // get operator
                let opno = (*expr).opno;
                let opr = get_operator(opno);
                if opr.is_null() {
                    return None;
                }

                let left = unnest_clause(*args.get(0).unwrap() as _);
                let right = unnest_clause(*args.get(1).unwrap() as _);

                if is_a(left, pg_sys::NodeTag::T_Var) && is_a(right, pg_sys::NodeTag::T_Const) {
                    let left = left as *mut pg_sys::Var;
                    let right = right as *mut pg_sys::Const;

                    if pg_sys::bms_is_member((*left).varno as c_int, baserel_ids)
                        && (*left).varattno >= 1
                    {
                        let field = pg_sys::get_attname(baserel_id, (*left).varattno, false);

                        let value: Option<Vec<Cell>> = form_array_from_datum(
                            (*right).constvalue,
                            (*right).constisnull,
                            (*right).consttype,
                        );
                        if let Some(value) = value {
                            let qual = Qual {
                                field: CStr::from_ptr(field).to_str().unwrap().to_string(),
                                operator: pgrx::name_data_to_str(&(*opr).oprname).to_string(),
                                value: Value::Array(value),
                                use_or: (*expr).useOr,
                                param: None,
                            };
                            return Some(qual);
                        }
                    }
                }

                if let Some(stm) = pgrx::nodes::node_to_string(expr as _) {
                    report_warning(&format!("only support const scalar array in qual: {stm}",));
                }
            }

            None
        })
    }
}

pub(crate) unsafe fn extract_from_var(
    _root: *mut pg_sys::PlannerInfo,
    baserel_id: pg_sys::Oid,
    baserel_ids: pg_sys::Relids,
    var: *mut pg_sys::Var,
) -> Option<Qual> {
    unsafe {
        if (*var).varattno < 1
            || (*var).vartype != pg_sys::BOOLOID
            || !pg_sys::bms_is_member((*var).varno as c_int, baserel_ids)
        {
            return None;
        }

        let field = pg_sys::get_attname(baserel_id, (*var).varattno, false);

        let qual = Qual {
            field: CStr::from_ptr(field).to_str().unwrap().to_string(),
            operator: "=".to_string(),
            value: Value::Cell(Cell::Bool(true)),
            use_or: false,
            param: None,
        };

        Some(qual)
    }
}

pub(crate) unsafe fn extract_from_bool_expr(
    _root: *mut pg_sys::PlannerInfo,
    baserel_id: pg_sys::Oid,
    baserel_ids: pg_sys::Relids,
    expr: *mut pg_sys::BoolExpr,
) -> Option<Qual> {
    unsafe {
        pgrx::memcx::current_context(|mcx| {
            if let Some(args) = List::<*mut c_void>::downcast_ptr_in_memcx((*expr).args, mcx) {
                if (*expr).boolop != pg_sys::BoolExprType::NOT_EXPR || args.len() != 1 {
                    return None;
                }

                let var = *args.get(0).unwrap() as *mut pg_sys::Var;
                if (*var).varattno < 1
                    || (*var).vartype != pg_sys::BOOLOID
                    || !pg_sys::bms_is_member((*var).varno as c_int, baserel_ids)
                {
                    return None;
                }

                let field = pg_sys::get_attname(baserel_id, (*var).varattno, false);

                let qual = Qual {
                    field: CStr::from_ptr(field).to_str().unwrap().to_string(),
                    operator: "=".to_string(),
                    value: Value::Cell(Cell::Bool(false)),
                    use_or: false,
                    param: None,
                };

                return Some(qual);
            }

            None
        })
    }
}

pub(crate) unsafe fn extract_from_boolean_test(
    baserel_id: pg_sys::Oid,
    expr: *mut pg_sys::BooleanTest,
) -> Option<Qual> {
    unsafe {
        let var = (*expr).arg as *mut pg_sys::Var;
        if !is_a(var as _, pg_sys::NodeTag::T_Var) || (*var).varattno < 1 {
            return None;
        }

        let field = pg_sys::get_attname(baserel_id, (*var).varattno, false);

        let (opname, value) = match (*expr).booltesttype {
            pg_sys::BoolTestType::IS_TRUE => ("is".to_string(), true),
            pg_sys::BoolTestType::IS_FALSE => ("is".to_string(), false),
            pg_sys::BoolTestType::IS_NOT_TRUE => ("is not".to_string(), true),
            pg_sys::BoolTestType::IS_NOT_FALSE => ("is not".to_string(), false),
            _ => return None,
        };

        let qual = Qual {
            field: CStr::from_ptr(field).to_str().unwrap().to_string(),
            operator: opname,
            value: Value::Cell(Cell::Bool(value)),
            use_or: false,
            param: None,
        };

        Some(qual)
    }
}

pub(crate) unsafe fn extract_quals(
    root: *mut pg_sys::PlannerInfo,
    baserel: *mut pg_sys::RelOptInfo,
    baserel_id: pg_sys::Oid,
) -> Vec<Qual> {
    unsafe {
        pgrx::memcx::current_context(|mcx| {
            let mut quals = Vec::new();

            if let Some(conds) =
                List::<*mut c_void>::downcast_ptr_in_memcx((*baserel).baserestrictinfo, mcx)
            {
                for cond in conds.iter() {
                    let expr = (*(*cond as *mut pg_sys::RestrictInfo)).clause as *mut pg_sys::Node;
                    let extracted = if is_a(expr, pg_sys::NodeTag::T_OpExpr) {
                        extract_from_op_expr(root, baserel_id, (*baserel).relids, expr as _)
                    } else if is_a(expr, pg_sys::NodeTag::T_NullTest) {
                        extract_from_null_test(baserel_id, expr as _)
                    } else if is_a(expr, pg_sys::NodeTag::T_ScalarArrayOpExpr) {
                        extract_from_scalar_array_op_expr(
                            root,
                            baserel_id,
                            (*baserel).relids,
                            expr as _,
                        )
                    } else if is_a(expr, pg_sys::NodeTag::T_Var) {
                        extract_from_var(root, baserel_id, (*baserel).relids, expr as _)
                    } else if is_a(expr, pg_sys::NodeTag::T_BoolExpr) {
                        extract_from_bool_expr(root, baserel_id, (*baserel).relids, expr as _)
                    } else if is_a(expr, pg_sys::NodeTag::T_BooleanTest) {
                        extract_from_boolean_test(baserel_id, expr as _)
                    } else {
                        if let Some(stm) = pgrx::nodes::node_to_string(expr) {
                            report_warning(&format!("unsupported qual: {stm}",));
                        }
                        None
                    };

                    if let Some(qual) = extracted {
                        quals.push(qual);
                    }
                }
            }

            quals
        })
    }
}
