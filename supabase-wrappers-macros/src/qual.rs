use proc_macro2::TokenStream as TokenStream2;
use quote::{quote, ToTokens, TokenStreamExt};

fn to_tokens() -> TokenStream2 {
    quote! {
        use ::supabase_wrappers::report_warning;

        // create array of Cell from constant datum array
        unsafe fn form_array_from_datum(
            datum: Datum,
            is_null: bool,
            typoid: Oid,
        ) -> Option<Vec<Cell>> {
            if is_null {
                return None;
            }

            let oid = PgOid::from(typoid);
            match oid {
                PgOid::BuiltIn(PgBuiltInOids::BOOLARRAYOID) => {
                    Vec::<Cell>::from_polymorphic_datum(datum, false, BOOLOID)
                }
                PgOid::BuiltIn(PgBuiltInOids::CHARARRAYOID) => {
                    Vec::<Cell>::from_polymorphic_datum(datum, false, CHAROID)
                }
                PgOid::BuiltIn(PgBuiltInOids::INT2ARRAYOID) => {
                    Vec::<Cell>::from_polymorphic_datum(datum, false, INT2OID)
                }
                PgOid::BuiltIn(PgBuiltInOids::FLOAT4ARRAYOID) => {
                    Vec::<Cell>::from_polymorphic_datum(datum, false, FLOAT4OID)
                }
                PgOid::BuiltIn(PgBuiltInOids::INT4ARRAYOID) => {
                    Vec::<Cell>::from_polymorphic_datum(datum, false, INT4OID)
                }
                PgOid::BuiltIn(PgBuiltInOids::FLOAT8ARRAYOID) => {
                    Vec::<Cell>::from_polymorphic_datum(datum, false, FLOAT8OID)
                }
                PgOid::BuiltIn(PgBuiltInOids::INT8ARRAYOID) => {
                    Vec::<Cell>::from_polymorphic_datum(datum, false, INT8OID)
                }
                PgOid::BuiltIn(PgBuiltInOids::TEXTARRAYOID) => {
                    Vec::<Cell>::from_polymorphic_datum(datum, false, TEXTOID)
                }
                PgOid::BuiltIn(PgBuiltInOids::DATEARRAYOID) => {
                    Vec::<Cell>::from_polymorphic_datum(datum, false, DATEOID)
                }
                PgOid::BuiltIn(PgBuiltInOids::TIMESTAMPARRAYOID) => {
                    Vec::<Cell>::from_polymorphic_datum(datum, false, TIMESTAMPOID)
                }
                PgOid::BuiltIn(PgBuiltInOids::JSONBARRAYOID) => {
                    Vec::<Cell>::from_polymorphic_datum(datum, false, JSONBOID)
                }
                _ => None,
            }
        }

        unsafe fn get_operator(opno: Oid) -> Form_pg_operator {
            let htup = SearchSysCache1(
                SysCacheIdentifier_OPEROID.try_into().unwrap(),
                opno.try_into().unwrap(),
            );
            if htup.is_null() {
                ReleaseSysCache(htup);
                elog(
                    PgLogLevel::ERROR,
                    &format!("cache lookup operator {} failed", opno),
                );
                return ptr::null_mut();
            }
            let op = pgx_GETSTRUCT(htup) as Form_pg_operator;
            ReleaseSysCache(htup);
            op
        }

        unsafe fn unnest_clause(node: *mut Node) -> *mut Node {
            if is_a(node, NodeTag_T_RelabelType) {
                (*(node as *mut RelabelType)).arg as _
            } else if is_a(node, NodeTag_T_ArrayCoerceExpr) {
                (*(node as *mut ArrayCoerceExpr)).arg as _
            } else {
                node
            }
        }

        unsafe fn extract_from_op_expr(
            _root: *mut PlannerInfo,
            baserel_id: Oid,
            baserel_ids: Relids,
            expr: *mut OpExpr,
        ) -> Option<Qual> {
            let args: PgList<Node> = PgList::from_pg((*expr).args);

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

            let mut left = unnest_clause(args.head().unwrap());
            let mut right = unnest_clause(args.tail().unwrap());

            // swap operands if needed
            if is_a(right, NodeTag_T_Var) && !is_a(left, NodeTag_T_Var) && (*opr).oprcom != 0 {
                let tmp = left;
                left = right;
                right = tmp;
            }

            if is_a(left, NodeTag_T_Var) && is_a(right, NodeTag_T_Const) {
                let left = left as *mut Var;
                let right = right as *mut Const;

                if bms_is_member((*left).varno.try_into().unwrap(), baserel_ids) && (*left).varattno >= 1 {
                    let field = get_attname(baserel_id, (*left).varattno, false);
                    let value = Cell::from_polymorphic_datum(
                        (*right).constvalue,
                        (*right).constisnull,
                        (*right).consttype,
                    );
                    if let Some(value) = value {
                        let qual = Qual {
                            field: CStr::from_ptr(field).to_str().unwrap().to_string(),
                            operator: name_data_to_str(&(*opr).oprname).to_string(),
                            value: Value::Cell(value),
                            use_or: false,
                        };
                        return Some(qual);
                    }
                }
            }

            None
        }

        unsafe fn extract_from_null_test(baserel_id: Oid, expr: *mut NullTest) -> Option<Qual> {
            let var = (*expr).arg as *mut Var;
            if !is_a(var as _, NodeTag_T_Var) || (*var).varattno < 1 {
                return None;
            }

            let field = get_attname(baserel_id, (*var).varattno, false);

            let opname = if (*expr).nulltesttype == NullTestType_IS_NULL {
                "is".to_string()
            } else {
                "is not".to_string()
            };

            let qual = Qual {
                field: CStr::from_ptr(field).to_str().unwrap().to_string(),
                operator: opname,
                value: Value::Cell(Cell::String("null".to_string())),
                use_or: false,
            };

            Some(qual)
        }

        unsafe fn extract_from_scalar_array_op_expr(
            _root: *mut PlannerInfo,
            baserel_id: Oid,
            baserel_ids: Relids,
            expr: *mut ScalarArrayOpExpr,
        ) -> Option<Qual> {
            let args: PgList<Node> = PgList::from_pg((*expr).args);

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

            let left = unnest_clause(args.head().unwrap());
            let right = unnest_clause(args.tail().unwrap());

            if is_a(left, NodeTag_T_Var) && is_a(right, NodeTag_T_Const) {
                let left = left as *mut Var;
                let right = right as *mut Const;

                if bms_is_member((*left).varno.try_into().unwrap(), baserel_ids) && (*left).varattno >= 1 {
                    let field = get_attname(baserel_id, (*left).varattno, false);

                    let value: Option<Vec<Cell>> = form_array_from_datum(
                        (*right).constvalue,
                        (*right).constisnull,
                        (*right).consttype,
                    );
                    if let Some(value) = value {
                        let qual = Qual {
                            field: CStr::from_ptr(field).to_str().unwrap().to_string(),
                            operator: name_data_to_str(&(*opr).oprname).to_string(),
                            value: Value::Array(value),
                            use_or: (*expr).useOr,
                        };
                        return Some(qual);
                    }
                }
            }

            None
        }

        pub(crate) unsafe fn extract_quals(
            root: *mut PlannerInfo,
            baserel: *mut RelOptInfo,
            baserel_id: Oid,
        ) -> Vec<Qual> {
            let mut quals = Vec::new();

            let conds = PgList::<RestrictInfo>::from_pg((*baserel).baserestrictinfo);
            for cond in conds.iter_ptr() {
                let expr = (*cond).clause as *mut Node;
                let extracted = if is_a(expr, NodeTag_T_OpExpr) {
                    extract_from_op_expr(root, baserel_id, (*baserel).relids, expr as _)
                } else if is_a(expr, NodeTag_T_NullTest) {
                    extract_from_null_test(baserel_id, expr as _)
                } else if is_a(expr, NodeTag_T_ScalarArrayOpExpr) {
                    extract_from_scalar_array_op_expr(root, baserel_id, (*baserel).relids, expr as _)
                } else {
                    if let Some(stm) = node_to_string(expr) {
                        report_warning(&format!("unsupported qual: {}", stm));
                    }
                    None
                };

                if let Some(qual) = extracted {
                    quals.push(qual);
                }
            }

            quals
        }
    }
}

pub(crate) struct Qual;

impl ToTokens for Qual {
    fn to_tokens(&self, tokens: &mut TokenStream2) {
        tokens.append_all(to_tokens());
    }
}
