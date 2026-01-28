mod http;
mod jwt;
mod stats;
mod time;
mod utils;

use pgrx::pg_sys;
use std::collections::HashMap;
use wasmtime::Result as WasmResult;
use wasmtime::component::*;

use supabase_wrappers::prelude::*;

#[derive(Debug)]
pub(super) struct FdwHost {
    pub rt: Runtime,
    pub svr_opts: HashMap<String, String>,
    pub tbl_opts: HashMap<String, String>,
    pub import_schema_opts: HashMap<String, String>,
    pub row: Row,
    pub columns: Vec<Column>,
    pub quals: Vec<Qual>,
    pub sorts: Vec<Sort>,
    pub limit: Option<Limit>,
}

impl FdwHost {
    const CTX_REP: u32 = 1000;
    const SVR_OPTS_REP: u32 = 2000;
    const TBL_OPTS_REP: u32 = 3000;
    const IMPORT_SCHEMA_OPTS_REP: u32 = 3100;
    const ROW_REP: u32 = 4000;
    const COLUMN_REP: u32 = 5000;
    const QUAL_REP: u32 = 6000;
    const SORT_REP: u32 = 7000;
    const LIMIT_REP: u32 = 8000;

    pub(super) fn new(rt: Runtime) -> Self {
        Self {
            rt,
            svr_opts: HashMap::new(),
            tbl_opts: HashMap::new(),
            import_schema_opts: HashMap::new(),
            row: Row::default(),
            columns: Vec::new(),
            quals: Vec::new(),
            sorts: Vec::new(),
            limit: None,
        }
    }
}

const _: () = {
    use super::bindings::v1::{
        exports::supabase::wrappers::routines::Context,
        supabase::wrappers::types::{
            Cell as GuestCell, Column as GuestColumn, FdwError as GuestFdwError, Host, HostColumn,
            HostContext, HostLimit, HostOptions, HostQual, HostRow, HostSort, Limit as GuestLimit,
            Options, OptionsType, Param as GuestParam, Qual as GuestQual, Row as GuestRow,
            Sort as GuestSort, TypeOid, Value as GuestValue,
        },
    };

    impl HostRow for FdwHost {
        fn new(&mut self) -> Resource<GuestRow> {
            Resource::new_own(Self::ROW_REP)
        }

        fn cols(&mut self, _rep: Resource<GuestRow>) -> Vec<String> {
            self.row.cols.clone()
        }

        fn cells(&mut self, _rep: Resource<GuestRow>) -> Vec<Option<GuestCell>> {
            self.row
                .cells
                .iter()
                .map(|c| c.as_ref().map(GuestCell::from))
                .collect()
        }

        fn push(&mut self, _rep: Resource<GuestRow>, cell: Option<GuestCell>) {
            let cell = cell.map(|c| Cell::try_from(c).expect("convert cell failed"));
            let idx = self.row.cols.len();
            let col = &self.columns[idx];
            self.row.push(&col.name, cell);
        }

        fn drop(&mut self, _rep: Resource<GuestRow>) -> WasmResult<()> {
            Ok(())
        }
    }

    impl HostColumn for FdwHost {
        fn new(&mut self, index: u32) -> Resource<GuestColumn> {
            Resource::new_own(Self::COLUMN_REP + index)
        }

        fn name(&mut self, rep: Resource<GuestColumn>) -> String {
            let index = (rep.rep() - Self::COLUMN_REP) as usize;
            self.columns[index].name.clone()
        }

        fn num(&mut self, rep: Resource<GuestColumn>) -> u32 {
            let index = (rep.rep() - Self::COLUMN_REP) as usize;
            self.columns[index].num as u32
        }

        fn type_oid(&mut self, rep: Resource<GuestColumn>) -> TypeOid {
            let index = (rep.rep() - Self::COLUMN_REP) as usize;
            match self.columns[index].type_oid {
                pg_sys::BOOLOID => TypeOid::Bool,
                pg_sys::CHAROID => TypeOid::I8,
                pg_sys::INT2OID => TypeOid::I16,
                pg_sys::FLOAT4OID => TypeOid::F32,
                pg_sys::INT4OID => TypeOid::I32,
                pg_sys::FLOAT8OID => TypeOid::F64,
                pg_sys::INT8OID => TypeOid::I64,
                pg_sys::NUMERICOID => TypeOid::Numeric,
                pg_sys::TEXTOID => TypeOid::String,
                pg_sys::DATEOID => TypeOid::Date,
                pg_sys::TIMESTAMPOID => TypeOid::Timestamp,
                pg_sys::TIMESTAMPTZOID => TypeOid::Timestamptz,
                pg_sys::JSONBOID => TypeOid::Json,
                _ => unimplemented!("column type oid not supported"),
            }
        }

        fn drop(&mut self, _rep: Resource<GuestColumn>) -> WasmResult<()> {
            Ok(())
        }
    }

    impl HostQual for FdwHost {
        fn new(&mut self, index: u32) -> Resource<GuestQual> {
            Resource::new_own(Self::QUAL_REP + index)
        }

        fn field(&mut self, rep: Resource<GuestQual>) -> String {
            let index = (rep.rep() - Self::QUAL_REP) as usize;
            self.quals[index].field.clone()
        }

        fn operator(&mut self, rep: Resource<GuestQual>) -> String {
            let index = (rep.rep() - Self::QUAL_REP) as usize;
            self.quals[index].operator.clone()
        }

        fn value(&mut self, rep: Resource<GuestQual>) -> GuestValue {
            let index = (rep.rep() - Self::QUAL_REP) as usize;
            GuestValue::from(self.quals[index].value.clone())
        }

        fn use_or(&mut self, rep: Resource<GuestQual>) -> bool {
            let index = (rep.rep() - Self::QUAL_REP) as usize;
            self.quals[index].use_or
        }

        fn param(&mut self, rep: Resource<GuestQual>) -> Option<GuestParam> {
            let index = (rep.rep() - Self::QUAL_REP) as usize;
            self.quals[index].param.clone().map(GuestParam::from)
        }

        fn deparse(&mut self, rep: Resource<GuestQual>) -> String {
            let index = (rep.rep() - Self::QUAL_REP) as usize;
            self.quals[index].deparse()
        }

        fn drop(&mut self, _rep: Resource<GuestQual>) -> WasmResult<()> {
            Ok(())
        }
    }

    impl HostSort for FdwHost {
        fn new(&mut self, index: u32) -> Resource<GuestSort> {
            Resource::new_own(Self::SORT_REP + index)
        }

        fn field(&mut self, rep: Resource<GuestSort>) -> String {
            let index = (rep.rep() - Self::SORT_REP) as usize;
            self.sorts[index].field.clone()
        }

        fn field_no(&mut self, rep: Resource<GuestSort>) -> u32 {
            let index = (rep.rep() - Self::SORT_REP) as usize;
            self.sorts[index].field_no as u32
        }

        fn reversed(&mut self, rep: Resource<GuestSort>) -> bool {
            let index = (rep.rep() - Self::SORT_REP) as usize;
            self.sorts[index].reversed
        }

        fn nulls_first(&mut self, rep: Resource<GuestSort>) -> bool {
            let index = (rep.rep() - Self::SORT_REP) as usize;
            self.sorts[index].nulls_first
        }

        fn collate(&mut self, rep: Resource<GuestSort>) -> Option<String> {
            let index = (rep.rep() - Self::SORT_REP) as usize;
            self.sorts[index].collate.clone()
        }

        fn deparse(&mut self, rep: Resource<GuestSort>) -> String {
            let index = (rep.rep() - Self::SORT_REP) as usize;
            self.sorts[index].deparse()
        }

        fn deparse_with_collate(&mut self, rep: Resource<GuestSort>) -> String {
            let index = (rep.rep() - Self::SORT_REP) as usize;
            self.sorts[index].deparse_with_collate()
        }

        fn drop(&mut self, _rep: Resource<GuestSort>) -> WasmResult<()> {
            Ok(())
        }
    }

    impl HostLimit for FdwHost {
        fn new(&mut self) -> Resource<GuestLimit> {
            Resource::new_own(Self::LIMIT_REP)
        }

        fn count(&mut self, _rep: Resource<GuestLimit>) -> i64 {
            self.limit.as_ref().map(|a| a.count).unwrap()
        }

        fn offset(&mut self, _rep: Resource<GuestLimit>) -> i64 {
            self.limit.as_ref().map(|a| a.offset).unwrap()
        }

        fn deparse(&mut self, _rep: Resource<GuestLimit>) -> String {
            self.limit.as_ref().map(|a| a.deparse()).unwrap()
        }

        fn drop(&mut self, _rep: Resource<GuestLimit>) -> WasmResult<()> {
            Ok(())
        }
    }

    impl HostOptions for FdwHost {
        fn new(&mut self, options_type: OptionsType) -> Resource<Options> {
            let opt_type = match options_type {
                OptionsType::Server => Self::SVR_OPTS_REP,
                OptionsType::Table => Self::TBL_OPTS_REP,
            };
            Resource::new_own(opt_type)
        }

        fn get(&mut self, rep: Resource<Options>, key: String) -> Option<String> {
            let opts = match rep.rep() {
                Self::SVR_OPTS_REP => &self.svr_opts,
                Self::TBL_OPTS_REP => &self.tbl_opts,
                _ => unreachable!(),
            };
            opts.get(&key).map(|s| s.to_owned())
        }

        fn require(
            &mut self,
            rep: Resource<Options>,
            key: String,
        ) -> Result<String, GuestFdwError> {
            let opts = match rep.rep() {
                Self::SVR_OPTS_REP => &self.svr_opts,
                Self::TBL_OPTS_REP => &self.tbl_opts,
                _ => unreachable!(),
            };
            require_option(&key, opts)
                .map(|s| s.to_owned())
                .map_err(|e| e.to_string())
        }

        fn require_or(&mut self, rep: Resource<Options>, key: String, default: String) -> String {
            let opts = match rep.rep() {
                Self::SVR_OPTS_REP => &self.svr_opts,
                Self::TBL_OPTS_REP => &self.tbl_opts,
                _ => unreachable!(),
            };
            require_option_or(&key, opts, &default).to_owned()
        }

        fn drop(&mut self, _rep: Resource<Options>) -> WasmResult<()> {
            Ok(())
        }
    }

    impl HostContext for FdwHost {
        fn new(&mut self) -> Resource<Context> {
            Resource::new_borrow(Self::CTX_REP)
        }

        fn get_options(
            &mut self,
            _rep: Resource<Context>,
            options_type: OptionsType,
        ) -> Resource<Options> {
            HostOptions::new(self, options_type)
        }

        fn get_quals(&mut self, _rep: Resource<Context>) -> Vec<Resource<GuestQual>> {
            let mut ret = Vec::new();
            for idx in 0..self.quals.len() {
                ret.push(HostQual::new(self, idx as u32));
            }
            ret
        }

        fn get_columns(&mut self, _rep: Resource<Context>) -> Vec<Resource<GuestColumn>> {
            let mut ret = Vec::new();
            for idx in 0..self.columns.len() {
                ret.push(HostColumn::new(self, idx as u32));
            }
            ret
        }

        fn get_sorts(&mut self, _rep: Resource<Context>) -> Vec<Resource<GuestSort>> {
            let mut ret = Vec::new();
            for idx in 0..self.sorts.len() {
                ret.push(HostSort::new(self, idx as u32));
            }
            ret
        }

        fn get_limit(&mut self, _rep: Resource<Context>) -> Option<Resource<GuestLimit>> {
            if self.limit.is_some() {
                Some(HostLimit::new(self))
            } else {
                None
            }
        }

        fn drop(&mut self, _rep: Resource<Context>) -> WasmResult<()> {
            Ok(())
        }
    }

    impl Host for FdwHost {}
};

const _: () = {
    use super::bindings::v2::{
        exports::supabase::wrappers::routines::Context,
        supabase::wrappers::types::{
            Cell as GuestCell, Column as GuestColumn, FdwError as GuestFdwError, Host, HostColumn,
            HostContext, HostLimit, HostOptions, HostQual, HostRow, HostSort, Limit as GuestLimit,
            Options, OptionsType, Param as GuestParam, Qual as GuestQual, Row as GuestRow,
            Sort as GuestSort, TypeOid, Value as GuestValue,
        },
    };

    impl HostRow for FdwHost {
        fn new(&mut self) -> Resource<GuestRow> {
            Resource::new_own(Self::ROW_REP)
        }

        fn cols(&mut self, _rep: Resource<GuestRow>) -> Vec<String> {
            self.row.cols.clone()
        }

        fn cells(&mut self, _rep: Resource<GuestRow>) -> Vec<Option<GuestCell>> {
            self.row
                .cells
                .iter()
                .map(|c| c.as_ref().map(GuestCell::from))
                .collect()
        }

        fn push(&mut self, _rep: Resource<GuestRow>, cell: Option<GuestCell>) {
            let cell = cell.map(|c| Cell::try_from(c).expect("convert cell failed"));
            let idx = self.row.cols.len();
            let col = &self.columns[idx];
            self.row.push(&col.name, cell);
        }

        fn drop(&mut self, _rep: Resource<GuestRow>) -> WasmResult<()> {
            Ok(())
        }
    }

    impl HostColumn for FdwHost {
        fn new(&mut self, index: u32) -> Resource<GuestColumn> {
            Resource::new_own(Self::COLUMN_REP + index)
        }

        fn name(&mut self, rep: Resource<GuestColumn>) -> String {
            let index = (rep.rep() - Self::COLUMN_REP) as usize;
            self.columns[index].name.clone()
        }

        fn num(&mut self, rep: Resource<GuestColumn>) -> u32 {
            let index = (rep.rep() - Self::COLUMN_REP) as usize;
            self.columns[index].num as u32
        }

        fn type_oid(&mut self, rep: Resource<GuestColumn>) -> TypeOid {
            let index = (rep.rep() - Self::COLUMN_REP) as usize;
            match self.columns[index].type_oid {
                pg_sys::BOOLOID => TypeOid::Bool,
                pg_sys::CHAROID => TypeOid::I8,
                pg_sys::INT2OID => TypeOid::I16,
                pg_sys::FLOAT4OID => TypeOid::F32,
                pg_sys::INT4OID => TypeOid::I32,
                pg_sys::FLOAT8OID => TypeOid::F64,
                pg_sys::INT8OID => TypeOid::I64,
                pg_sys::NUMERICOID => TypeOid::Numeric,
                pg_sys::TEXTOID => TypeOid::String,
                pg_sys::DATEOID => TypeOid::Date,
                pg_sys::TIMESTAMPOID => TypeOid::Timestamp,
                pg_sys::TIMESTAMPTZOID => TypeOid::Timestamptz,
                pg_sys::JSONBOID => TypeOid::Json,
                pg_sys::UUIDOID => TypeOid::Uuid,
                _ => unimplemented!("column type oid not supported"),
            }
        }

        fn drop(&mut self, _rep: Resource<GuestColumn>) -> WasmResult<()> {
            Ok(())
        }
    }

    impl HostQual for FdwHost {
        fn new(&mut self, index: u32) -> Resource<GuestQual> {
            Resource::new_own(Self::QUAL_REP + index)
        }

        fn field(&mut self, rep: Resource<GuestQual>) -> String {
            let index = (rep.rep() - Self::QUAL_REP) as usize;
            self.quals[index].field.clone()
        }

        fn operator(&mut self, rep: Resource<GuestQual>) -> String {
            let index = (rep.rep() - Self::QUAL_REP) as usize;
            self.quals[index].operator.clone()
        }

        fn value(&mut self, rep: Resource<GuestQual>) -> GuestValue {
            let index = (rep.rep() - Self::QUAL_REP) as usize;
            GuestValue::from(self.quals[index].value.clone())
        }

        fn use_or(&mut self, rep: Resource<GuestQual>) -> bool {
            let index = (rep.rep() - Self::QUAL_REP) as usize;
            self.quals[index].use_or
        }

        fn param(&mut self, rep: Resource<GuestQual>) -> Option<GuestParam> {
            let index = (rep.rep() - Self::QUAL_REP) as usize;
            self.quals[index].param.clone().map(GuestParam::from)
        }

        fn deparse(&mut self, rep: Resource<GuestQual>) -> String {
            let index = (rep.rep() - Self::QUAL_REP) as usize;
            self.quals[index].deparse()
        }

        fn drop(&mut self, _rep: Resource<GuestQual>) -> WasmResult<()> {
            Ok(())
        }
    }

    impl HostSort for FdwHost {
        fn new(&mut self, index: u32) -> Resource<GuestSort> {
            Resource::new_own(Self::SORT_REP + index)
        }

        fn field(&mut self, rep: Resource<GuestSort>) -> String {
            let index = (rep.rep() - Self::SORT_REP) as usize;
            self.sorts[index].field.clone()
        }

        fn field_no(&mut self, rep: Resource<GuestSort>) -> u32 {
            let index = (rep.rep() - Self::SORT_REP) as usize;
            self.sorts[index].field_no as u32
        }

        fn reversed(&mut self, rep: Resource<GuestSort>) -> bool {
            let index = (rep.rep() - Self::SORT_REP) as usize;
            self.sorts[index].reversed
        }

        fn nulls_first(&mut self, rep: Resource<GuestSort>) -> bool {
            let index = (rep.rep() - Self::SORT_REP) as usize;
            self.sorts[index].nulls_first
        }

        fn collate(&mut self, rep: Resource<GuestSort>) -> Option<String> {
            let index = (rep.rep() - Self::SORT_REP) as usize;
            self.sorts[index].collate.clone()
        }

        fn deparse(&mut self, rep: Resource<GuestSort>) -> String {
            let index = (rep.rep() - Self::SORT_REP) as usize;
            self.sorts[index].deparse()
        }

        fn deparse_with_collate(&mut self, rep: Resource<GuestSort>) -> String {
            let index = (rep.rep() - Self::SORT_REP) as usize;
            self.sorts[index].deparse_with_collate()
        }

        fn drop(&mut self, _rep: Resource<GuestSort>) -> WasmResult<()> {
            Ok(())
        }
    }

    impl HostLimit for FdwHost {
        fn new(&mut self) -> Resource<GuestLimit> {
            Resource::new_own(Self::LIMIT_REP)
        }

        fn count(&mut self, _rep: Resource<GuestLimit>) -> i64 {
            self.limit.as_ref().map(|a| a.count).unwrap()
        }

        fn offset(&mut self, _rep: Resource<GuestLimit>) -> i64 {
            self.limit.as_ref().map(|a| a.offset).unwrap()
        }

        fn deparse(&mut self, _rep: Resource<GuestLimit>) -> String {
            self.limit.as_ref().map(|a| a.deparse()).unwrap()
        }

        fn drop(&mut self, _rep: Resource<GuestLimit>) -> WasmResult<()> {
            Ok(())
        }
    }

    impl HostOptions for FdwHost {
        fn new(&mut self, options_type: OptionsType) -> Resource<Options> {
            let opt_type = match options_type {
                OptionsType::Server => Self::SVR_OPTS_REP,
                OptionsType::Table => Self::TBL_OPTS_REP,
                OptionsType::ImportSchema => Self::IMPORT_SCHEMA_OPTS_REP,
                OptionsType::Other(_) => todo!("Add support for more options type"),
            };
            Resource::new_own(opt_type)
        }

        fn get(&mut self, rep: Resource<Options>, key: String) -> Option<String> {
            let opts = match rep.rep() {
                Self::SVR_OPTS_REP => &self.svr_opts,
                Self::TBL_OPTS_REP => &self.tbl_opts,
                Self::IMPORT_SCHEMA_OPTS_REP => &self.import_schema_opts,
                _ => unreachable!(),
            };
            opts.get(&key).map(|s| s.to_owned())
        }

        fn require(
            &mut self,
            rep: Resource<Options>,
            key: String,
        ) -> Result<String, GuestFdwError> {
            let opts = match rep.rep() {
                Self::SVR_OPTS_REP => &self.svr_opts,
                Self::TBL_OPTS_REP => &self.tbl_opts,
                Self::IMPORT_SCHEMA_OPTS_REP => &self.import_schema_opts,
                _ => unreachable!(),
            };
            require_option(&key, opts)
                .map(|s| s.to_owned())
                .map_err(|e| e.to_string())
        }

        fn require_or(&mut self, rep: Resource<Options>, key: String, default: String) -> String {
            let opts = match rep.rep() {
                Self::SVR_OPTS_REP => &self.svr_opts,
                Self::TBL_OPTS_REP => &self.tbl_opts,
                Self::IMPORT_SCHEMA_OPTS_REP => &self.import_schema_opts,
                _ => unreachable!(),
            };
            require_option_or(&key, opts, &default).to_owned()
        }

        fn drop(&mut self, _rep: Resource<Options>) -> WasmResult<()> {
            Ok(())
        }
    }

    impl HostContext for FdwHost {
        fn new(&mut self) -> Resource<Context> {
            Resource::new_borrow(Self::CTX_REP)
        }

        fn get_options(
            &mut self,
            _rep: Resource<Context>,
            options_type: OptionsType,
        ) -> Resource<Options> {
            HostOptions::new(self, options_type)
        }

        fn get_quals(&mut self, _rep: Resource<Context>) -> Vec<Resource<GuestQual>> {
            let mut ret = Vec::new();
            for idx in 0..self.quals.len() {
                ret.push(HostQual::new(self, idx as u32));
            }
            ret
        }

        fn get_columns(&mut self, _rep: Resource<Context>) -> Vec<Resource<GuestColumn>> {
            let mut ret = Vec::new();
            for idx in 0..self.columns.len() {
                ret.push(HostColumn::new(self, idx as u32));
            }
            ret
        }

        fn get_sorts(&mut self, _rep: Resource<Context>) -> Vec<Resource<GuestSort>> {
            let mut ret = Vec::new();
            for idx in 0..self.sorts.len() {
                ret.push(HostSort::new(self, idx as u32));
            }
            ret
        }

        fn get_limit(&mut self, _rep: Resource<Context>) -> Option<Resource<GuestLimit>> {
            if self.limit.is_some() {
                Some(HostLimit::new(self))
            } else {
                None
            }
        }

        fn drop(&mut self, _rep: Resource<Context>) -> WasmResult<()> {
            Ok(())
        }
    }

    impl Host for FdwHost {}
};
