use pgrx::prelude::PgSqlErrorCode;

use supabase_wrappers::prelude::*;

use super::FdwHost;

const _: () = {
    use super::super::bindings::v1::supabase::wrappers::{types::Cell as GuestCell, utils};

    impl utils::Host for FdwHost {
        fn report_info(&mut self, msg: String) {
            report_info(&msg);
        }

        fn report_notice(&mut self, msg: String) {
            report_notice(&msg);
        }

        fn report_warning(&mut self, msg: String) {
            report_warning(&msg);
        }

        fn report_error(&mut self, msg: String) {
            report_error(PgSqlErrorCode::ERRCODE_FDW_ERROR, &msg);
        }

        fn cell_to_string(&mut self, cell: Option<GuestCell>) -> String {
            match cell {
                Some(c) => Cell::try_from(c)
                    .map(|a| a.to_string())
                    .expect("convert cell failed"),
                None => "null".to_string(),
            }
        }

        fn get_vault_secret(&mut self, secret_id: String) -> Option<String> {
            get_vault_secret(&secret_id)
        }
    }
};

const _: () = {
    use super::super::bindings::v2::supabase::wrappers::{types::Cell as GuestCell, utils};

    impl utils::Host for FdwHost {
        fn report_info(&mut self, msg: String) {
            report_info(&msg);
        }

        fn report_notice(&mut self, msg: String) {
            report_notice(&msg);
        }

        fn report_warning(&mut self, msg: String) {
            report_warning(&msg);
        }

        fn report_error(&mut self, msg: String) {
            report_error(PgSqlErrorCode::ERRCODE_FDW_ERROR, &msg);
        }

        fn cell_to_string(&mut self, cell: Option<GuestCell>) -> String {
            match cell {
                Some(c) => Cell::try_from(c)
                    .map(|a| a.to_string())
                    .expect("convert cell failed"),
                None => "null".to_string(),
            }
        }

        fn get_vault_secret(&mut self, secret_id: String) -> Option<String> {
            get_vault_secret(&secret_id)
        }
    }
};
