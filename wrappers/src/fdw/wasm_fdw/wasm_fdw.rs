use pgrx::pg_sys;
use semver::{Version, VersionReq};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fs;
use warg_client as warg;
use wasmtime::component::*;
use wasmtime::{Config, Engine, Store};

use supabase_wrappers::prelude::*;

use super::bindings::{
    exports::supabase::wrappers::routines::Context,
    supabase::wrappers::types::{Cell as GuestCell, HostContext, HostRow},
    Wrappers,
};
use super::host::FdwHost;
use super::{WasmFdwError, WasmFdwResult};

// check minimal host version requirement, e.g, ">=1.2.3"
fn check_version_requirement(ver_req: &str) -> WasmFdwResult<()> {
    let req = VersionReq::parse(ver_req)?;
    let meta = __wasm_fdw_pgrx::wasm_fdw_get_meta();
    let host_ver = meta.get("version").expect("version should be defined");
    let version = Version::parse(host_ver)?;
    if !req.matches(&version) {
        return Err(format!(
            "host version {} not match requirement {}",
            host_ver, ver_req
        )
        .into());
    }
    Ok(())
}

// Download wasm component package from warg registry or custom url.
// The url protoal can be 'file://', 'warg(s)://' or 'http(s)://'.
fn download_component(
    rt: &Runtime,
    engine: &Engine,
    url: &str,
    name: &str,
    version: &str,
    checksum: Option<&str>,
) -> WasmFdwResult<Component> {
    if let Some(file_path) = url.strip_prefix("file://") {
        return Ok(Component::from_file(engine, file_path)?);
    }

    if url.starts_with("warg://") || url.starts_with("wargs://") {
        let url = url
            .replacen("warg://", "http://", 1)
            .replacen("wargs://", "https://", 1);

        // download from warg registry
        let config = warg::Config {
            disable_interactive: true,
            ..Default::default()
        };
        let client = rt.block_on(warg::FileSystemClient::new_with_config(
            Some(&url),
            &config,
            None,
        ))?;

        let pkg_name = warg_protocol::registry::PackageName::new(name)?;
        let ver = semver::VersionReq::parse(version)?;
        let pkg = rt
            .block_on(client.download(&pkg_name, &ver))?
            .ok_or(format!("{}@{} not found on {}", name, version, url))?;

        return Ok(Component::from_file(engine, pkg.path)?);
    }

    // otherwise, download from custom url if it is not in local cache

    // calculate file name hash and make up cache path
    let hash = Sha256::digest(format!(
        "{}:{}:{}@{}",
        unsafe { pg_sys::GetUserId().as_u32() },
        url,
        name,
        version
    ));
    let file_name = hex::encode(hash);
    let mut path = dirs::cache_dir().expect("no cache dir found");
    path.push(file_name);
    path.set_extension("wasm");

    if !path.exists() {
        // package checksum must be specified
        let option_checksum = checksum.ok_or("package checksum option not specified".to_owned())?;

        // download component wasm from remote and check its checksum
        let resp = rt.block_on(reqwest::get(url))?;
        let bytes = rt.block_on(resp.bytes())?;
        let bytes_checksum = hex::encode(Sha256::digest(&bytes));
        if bytes_checksum != option_checksum {
            return Err("package checksum not match".to_string().into());
        }

        // save the component wasm to local cache
        if let Some(parent) = path.parent() {
            // create all parent directories if they do not exist
            fs::create_dir_all(parent)?;
        }
        fs::write(&path, bytes)?;
    }

    Ok(Component::from_file(engine, &path).inspect_err(|_| {
        // remove the cache file if it cannot be loaded as component
        let _ = fs::remove_file(&path);
    })?)
}

#[wrappers_fdw(
    version = "0.1.3",
    author = "Supabase",
    website = "https://github.com/supabase/wrappers/tree/main/wrappers/src/fdw/wasm_fdw",
    error_type = "WasmFdwError"
)]
pub(crate) struct WasmFdw {
    store: Store<FdwHost>,
    bindings: Wrappers,
}

impl WasmFdw {
    fn get_context(&mut self) -> Resource<Context> {
        HostContext::new(self.store.data_mut())
    }
}

impl ForeignDataWrapper<WasmFdwError> for WasmFdw {
    fn new(server: ForeignServer) -> WasmFdwResult<Self> {
        let pkg_url = require_option("fdw_package_url", &server.options)?;
        let pkg_name = require_option("fdw_package_name", &server.options)?;
        let pkg_version = require_option("fdw_package_version", &server.options)?;
        let pkg_checksum = server
            .options
            .get("fdw_package_checksum")
            .map(|t| t.as_str());

        let rt = create_async_runtime()?;

        let mut config = Config::new();
        config.wasm_component_model(true);
        let engine = Engine::new(&config)?;

        report_info(&format!(
            "==curr dir {}",
            std::env::current_dir().unwrap().display()
        ));
        let component =
            download_component(&rt, &engine, pkg_url, pkg_name, pkg_version, pkg_checksum)?;

        let mut linker = Linker::new(&engine);
        Wrappers::add_to_linker(&mut linker, |host: &mut FdwHost| host)?;

        let mut fdw_host = FdwHost::new(rt);
        fdw_host.svr_opts.clone_from(&server.options);

        let mut store = Store::new(&engine, fdw_host);
        let (bindings, _) = Wrappers::instantiate(&mut store, &component, &linker)?;

        let mut wasm_fdw = Self { store, bindings };

        // check version requirement
        let ver_req = wasm_fdw
            .bindings
            .supabase_wrappers_routines()
            .call_host_version_requirement(&mut wasm_fdw.store)?;
        check_version_requirement(&ver_req)?;

        // call wasm fdw's init() function
        let ctx = wasm_fdw.get_context();
        wasm_fdw
            .bindings
            .supabase_wrappers_routines()
            .call_init(&mut wasm_fdw.store, ctx)??;

        Ok(wasm_fdw)
    }

    fn begin_scan(
        &mut self,
        quals: &[Qual],
        columns: &[Column],
        sorts: &[Sort],
        limit: &Option<Limit>,
        options: &HashMap<String, String>,
    ) -> WasmFdwResult<()> {
        let fdw_state = self.store.data_mut();
        fdw_state.quals = quals.to_vec();
        fdw_state.columns = columns.to_vec();
        fdw_state.sorts = sorts.to_vec();
        fdw_state.limit.clone_from(limit);
        fdw_state.tbl_opts.clone_from(options);

        let ctx = self.get_context();
        self.bindings
            .supabase_wrappers_routines()
            .call_begin_scan(&mut self.store, ctx)??;
        Ok(())
    }

    fn iter_scan(&mut self, row: &mut Row) -> WasmFdwResult<Option<()>> {
        self.store.data_mut().row.clear();

        let ctx = self.get_context();
        let host_row = HostRow::new(self.store.data_mut());
        let ret: Option<_> = self.bindings.supabase_wrappers_routines().call_iter_scan(
            &mut self.store,
            ctx,
            host_row,
        )??;
        if ret.is_some() {
            row.replace_with(self.store.data().row.clone());
            return Ok(Some(()));
        }
        Ok(None)
    }

    fn re_scan(&mut self) -> WasmFdwResult<()> {
        let ctx = self.get_context();
        self.bindings
            .supabase_wrappers_routines()
            .call_re_scan(&mut self.store, ctx)??;
        Ok(())
    }

    fn end_scan(&mut self) -> WasmFdwResult<()> {
        let ctx = self.get_context();
        self.bindings
            .supabase_wrappers_routines()
            .call_end_scan(&mut self.store, ctx)??;
        Ok(())
    }

    fn begin_modify(&mut self, options: &HashMap<String, String>) -> WasmFdwResult<()> {
        let fdw_state = self.store.data_mut();
        fdw_state.tbl_opts.clone_from(options);
        let ctx = self.get_context();
        self.bindings
            .supabase_wrappers_routines()
            .call_begin_modify(&mut self.store, ctx)??;
        Ok(())
    }

    fn insert(&mut self, src: &Row) -> WasmFdwResult<()> {
        self.store.data_mut().row = src.clone();
        let ctx = self.get_context();
        let host_row = HostRow::new(self.store.data_mut());
        self.bindings.supabase_wrappers_routines().call_insert(
            &mut self.store,
            ctx,
            host_row,
        )??;
        Ok(())
    }

    fn update(&mut self, rowid: &Cell, new_row: &Row) -> WasmFdwResult<()> {
        self.store.data_mut().row = new_row.clone();
        let ctx = self.get_context();
        let host_row = HostRow::new(self.store.data_mut());
        let cell = GuestCell::from(rowid);
        self.bindings.supabase_wrappers_routines().call_update(
            &mut self.store,
            ctx,
            &cell,
            host_row,
        )??;
        Ok(())
    }

    fn delete(&mut self, rowid: &Cell) -> WasmFdwResult<()> {
        let ctx = self.get_context();
        let cell = GuestCell::from(rowid);
        self.bindings
            .supabase_wrappers_routines()
            .call_delete(&mut self.store, ctx, &cell)??;
        Ok(())
    }

    fn end_modify(&mut self) -> WasmFdwResult<()> {
        let ctx = self.get_context();
        self.bindings
            .supabase_wrappers_routines()
            .call_end_modify(&mut self.store, ctx)??;
        Ok(())
    }

    fn validator(options: Vec<Option<String>>, catalog: Option<pg_sys::Oid>) -> WasmFdwResult<()> {
        if let Some(oid) = catalog {
            if oid == FOREIGN_SERVER_RELATION_ID {
                check_options_contain(&options, "fdw_package_url")?;
                check_options_contain(&options, "fdw_package_name")?;
                check_options_contain(&options, "fdw_package_version")?;
            }
        }

        Ok(())
    }
}
