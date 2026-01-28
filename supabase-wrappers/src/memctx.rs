//! Helper functions for Wrappers Memory Context management
//!

use pgrx::{
    memcxt::PgMemoryContexts,
    pg_sys::{AsPgCStr, MemoryContext},
    prelude::*,
};

// Wrappers root memory context name
const ROOT_MEMCTX_NAME: &str = "WrappersRootMemCtx";

// search memory context by name under specified MemoryContext
unsafe fn find_memctx_under(name: &str, under: PgMemoryContexts) -> Option<PgMemoryContexts> {
    let mut ctx = unsafe { (*under.value()).firstchild };
    while !ctx.is_null() {
        if let Ok(ctx_name) = unsafe { std::ffi::CStr::from_ptr((*ctx).name).to_str() }
            && ctx_name == name
        {
            return Some(PgMemoryContexts::For(ctx));
        }
        ctx = unsafe { (*ctx).nextchild };
    }
    None
}

// search for root memory context under CacheMemoryContext, create a new one if not exists
unsafe fn ensure_root_wrappers_memctx() -> PgMemoryContexts {
    unsafe {
        find_memctx_under(ROOT_MEMCTX_NAME, PgMemoryContexts::CacheMemoryContext).unwrap_or_else(
            || {
                let name = PgMemoryContexts::CacheMemoryContext.pstrdup(ROOT_MEMCTX_NAME);
                let ctx = pg_sys::AllocSetContextCreateExtended(
                    PgMemoryContexts::CacheMemoryContext.value(),
                    name,
                    pg_sys::ALLOCSET_DEFAULT_MINSIZE as usize,
                    pg_sys::ALLOCSET_DEFAULT_INITSIZE as usize,
                    pg_sys::ALLOCSET_DEFAULT_MAXSIZE as usize,
                );
                PgMemoryContexts::For(ctx)
            },
        )
    }
}

pub(super) unsafe fn create_wrappers_memctx(name: &str) -> MemoryContext {
    unsafe {
        let mut root = ensure_root_wrappers_memctx();
        let name = root.switch_to(|_| name.as_pg_cstr());
        pg_sys::AllocSetContextCreateExtended(
            root.value(),
            name,
            pg_sys::ALLOCSET_DEFAULT_MINSIZE as usize,
            pg_sys::ALLOCSET_DEFAULT_INITSIZE as usize,
            pg_sys::ALLOCSET_DEFAULT_MAXSIZE as usize,
        )
    }
}

pub(super) unsafe fn delete_wrappers_memctx(ctx: MemoryContext) {
    if !ctx.is_null() {
        unsafe {
            pg_sys::pfree((*ctx).name as _);
            pg_sys::MemoryContextDelete(ctx)
        }
    }
}
