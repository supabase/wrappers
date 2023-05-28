//! Helper functions for Wrappers Memory Context management
//!

use pgrx::{memcxt::PgMemoryContexts, pg_sys::AsPgCStr, prelude::*};

// Wrappers root memory context name
const ROOT_MEMCTX_NAME: &str = "WrappersRootMemCtx";

// search memory context by name under specified MemoryContext
unsafe fn find_memctx_under(name: &str, under: PgMemoryContexts) -> Option<PgMemoryContexts> {
    let mut ctx = (*under.value()).firstchild;
    while !ctx.is_null() {
        if let Ok(ctx_name) = std::ffi::CStr::from_ptr((*ctx).name).to_str() {
            if ctx_name == name {
                return Some(PgMemoryContexts::For(ctx));
            }
        }
        ctx = (*ctx).nextchild;
    }
    None
}

// search for root memory context under CacheMemoryContext, create a new one if not exists
unsafe fn ensure_root_wrappers_memctx() -> PgMemoryContexts {
    find_memctx_under(ROOT_MEMCTX_NAME, PgMemoryContexts::CacheMemoryContext).unwrap_or_else(|| {
        let name = PgMemoryContexts::CacheMemoryContext.pstrdup(ROOT_MEMCTX_NAME);
        let ctx = pg_sys::AllocSetContextCreateExtended(
            PgMemoryContexts::CacheMemoryContext.value(),
            name,
            pg_sys::ALLOCSET_DEFAULT_MINSIZE as usize,
            pg_sys::ALLOCSET_DEFAULT_INITSIZE as usize,
            pg_sys::ALLOCSET_DEFAULT_MAXSIZE as usize,
        );
        PgMemoryContexts::For(ctx)
    })
}

// search Wrappers memory context by name, reset it if exists otherwise create a new one
pub(super) unsafe fn refresh_wrappers_memctx(name: &str) -> PgMemoryContexts {
    let mut root = ensure_root_wrappers_memctx();
    find_memctx_under(name, PgMemoryContexts::For(root.value()))
        .map(|mut ctx| {
            ctx.reset();
            ctx
        })
        .unwrap_or_else(|| {
            let name = root.switch_to(|_| name.as_pg_cstr());
            let ctx = pg_sys::AllocSetContextCreateExtended(
                root.value(),
                name,
                pg_sys::ALLOCSET_DEFAULT_MINSIZE as usize,
                pg_sys::ALLOCSET_DEFAULT_INITSIZE as usize,
                pg_sys::ALLOCSET_DEFAULT_MAXSIZE as usize,
            );
            PgMemoryContexts::For(ctx)
        })
}
