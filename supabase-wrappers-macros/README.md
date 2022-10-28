# supabase-wrappers-macros

A macro `wrappers_magic` to set up facility functions for Postgres FDW
development framework `Wrappers`.

This crate is NOT supposed to be used directly, please use [supabase-wrappers](https://github.com/supabase/wrappers/tree/main/supabase-wrappers) instead.

See more details about [Wrappers](https://github.com/supabase/wrappers).

# Example

Suppose your developed two FDWs `FooFdw` and `BarFdw`, then you can use them
like below,

```rust
use supabase_wrappers::wrappers_magic;
use crate::{FooFdw, BarFdw};

// use single FDW
wrappers_magic!(FooFdw);

// or use multiple FDWs
wrappers_magic!(FooFdw, BarFdw);
```

Feature based conditional compilation is also supported.

```rust
#[cfg(feature = "foo_fdw")]
use crate::FooFdw;

#[cfg(feature = "bar_fdw")]
use crate::BarFdw;

wrappers_magic!(
    #[cfg(feature = "foo_fdw")]
    FooFdw,

    #[cfg(feature = "bar_fdw")]
    BarFdw,
);
```

