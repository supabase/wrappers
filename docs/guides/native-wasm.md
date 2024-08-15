# Native vs Wasm Wrappers

## Wasm Wrappers

Since v0.4.0, Wrappers supports WebAssembly (Wasm) FDWs. Anyone can develop a Wasm Wrapper. Wasm foreign data wrappers are dynamically loaded on the first query and then cached locally, they can be installed directly from remotes like GitHub and S3.

Check out [Developing a Wrapper](create-wasm-wrapper.md) to develop your own Wasm Wrapper.

## Native Wrappers

Native Wrappers are developed and supported by Supabase. These have better performance than Wasm wrappers, but they must be pre-installed on the Postgres database before a developer can use it.
