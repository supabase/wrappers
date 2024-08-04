First, install [pgrx](https://github.com/tcdi/pgrx)

Then clone the repo and install using

```bash
git clone https://github.com/supabase/wrappers.git
cd wrappers/wrappers 
cargo pgrx install --no-default-features --features pg14,<some_integration>_fdw --release
```

To enable the extension in PostgreSQL we must execute a `create extension` statement.

```psql
create extension wrappers;
```
