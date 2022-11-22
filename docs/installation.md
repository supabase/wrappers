First, install [pgx](https://github.com/tcdi/pgx)

Then clone the repo and install using

```bash
git clone https://github.com/supabase/wrappers.git
cd wrappers/wrappers 
cargo pgx install --no-default-features --features pg14,clickhouse_fdw --release
```

To enable the extension in PostgreSQL we must execute a `create extension` statement.

```psql
create extension wrappers;
```
