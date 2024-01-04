`supabase/wrappers` is OSS. PRs and issues are welcome.

## Development

Requirements:

- rust
- cargo
- docker-compose
- [pgrx](https://github.com/tcdi/pgrx)

### Testing

Tests are located in `./test/sql` with expected output in `./test/expected`

To run tests locally, execute:

```bash
cd wrappers
docker-compose -f .ci/docker-compose.yaml run test-wrappers 
```

### Interactive PSQL Development

To reduce the iteration cycle, you may want to launch a psql prompt with `wrappers` installed to experiment

```bash
cd wrappers
cargo pgrx run pg14 --features clickhouse_fdw
```

Try out the commands below to spin up a database with the extension installed & query a table using GraphQL. Experiment with aliasing field/table names and filtering on different columns.

```sql
> create extension wrappers cascade;
CREATE EXTENSION
```

For debugging, you can make use of [`notice!` macros](https://docs.rs/pgrx/latest/pgrx/macro.notice.html) to print out statements while using your wrapper in `psql`.

## Documentation

All public API must be documented. Building documentation requires python 3.6+


### Install Dependencies

Install mkdocs, themes, and extensions.

```shell
pip install -r docs/requirements_docs.txt
```

### Serving

To serve the documentation locally run

```shell
mkdocs serve
```

and visit the docs at [http://127.0.0.1:8000/wrappers/](http://127.0.0.1:8000/wrappers/)

### Deploying

If you have write access to the repo, docs can be updated using

```
mkdocs gh-deploy
```

## Community Wrappers

Unfortunately, we cannot support community Wrappers inside the Supabase Dashboard until the Wrappers API is stabilized. You can [vote your favorite Wrapper](https://github.com/supabase/wrappers/discussions/136) if you'd like it to be added to Supabase in the future.

If you have developed a Wrapper that you want inside the Supabase Dashboard, please contribute it as a PR in this repo.

Once we release Wrappers 1.0, we will support community Wrappers within the Supabase Dashboard.

