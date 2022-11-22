`supabase/wrappers` is OSS. PRs and issues are welcome.

## Development

Requirements:

- rust
- cargo
- docker-compose
- [pgx](https://github.com/tcdi/pgx)

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
cargo pgx run pg14 --features clickhouse_fdw
```

Try out the commands below to spin up a database with the extension installed & query a table using GraphQL. Experiment with aliasing field/table names and filtering on different columns.

```sql
= create extension wrappers cascade;
CREATE EXTENSION
```

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
