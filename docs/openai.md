[OpenAI](https://openai.com/) is an artificial intelligence research laboratory that conducts AI research with the intention of promoting and developing a friendly AI.

OpenAI FDW supports reading embedding data based on [text-embedding-ada-002 model](https://platform.openai.com/docs/guides/embeddings/second-generation-models), the embedding is in `string` format which can be saved to a `vector` column created by [pgvector](https://github.com/pgvector/pgvector).


### Wrapper 
To get started with the OpenAI wrapper, create a foreign data wrapper specifying `handler` and `validator` as below.

```sql
create extension if not exists wrappers;

create foreign data wrapper openai_wrapper
  handler openai_fdw_handler
  validator openai_fdw_validator;
```

### Server 

Next, we need to create a server for the FDW to hold options and credentials.

#### Auth (Supabase)

If you are using the Supabase platform, this is the recommended approach for securing your OpenAI API key.

For example, to store OpenAI API key in Vault and retrieve the `key_id`,

```sql
-- save OpenAI API key in Vault and get its key id
select pgsodium.create_key(name := 'openai');
insert into vault.secrets (secret, key_id) values (
  'sk-xxx',
  (select id from pgsodium.valid_key where name = 'openai')
) returning key_id;
```

Then create the foreign server:

```sql
do $$
declare
  key_id text;
begin
  select id into key_id from pgsodium.valid_key where name = 'openai' limit 1;

  execute format(
    E'create server openai_server \n'
    '   foreign data wrapper openai_server \n'
    '   options (api_key_id ''%s'');',
    key_id
  );
end $$;
```

#### Auth (Non-Supabase)

If the platform you are using does not support `pgsodium` and `Vault`, you can create a server by storing your OpenAI credentials directly.


!!! important

    Credentials stored using this method can be viewed as plain text by anyone with access to `pg_catalog.pg_foreign_server`


```sql
create server openai_server
  foreign data wrapper openai_wrapper
  options (
    api_key 'sk-xxx'
  );
```


### Tables

OpenAI wrapper currently supports reading [embedding](https://platform.openai.com/docs/guides/embeddings/what-are-embeddings) data through OpenAI API. The foreign table must contain `id` and `embedding` column.


#### Foreign Table Options

The full list of foreign table options are below:

- `source` - Source table name in your database, required.

   The source table must contain a primary key `id` column and an `input` column for embedding retrieving.

   This can also be a subquery enclosed in parentheses, for example,

   ```
   source '(select * from my_table)'
   ```

   The subquery also needs to contain `id` and `input` columns.

   Parameters are supported in the subquery. In this case, you need to define a column for each parameter and use query conditions to pass values to them. Parameter must be `text` type. For example,

   ```
    create foreign table my_table (
      id bigint,
      embedding text,
      _param1 text,
      _param2 text
    )
      server openai_server
      options (
        -- suppose 'column1' type is text and 'column2' type is bigint
        source '(select id, input from my_source where column1='${_param1}' and column2=${_param2})'
      );

    select * from my_table where _param1='aaa' and _param2='32';
   ```


#### Examples

A simple example,

```sql
-- source table with column to store embedding
-- Note: source table must have an 'id' and an 'input' column.
create table my_table (
  id bigint primary key,
  input text,  -- input text to get embedding for
  embedding vector(1536)  -- embedding for 'input' column based on text-embedding-ada-002 model
);

-- insert example input
insert into my_table(id, input) values (1, 'hello world');

-- foreign table to fetch embedding
-- Note: foreign table must have an 'id' and an 'embedding' column,
--       corresponding 'id' and 'input' columns in source table.
create foreign table my_embedding (
  id bigint,
  embedding text  -- embedding in string format, e.g. '[0.0051006232, -0.03568436, ...]'
)
  server openai_server
  options (
    source 'my_table'  -- source table, required
  );

-- get embedding for each row of input text
-- Note: this will call OpenAI API for each row, so the source table shouldn't have too many records.
select * from my_embedding;

-- save the embedding
update my_table t
set embedding = s.embedding::vector
from my_embedding s
where s.id = t.id;

-- check if embedding has been saved
select * from my_table;
```

This is another more complex example with query parameters, it incrementally updates embedding for specific input.

```sql
-- source table with an embedding column and an update timestamp
create table my_table2 (
  id bigint primary key,
  author text,
  content text,
  embedding vector(1536),
  ts timestamp
);

-- add some example data
insert into my_table2(id, author, content, ts) values (1, 'foo', 'hello world', now());
insert into my_table2(id, author, content, ts) values (2, 'bar', 'hello ai', now());

-- foreign table to fetch embedding
-- Note: foreign table must have an 'id' and an 'embedding' column.
create foreign table my_embedding2 (
  id bigint,
  embedding text,
  _param_author text, -- column for parameter ${author}
  _param_ts text  -- column for parameter ${ts}
)
  server openai_server
  options (
    -- Note: the source sub-query must have an 'id' and an 'input' column, and the
    -- corresponding parameters.
    source '(
      select id, content as input
      from my_table2
      where author = ''${_param_author}'' and ts >= ''${_param_ts}''
    )'
  );

-- get embedding for each specified row of input text
select id, embedding
from my_embedding2
where _param_author = 'foo'
  and _param_ts = '2023-05-21';

-- save the embedding
update my_table2 t
set
  embedding = s.embedding::vector,
  ts = now()
from my_embedding2 s
where s.id = t.id
  and s._param_author = 'foo'
  and s._param_ts = '2023-05-21';

-- check if embedding has been saved
select * from my_table2;
```
