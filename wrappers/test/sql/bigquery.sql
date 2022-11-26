-- create foreign data wrapper and enable 'BigQueryFdw'
drop foreign data wrapper if exists bigquery_wrapper cascade;
create foreign data wrapper bigquery_wrapper
  handler wrappers_handler
  validator wrappers_validator
  options (
    wrapper 'BigQueryFdw'
  );

-- create a wrappers BigQuery server and specify connection info
drop server if exists my_bigquery_server cascade;
create server my_bigquery_server
  foreign data wrapper bigquery_wrapper
  options (
    project_id 'test_project',
    dataset_id 'test_dataset',
	api_endpoint 'http://bigquery:9111',
	mock_auth 'true'
  );
  
-- create an example foreign table
drop foreign table if exists test_table;
create foreign table test_table (
  id bigint,
  name text
)
  server my_bigquery_server
  options (
    table 'test_table',
    rowid_column 'id'
  );
  
  
select * from test_table;

insert into test_table (id, name)
values (3, 'baz');
select * from test_table;
