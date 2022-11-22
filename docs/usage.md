
`supabase/wrappers` leverages PostgreSQL's builtin foreign data wrapper (FDW) functionality to integrate with third-party services. 

## Wrappers 

### BigQuery

What is BigQuery

#### Options

##### Server

project_id
dataset_id

~ optional
location


##### Table
sa_key



#### Example

```sql
create foreign data wrapper bigquery_wrapper
  handler wrappers_handler
  validator wrappers_validator
  options (
    wrapper 'BigQueryFdw'
  );

create server bigquery_server
  foreign data wrapper bigquery_wrapper
  options (
    project_id 'test_project',
    dataset_id 'test_dataset',
    
  );
  
create foreign table test_table (
  id bigint,
  name text
)
  server bigquery_server
  options (
    table 'test_table',
    rowid_column 'id'
  );
  
select * from test_table;
```



### ClickHouse
### Firebase
### Stripe
