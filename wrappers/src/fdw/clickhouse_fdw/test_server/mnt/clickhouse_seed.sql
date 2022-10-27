drop table if exists people;
create table people (
    id Int64, 
    name String
) engine=MergeTree() order by id;

insert into people values (1, 'Luke Skywalker'), (2, 'Leia Organa'), (3, 'Han Solo');

