#!/bin/bash

# for more details about this LocalStack initial script, see https://docs.localstack.cloud/references/init-hooks/

# create test bucket
awslocal s3 mb s3://test

# upload test data files
awslocal s3 cp /data/test_data.csv s3://test/test_data.csv
awslocal s3 cp /data/test_data.csv.gz s3://test/test_data.csv.gz
awslocal s3 cp /data/test_data.jsonl s3://test/test_data.jsonl
awslocal s3 cp /data/test_data.jsonl.bz2 s3://test/test_data.jsonl.bz2
awslocal s3 cp /data/test_data.parquet s3://test/test_data.parquet
awslocal s3 cp /data/test_data.parquet.gz s3://test/test_data.parquet.gz
