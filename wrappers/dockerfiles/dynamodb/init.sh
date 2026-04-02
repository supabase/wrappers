#!/bin/sh
set -e

ENDPOINT="http://dynamodb:8000"

# Create wrappers_test_users table (partition key: id)
aws dynamodb create-table \
  --endpoint-url "$ENDPOINT" \
  --table-name wrappers_test_users \
  --attribute-definitions AttributeName=id,AttributeType=S \
  --key-schema AttributeName=id,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST

# Seed test users
aws dynamodb put-item \
  --endpoint-url "$ENDPOINT" \
  --table-name wrappers_test_users \
  --item '{
    "id":     {"S": "user1"},
    "name":   {"S": "Alice"},
    "age":    {"N": "25"},
    "active": {"BOOL": true},
    "tags":   {"M": {"role": {"S": "admin"}, "score": {"N": "99"}}}
  }'

aws dynamodb put-item \
  --endpoint-url "$ENDPOINT" \
  --table-name wrappers_test_users \
  --item '{
    "id":     {"S": "user2"},
    "name":   {"S": "Bob"},
    "age":    {"N": "30"},
    "active": {"BOOL": false}
  }'

aws dynamodb put-item \
  --endpoint-url "$ENDPOINT" \
  --table-name wrappers_test_users \
  --item '{
    "id":     {"S": "user3"},
    "name":   {"S": "Carol"},
    "age":    {"N": "22"},
    "active": {"BOOL": true}
  }'

# Create wrappers_write_test table (partition key: id)
aws dynamodb create-table \
  --endpoint-url "$ENDPOINT" \
  --table-name wrappers_write_test \
  --attribute-definitions AttributeName=id,AttributeType=S \
  --key-schema AttributeName=id,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST

# Create wrappers_types_test table — one item exercising every attribute type
aws dynamodb create-table \
  --endpoint-url "$ENDPOINT" \
  --table-name wrappers_types_test \
  --attribute-definitions AttributeName=id,AttributeType=S \
  --key-schema AttributeName=id,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST

# "aGVsbG8=" is base64("hello")
aws dynamodb put-item \
  --endpoint-url "$ENDPOINT" \
  --table-name wrappers_types_test \
  --item '{
    "id":          {"S": "item1"},
    "col_bool":    {"BOOL": true},
    "col_bool_txt":{"BOOL": false},
    "col_null":    {"NULL": true},
    "col_date":    {"S": "2024-01-15"},
    "col_ts":      {"S": "2024-01-15 10:30:00"},
    "col_tstz":    {"S": "2024-01-15 10:30:00+00"},
    "col_num_txt": {"N": "42"},
    "col_si":      {"N": "42"},
    "col_int":     {"N": "1000"},
    "col_bigint":  {"N": "9999999"},
    "col_real":    {"N": "2.5"},
    "col_double":  {"N": "2.5"},
    "col_numeric": {"N": "42"},
    "col_bytes":   {"B": "aGVsbG8="},
    "col_list":    {"L": [{"S": "a"}, {"N": "1"}, {"N": "3.5"}, {"BOOL": true}, {"NULL": true}]},
    "col_sset":    {"SS": ["alpha", "beta"]},
    "col_nset":    {"NS": ["10", "3.5"]},
    "col_map":     {"M": {"x": {"N": "1"}, "data": {"B": "aGVsbG8="}}},
    "col_json_s":  {"S": "{\"key\": \"value\"}"}
  }'

# Create wrappers_types_write_test table — empty, used for write-path type tests
aws dynamodb create-table \
  --endpoint-url "$ENDPOINT" \
  --table-name wrappers_types_write_test \
  --attribute-definitions AttributeName=id,AttributeType=S \
  --key-schema AttributeName=id,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST

echo "DynamoDB seed complete."
