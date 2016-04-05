#!/bin/sh

aws dynamodb create-table \
	--attribute-definitions='[{"AttributeName":"pri-key","AttributeType":"S"},{"AttributeName":"sort-key","AttributeType":"N"}]' \
	--table-name='EMBULK_DYNAMODB_TEST_TABLE' \
	--key-schema='[{"AttributeName":"pri-key","KeyType":"HASH"},{"AttributeName":"sort-key","KeyType":"RANGE"}]' \
	--provisioned-throughput='{"ReadCapacityUnits":5, "WriteCapacityUnits":5}' \
	--endpoint-url http://localhost:8000
