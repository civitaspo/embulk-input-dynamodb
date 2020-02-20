#!/usr/bin/env bash

if aws dynamodb describe-table --table-name='embulk-input-dynamodb_example'  --endpoint-url http://localhost:8000 --region us-east-1 2>/dev/null; then
    aws dynamodb delete-table --table-name='embulk-input-dynamodb_example' \
        --endpoint-url http://localhost:8000 \
        --region us-east-1
fi

aws dynamodb create-table \
    --table-name='embulk-input-dynamodb_example' \
    --attribute-definitions='[
        {"AttributeName":"primary-key","AttributeType":"S"},
        {"AttributeName":"sort-key","AttributeType":"N"}
    ]' \
    --key-schema='[
        {"AttributeName":"primary-key","KeyType":"HASH"},
        {"AttributeName":"sort-key","KeyType":"RANGE"}
    ]' \
    --provisioned-throughput='{"ReadCapacityUnits":5, "WriteCapacityUnits":5}' \
    --endpoint-url http://localhost:8000 \
    --region us-east-1

aws dynamodb put-item \
    --table-name='embulk-input-dynamodb_example' \
    --item='{
        "primary-key" : { "S" : "key-1" },
        "sort-key" : { "N" : "0" },
        "doubleValue" : { "N" : "42.195" },
        "boolValue" : { "BOOL" : true },
        "listValue" : { "L":
            [
                { "S" : "list-value"},
                { "N" : "123"}
            ]
        },
        "mapValue" : { "M":
            {
                "map-key-1" : { "S" : "map-value-1" },
                "map-key-2" : { "N" : "456" }
            }
        }
    }' \
    --endpoint-url http://localhost:8000 \
    --region us-east-1