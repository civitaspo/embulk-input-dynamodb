#!/bin/sh

aws dynamodb put-item \
	--table-name='EMBULK_DYNAMODB_TEST_TABLE' \
	--item='{
		"pri-key" : { "S" : "key-1" },
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

