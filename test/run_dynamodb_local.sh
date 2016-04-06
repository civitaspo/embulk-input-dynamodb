#!/bin/sh

docker run -d \
	-p 8000:8000 \
	-v $PWD/dynamodb-local:/data \
	--env DYNAMO_OPT='-dbPath /data -sharedDb' \
	lulichn/dynamodb-local
