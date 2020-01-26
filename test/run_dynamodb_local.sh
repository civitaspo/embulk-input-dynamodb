#!/bin/sh

docker run -i -t -d \
	-p 8000:8000 \
	amazon/dynamodb-local:latest \
	-jar DynamoDBLocal.jar \
	-inMemory -sharedDb -port 8000

