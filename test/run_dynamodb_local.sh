#!/bin/sh

docker run -i -t -d \
	-p 8000:8000 \
	tray/dynamodb-local -inMemory -port 8000

