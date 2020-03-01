package org.embulk.input.dynamodb.item

trait DynamodbItemIterator extends Iterator[Map[String, DynamodbAttributeValue]]
