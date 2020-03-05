package org.embulk.input.dynamodb.operation

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.model.AttributeValue

trait EmbulkDynamodbOperation {

  def getEmbulkTaskCount: Int = 1

  def run(
      dynamodb: AmazonDynamoDB,
      embulkTaskIndex: Int,
      f: List[Map[String, AttributeValue]] => Unit
  ): Unit
}
