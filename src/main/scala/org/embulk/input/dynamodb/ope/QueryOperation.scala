package org.embulk.input.dynamodb.ope

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import org.embulk.input.dynamodb.PluginTask
import org.embulk.spi.{PageOutput, Schema}

class QueryOperation(client: AmazonDynamoDBClient) extends AbstractOperation {
  override def execute(task: PluginTask,
                       schema: Schema,
                       output: PageOutput): Unit =
  {

  }
}
