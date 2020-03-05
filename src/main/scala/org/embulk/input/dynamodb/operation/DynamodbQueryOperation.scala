package org.embulk.input.dynamodb.operation

import com.amazonaws.services.dynamodbv2.model.QueryRequest
import org.embulk.config.{Config, ConfigDefault}

import scala.util.chaining._

object DynamodbQueryOperation {

  trait Task extends DynamodbOperationCommonOptions.Task {

    // ref. https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Query.html#Query.KeyConditionExpressions
    @Config("key_condition_expression")
    def getKeyConditionExpression: String

    // TODO: Is it needed in the embulk context?
    // ref. https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Query.html#Query.KeyConditionExpressions
    @Config("scan_index_forward")
    @ConfigDefault("true")
    def getScanIndexForward: Boolean

  }
}

case class DynamodbQueryOperation(task: DynamodbQueryOperation.Task) {

  def newRequest: QueryRequest = {
    new QueryRequest()
      .tap(r => DynamodbOperationCommonOptions.configureRequest(r, task))
      .tap(r => r.setKeyConditionExpression(task.getKeyConditionExpression))
      .tap(r => r.setScanIndexForward(task.getScanIndexForward))
  }

  def getEmbulkTaskCount: Int = 1
}
