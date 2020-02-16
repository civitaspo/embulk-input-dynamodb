package org.embulk.input.dynamodb.operation

import java.util.{Optional, Map => JMap}

import com.amazonaws.services.dynamodbv2.model.{ReturnConsumedCapacity, Select}
import org.embulk.config.{Config, ConfigDefault, Task => EmbulkTask}
import org.embulk.input.dynamodb.model.AttributeValue

object AbstractDynamodbOperation {

  trait Task extends EmbulkTask {

    // ref. https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Query.html#Query.ReadConsistency
    // ref. https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Scan.html#Scan.ReadConsistency
    @Config("consistent_read")
    @ConfigDefault("false")
    def getConsistentRead: Boolean

    @Config("exclusive_start_key")
    @ConfigDefault("{}")
    def getExclusiveStartKey: JMap[String, AttributeValue.Task]

    // ref. https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.ExpressionAttributeNames.html
    @Config("expression_attribute_names")
    @ConfigDefault("{}")
    def getExpressionAttributeNames: JMap[String, String]

    // ref. https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.ExpressionAttributeValues.html
    @Config("expression_attribute_values")
    @ConfigDefault("{}")
    def getExpressionAttributeValues: JMap[String, AttributeValue.Task]

    // ref. https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Query.html#Query.FilterExpression
    // ref. https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Scan.html#Scan.FilterExpression
    @Config("filter_expression")
    @ConfigDefault("null")
    def getFilterExpression: Optional[String]

    // ref. https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/SecondaryIndexes.html
    @Config("index_name")
    @ConfigDefault("null")
    def getIndexName: Optional[String]

    // NOTE: Use batch_size for Query/Scan limit per 1 Query/Scan.
    // ref. https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Query.html#Query.Limit
    // ref. https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Scan.html#Scan.Limit
    @Config("batch_size")
    @ConfigDefault("null")
    def getBatchSize: Optional[Int]

    // NOTE: This limit is total records limit, not the limit of Query/Scan request.
    @Config("limit")
    @ConfigDefault("null")
    def getLimit: Optional[Long]

    // ref. https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.ProjectionExpressions.html
    @Config("projection_expression")
    @ConfigDefault("null")
    def getProjectionExpression: Optional[String]

    // TODO: just reporting ?
    @Config("return_consumed_capacity")
    @ConfigDefault("null")
    def getReturnConsumedCapacity: Optional[ReturnConsumedCapacity]

    // ref. https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html#DDB-Query-request-Select
    // ref. https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Scan.html#DDB-Scan-request-Select
    @Config("select")
    @ConfigDefault("null")
    def getSelect: Optional[Select]

    def getTableName: String
    def setTableName(tableName: String): Unit
  }
}

abstract class AbstractDynamodbOperation(task: AbstractDynamodbOperation.Task) {}
