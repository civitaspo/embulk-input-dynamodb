package org.embulk.input.dynamodb.operation

import java.lang.{
  Boolean => JBoolean,
  Integer => JInteger,
  Long => JLong,
  String => JString
}
import java.util.{Optional, Map => JMap}

import com.amazonaws.services.dynamodbv2.model.{
  AttributeValue,
  ReturnConsumedCapacity,
  Select
}
import org.embulk.config.ConfigException
import org.embulk.util.config.{Config, ConfigDefault, Task => EmbulkTask}
import org.embulk.input.dynamodb.item.DynamodbAttributeValue

import scala.jdk.CollectionConverters._
import scala.language.reflectiveCalls

object AbstractDynamodbOperation {

  trait Task extends EmbulkTask {

    // ref. https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Query.html#Query.ReadConsistency
    // ref. https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Scan.html#Scan.ReadConsistency
    @Config("consistent_read")
    @ConfigDefault("false")
    def getConsistentRead: Boolean

    // ref. https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Scan.html#Scan.Pagination
    // ref. https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Query.Pagination.html
    @Config("exclusive_start_key")
    @ConfigDefault("{}")
    def getExclusiveStartKey: JMap[String, DynamodbAttributeValue.Task]

    // ref. https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.ExpressionAttributeNames.html
    @Config("expression_attribute_names")
    @ConfigDefault("{}")
    def getExpressionAttributeNames: JMap[String, String]

    // ref. https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.ExpressionAttributeValues.html
    @Config("expression_attribute_values")
    @ConfigDefault("{}")
    def getExpressionAttributeValues: JMap[String, DynamodbAttributeValue.Task]

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
    def getLimit: Optional[JLong]

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

  type RequestBuilderMethods = {
    def setConsistentRead(v: JBoolean): Unit
    def setExclusiveStartKey(v: JMap[JString, AttributeValue]): Unit
    def setExpressionAttributeNames(v: JMap[JString, JString]): Unit
    def setExpressionAttributeValues(v: JMap[JString, AttributeValue]): Unit
    def setFilterExpression(v: JString): Unit
    def setIndexName(v: JString): Unit
    def setLimit(v: JInteger): Unit
    def setProjectionExpression(v: JString): Unit
    def setReturnConsumedCapacity(v: ReturnConsumedCapacity): Unit
    def setSelect(v: Select): Unit
    def setTableName(v: JString): Unit
  }

}

abstract class AbstractDynamodbOperation(
    task: AbstractDynamodbOperation.Task
) extends EmbulkDynamodbOperation {

  protected def calculateLoadableRecords(loadedRecords: Long): Option[Long] = {
    if (!task.getLimit.isPresent) return None
    val loadableRecords = task.getLimit.get() - loadedRecords
    if (loadableRecords <= 0) Option(0L)
    else Option(loadableRecords)
  }

  protected def configureRequest[
      A <: AbstractDynamodbOperation.RequestBuilderMethods
  ](
      req: A,
      lastEvaluatedKey: Option[Map[String, AttributeValue]]
  ): Unit = {
    def attributeValueTaskToAttributeValue(
        x: (String, DynamodbAttributeValue.Task)
    ): (String, AttributeValue) = {
      (x._1, DynamodbAttributeValue(x._2).getOriginal)
    }

    req.setConsistentRead(task.getConsistentRead)
    lastEvaluatedKey match {
      case Some(v) => req.setExclusiveStartKey(v.asJava)
      case None =>
        if (!task.getExclusiveStartKey.isEmpty)
          req.setExclusiveStartKey(
            task.getExclusiveStartKey.asScala
              .map(attributeValueTaskToAttributeValue)
              .asJava
          )
    }

    if (!task.getExpressionAttributeNames.isEmpty)
      req.setExpressionAttributeNames(task.getExpressionAttributeNames)
    if (!task.getExpressionAttributeValues.isEmpty)
      req.setExpressionAttributeValues(
        task.getExpressionAttributeValues.asScala
          .map(attributeValueTaskToAttributeValue)
          .asJava
      )
    task.getFilterExpression.ifPresent(req.setFilterExpression)
    task.getIndexName.ifPresent(req.setIndexName)
    task.getBatchSize.ifPresent { v =>
      if (v <= 0)
        throw new ConfigException(
          "\"batch_size\" must be greater than or equal to 1."
        )
      req.setLimit(
        JInteger.valueOf(v)
      ) // Note: Use BatchSize for the limit per a request.
    }
    task.getProjectionExpression.ifPresent(req.setProjectionExpression)
    task.getReturnConsumedCapacity.ifPresent(req.setReturnConsumedCapacity)
    task.getSelect.ifPresent(req.setSelect)
    req.setTableName(task.getTableName)
  }
}
