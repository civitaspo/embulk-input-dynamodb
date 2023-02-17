package org.embulk.input.dynamodb.operation

import com.amazonaws.services.dynamodbv2.model.{AttributeValue, QueryRequest}
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import org.embulk.util.config.{Config, ConfigDefault}
import org.embulk.input.dynamodb.logger

import scala.jdk.CollectionConverters._
import scala.util.chaining._
import scala.annotation.tailrec

object DynamodbQueryOperation {

  trait Task extends AbstractDynamodbOperation.Task {

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

case class DynamodbQueryOperation(task: DynamodbQueryOperation.Task)
    extends AbstractDynamodbOperation(task) {

  private def newRequest(
      lastEvaluatedKey: Option[Map[String, AttributeValue]]
  ): QueryRequest = {
    new QueryRequest()
      .tap(configureRequest(_, lastEvaluatedKey))
      .tap(r => r.setKeyConditionExpression(task.getKeyConditionExpression))
      .tap(r => r.setScanIndexForward(task.getScanIndexForward))
  }

  @tailrec
  private def runInternal(
      dynamodb: AmazonDynamoDB,
      f: Seq[Map[String, AttributeValue]] => Unit,
      lastEvaluatedKey: Option[Map[String, AttributeValue]] = None,
      loadedRecords: Long = 0
  ): Unit = {
    val loadableRecords: Option[Long] = calculateLoadableRecords(loadedRecords)

    val result = dynamodb.query(newRequest(lastEvaluatedKey).tap { req =>
      logger.info(s"Call DynamodbQueryRequest: ${req.toString}")
    })
    loadableRecords match {
      case Some(v) if (result.getCount > v) =>
        f(result.getItems.asScala.take(v.toInt).map(_.asScala.toMap).toSeq)
      case _ =>
        f(result.getItems.asScala.map(_.asScala.toMap).toSeq)
        Option(result.getLastEvaluatedKey) match {
          case Some(v) =>
            runInternal(
              dynamodb,
              f,
              lastEvaluatedKey = Option(v.asScala.toMap),
              loadedRecords = loadedRecords + result.getCount
            )
          case None => // do nothing
        }
    }
  }

  override def run(
      dynamodb: AmazonDynamoDB,
      embulkTaskIndex: Int,
      f: Seq[Map[String, AttributeValue]] => Unit
  ): Unit = runInternal(dynamodb, f)
}
