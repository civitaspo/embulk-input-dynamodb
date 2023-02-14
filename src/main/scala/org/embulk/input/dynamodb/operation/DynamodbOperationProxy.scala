package org.embulk.input.dynamodb.operation

import java.util.Optional

import com.amazonaws.services.dynamodbv2.model.AttributeValue
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import org.embulk.config.ConfigException
import org.embulk.util.config.{Config, ConfigDefault, Task => EmbulkTask}
import org.embulk.input.dynamodb.operation.{
  DynamodbQueryOperation,
  DynamodbScanOperation,
  EmbulkDynamodbOperation
}

object DynamodbOperationProxy {

  trait Task extends EmbulkTask {

    @Config("scan")
    @ConfigDefault("null")
    def getScan: Optional[DynamodbScanOperation.Task]

    @Config("query")
    @ConfigDefault("null")
    def getQuery: Optional[DynamodbQueryOperation.Task]

    @Config("table")
    def getTable: String
  }

  def apply(task: Task): DynamodbOperationProxy = {
    if (task.getScan.isPresent && task.getQuery.isPresent)
      throw new ConfigException("You can use either \"scan\" or \"query\".")
    if (!task.getScan.isPresent && !task.getQuery.isPresent)
      throw new ConfigException("You must set either \"scan\" or \"query\".")
    task.getScan.ifPresent(_.setTableName(task.getTable))
    task.getQuery.ifPresent(_.setTableName(task.getTable))
    new DynamodbOperationProxy(task)
  }
}

case class DynamodbOperationProxy(task: DynamodbOperationProxy.Task)
    extends EmbulkDynamodbOperation {

  private def getOperation: EmbulkDynamodbOperation = {
    task.getScan.ifPresent(t => return DynamodbScanOperation(t))
    task.getQuery.ifPresent(t => return DynamodbQueryOperation(t))
    throw new IllegalStateException()
  }

  private val operation: EmbulkDynamodbOperation = getOperation

  override def getEmbulkTaskCount: Int = operation.getEmbulkTaskCount

  override def run(
      dynamodb: AmazonDynamoDB,
      embulkTaskIndex: Int,
      f: Seq[Map[String, AttributeValue]] => Unit
  ): Unit = operation.run(dynamodb, embulkTaskIndex, f)
}
