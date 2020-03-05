package org.embulk.input.dynamodb.operation

import java.util.Optional

import org.embulk.config.{
  Config,
  ConfigDefault,
  ConfigException,
  Task => EmbulkTask
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

case class DynamodbOperationProxy(task: DynamodbOperationProxy.Task) {}
