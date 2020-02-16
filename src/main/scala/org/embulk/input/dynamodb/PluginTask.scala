package org.embulk.input.dynamodb

import java.util.Optional

import org.embulk.config.{
  Config,
  ConfigDefault,
  ConfigInject,
  ConfigSource,
  Task,
  TaskSource
}
import org.embulk.input.dynamodb.aws.Aws
import org.embulk.input.dynamodb.deprecated.Filter
import org.embulk.input.dynamodb.operation.{
  DynamodbQueryOperation,
  DynamodbScanOperation
}
import org.embulk.spi.{BufferAllocator, SchemaConfig}

import scala.util.chaining._

trait PluginTask extends Task with Aws.Task {

  @deprecated(
    message = "Use #getScan() or #getQuery() instead.",
    since = "0.3.0"
  )
  @Config("operation")
  def getOperation: String

  @deprecated(
    message =
      "Use DynamodbQueryOperation.Task#getBatchSize() or DynamodbScanOperation.Task#getBatchSize() instead.",
    since = "0.3.0"
  )
  @Config("limit")
  @ConfigDefault("0")
  def getLimit: Long

  @deprecated(
    message =
      "Use DynamodbQueryOperation.Task#getBatchSize() or DynamodbScanOperation.Task#getBatchSize() instead.",
    since = "0.3.0"
  )
  @Config("scan_limit")
  @ConfigDefault("0")
  def getScanLimit: Long

  @deprecated(
    message =
      "Use DynamodbQueryOperation.Task#getLimit() or DynamodbScanOperation.Task#getLimit() instead.",
    since = "0.3.0"
  )
  @Config("record_limit")
  @ConfigDefault("0")
  def getRecordLimit: Long

  @Config("table")
  def getTable: String

  @Config("columns")
  def getColumns: SchemaConfig

  @deprecated(
    message =
      "Use DynamodbQueryOperation.Task#getFilterExpression() or DynamodbScanOperation.Task#getFilterExpression() instead.",
    since = "0.3.0"
  )
  @Config("filters")
  @ConfigDefault("null")
  def getFilters: Optional[Filter]

  @Config("scan")
  @ConfigDefault("null")
  def getScan: Optional[DynamodbScanOperation.Task]

  @Config("query")
  @ConfigDefault("null")
  def getQuery: Optional[DynamodbQueryOperation.Task]

  @ConfigInject
  def getBufferAllocator: BufferAllocator
}

object PluginTask {

  def load(configSource: ConfigSource): PluginTask = {
    configSource
      .loadConfig(classOf[PluginTask])
      .tap(configure)
  }

  def load(taskSource: TaskSource): PluginTask = {
    taskSource
      .loadTask(classOf[PluginTask])
      .tap(configure)
  }

  private def configure(task: PluginTask): Unit = {
    // TODO: Show deprecation warnings
  }
}
