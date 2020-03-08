package org.embulk.input.dynamodb

import java.util.Optional

import org.embulk.config.{
  Config,
  ConfigDefault,
  ConfigException,
  ConfigInject,
  ConfigSource,
  Task,
  TaskSource
}
import org.embulk.input.dynamodb.aws.Aws
import org.embulk.input.dynamodb.deprecated.Filter
import org.embulk.input.dynamodb.item.DynamodbItemSchema
import org.embulk.input.dynamodb.operation.{
  DynamodbOperationProxy,
  DynamodbQueryOperation,
  DynamodbScanOperation
}
import org.embulk.spi.BufferAllocator
import zio.macros.annotation.delegate

import scala.util.chaining._

trait PluginTask
    extends Task
    with Aws.Task
    with DynamodbItemSchema.Task
    with DynamodbOperationProxy.Task {

  @deprecated(
    message = "Use #getScan() or #getQuery() instead.",
    since = "0.3.0"
  )
  @Config("operation")
  @ConfigDefault("null")
  def getOperation: Optional[String]

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

case class PluginTaskCompat(@delegate task: PluginTask) extends PluginTask {

  override def getOperation: Optional[String] = {
    task.getOperation.ifPresent { op =>
      logger.warn(
        "[Deprecated] The option \"operation\" is deprecated. Use \"scan\" or \"query\" option instead."
      )

      op.toLowerCase match {
        case "scan" | "query" => // do nothing
        case x =>
          throw new ConfigException(
            s"Operation '$x' is unsupported. Available values are 'scan' or 'query'."
          )
      }

      if (getScan.isPresent || getQuery.isPresent)
        throw new ConfigException(
          "The option \"operation\" must not be used together with either \"scan\" or \"query\" options."
        )
    }
    task.getOperation
  }

  override def getLimit: Long = {
    logger.warn(
      "[Deprecated] The option \"limit\" is deprecated. Use \"query.batch_size\" or \"scan.batch_size\" instead."
    )
    task.getLimit
  }

  override def getScanLimit: Long = {
    logger.warn(
      "[Deprecated] The option \"scan_limit\" is deprecated. Use \"query.batch_size\" or \"scan.batch_size\" instead."
    )
    task.getScanLimit
  }

  override def getRecordLimit: Long = {
    logger.warn(
      "[Deprecated] The option \"record_limit\" is deprecated. Use \"query.limit\" or \"scan.limit\" instead."
    )
    task.getRecordLimit
  }

  override def getFilters: Optional[Filter] = {
    logger.warn(
      "[Deprecated] The option \"filters\" is deprecated. Use \"query.filter_expression\" or \"scan.filter_expression\" instead."
    )
    task.getFilters
  }
}

object PluginTask {

  def load(configSource: ConfigSource): PluginTask = {
    configSource
      .loadConfig(classOf[PluginTask])
      .pipe(PluginTaskCompat)
      .tap(configure)
  }

  def load(taskSource: TaskSource): PluginTask = {
    taskSource
      .loadTask(classOf[PluginTask])
      .pipe(PluginTaskCompat)
      .tap(configure)
  }

  private def configure(task: PluginTask): Unit = {
    if (!task.getOperation.isPresent && !task.getScan.isPresent && !task.getQuery.isPresent) {
      // NOTE: "operation" option is deprecated, so this is not shown the message.
      throw new ConfigException(
        "Either \"scan\" or \"query\" option is required."
      )
    }
  }
}
