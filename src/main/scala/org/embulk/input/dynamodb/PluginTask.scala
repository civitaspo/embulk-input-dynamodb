package org.embulk.input.dynamodb

import java.util.Optional

import org.embulk.config.{
  ConfigDiff,
  ConfigException,
  ConfigSource,
  TaskSource,
  TaskReport
}
import org.embulk.util.config.{
  Config,
  ConfigDefault,
  ConfigMapperFactory,
  Task => EmbulkTask
}
import org.embulk.input.dynamodb.aws.Aws
import org.embulk.input.dynamodb.item.DynamodbItemSchema
import org.embulk.input.dynamodb.operation.DynamodbOperationProxy
import org.embulk.spi.BufferAllocator

import scala.util.chaining._

trait PluginTask
    extends EmbulkTask
    with Aws.Task
    with DynamodbItemSchema.Task
    with DynamodbOperationProxy.Task {}

object PluginTask {
  private val configMapperFactory: ConfigMapperFactory =
    ConfigMapperFactory.builder().addDefaultModules().build()

  def load(configSource: ConfigSource): PluginTask = {
    configMapperFactory
      .createConfigMapper()
      .map(configSource, classOf[PluginTask])
  }

  def load(taskSource: TaskSource): PluginTask = {
    configMapperFactory.createTaskMapper().map(taskSource, classOf[PluginTask])
  }

  def newConfigDiff(): ConfigDiff = configMapperFactory.newConfigDiff()
  def newTaskReport(): TaskReport = configMapperFactory.newTaskReport()
}
