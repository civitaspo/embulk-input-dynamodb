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
import org.embulk.input.dynamodb.item.DynamodbItemSchema
import org.embulk.input.dynamodb.operation.DynamodbOperationProxy
import org.embulk.spi.BufferAllocator
import zio.macros.annotation.delegate

import scala.util.chaining._

trait PluginTask
    extends Task
    with Aws.Task
    with DynamodbItemSchema.Task
    with DynamodbOperationProxy.Task {

  @ConfigInject
  def getBufferAllocator: BufferAllocator
}

object PluginTask {

  def load(configSource: ConfigSource): PluginTask = {
    configSource
      .loadConfig(classOf[PluginTask])
  }

  def load(taskSource: TaskSource): PluginTask = {
    taskSource
      .loadTask(classOf[PluginTask])
  }
}
