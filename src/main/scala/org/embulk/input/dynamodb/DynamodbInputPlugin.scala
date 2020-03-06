package org.embulk.input.dynamodb

import java.util.{List => JList}

import org.embulk.config.{ConfigDiff, ConfigSource, TaskReport, TaskSource}
import org.embulk.input.dynamodb.aws.Aws
import org.embulk.input.dynamodb.item.DynamodbItemSchema
import org.embulk.input.dynamodb.operation.DynamodbOperationProxy
import org.embulk.spi.{Exec, InputPlugin, PageBuilder, PageOutput, Schema}

class DynamodbInputPlugin extends InputPlugin {

  override def transaction(
      config: ConfigSource,
      control: InputPlugin.Control
  ): ConfigDiff = {
    val task: PluginTask = PluginTask.load(config)
    if (isDeprecatedOperationRequired(task))
      return DeprecatedDynamodbInputPlugin.transaction(config, control)

    val schema: Schema = DynamodbItemSchema(task).getEmbulkSchema
    val taskCount: Int = DynamodbOperationProxy(task).getEmbulkTaskCount

    control.run(task.dump(), schema, taskCount)
    Exec.newConfigDiff()
  }

  override def resume(
      taskSource: TaskSource,
      schema: Schema,
      taskCount: Int,
      control: InputPlugin.Control
  ): ConfigDiff = {
    val task: PluginTask = PluginTask.load(taskSource)
    if (isDeprecatedOperationRequired(task))
      return DeprecatedDynamodbInputPlugin.resume(
        taskSource,
        schema,
        taskCount,
        control
      )
    throw new UnsupportedOperationException
  }

  override def run(
      taskSource: TaskSource,
      schema: Schema,
      taskIndex: Int,
      output: PageOutput
  ): TaskReport = {
    val task: PluginTask = PluginTask.load(taskSource)
    if (isDeprecatedOperationRequired(task))
      return DeprecatedDynamodbInputPlugin.run(
        taskSource,
        schema,
        taskIndex,
        output
      )

    val pageBuilder = new PageBuilder(task.getBufferAllocator, schema, output)

    Aws(task).withDynamodb { dynamodb =>
      DynamodbOperationProxy(task).run(
        dynamodb,
        taskIndex,
        DynamodbItemSchema(task).getItemsConsumer(pageBuilder)
      )
    }
    pageBuilder.finish()
    Exec.newTaskReport()
  }

  override def cleanup(
      taskSource: TaskSource,
      schema: Schema,
      taskCount: Int,
      successTaskReports: JList[TaskReport]
  ): Unit = {
    val task: PluginTask = PluginTask.load(taskSource)
    if (isDeprecatedOperationRequired(task))
      DeprecatedDynamodbInputPlugin.cleanup(
        taskSource,
        schema,
        taskCount,
        successTaskReports
      )
  }

  override def guess(config: ConfigSource): ConfigDiff = {
    val task: PluginTask = PluginTask.load(config)
    if (isDeprecatedOperationRequired(task))
      return DeprecatedDynamodbInputPlugin.guess(config)
    throw new UnsupportedOperationException
  }

  private def isDeprecatedOperationRequired(task: PluginTask): Boolean =
    task.getOperation.isPresent
}
