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
    val schema: Schema = DynamodbItemSchema(task).getEmbulkSchema
    val taskCount: Int = DynamodbOperationProxy(task).getEmbulkTaskCount

    control.run(task.toTaskSource(), schema, taskCount)
    PluginTask.newConfigDiff()
  }

  override def resume(
      taskSource: TaskSource,
      schema: Schema,
      taskCount: Int,
      control: InputPlugin.Control
  ): ConfigDiff = {
    throw new UnsupportedOperationException
  }

  override def run(
      taskSource: TaskSource,
      schema: Schema,
      taskIndex: Int,
      output: PageOutput
  ): TaskReport = {
    val task: PluginTask = PluginTask.load(taskSource)
    val pageBuilder =
      Exec.getPageBuilder(Exec.getBufferAllocator(), schema, output)

    Aws(task).withDynamodb { dynamodb =>
      DynamodbOperationProxy(task).run(
        dynamodb,
        taskIndex,
        DynamodbItemSchema(task).getItemsConsumer(pageBuilder)
      )
    }
    pageBuilder.finish()
    PluginTask.newTaskReport()
  }

  override def cleanup(
      taskSource: TaskSource,
      schema: Schema,
      taskCount: Int,
      successTaskReports: JList[TaskReport]
  ): Unit = {}

  override def guess(config: ConfigSource): ConfigDiff = {
    throw new UnsupportedOperationException
  }
}
