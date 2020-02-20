package org.embulk.input.dynamodb

import java.util.{List => JList}

import org.embulk.config._
import org.embulk.spi._

class DynamodbInputPlugin extends InputPlugin {

  override def transaction(
      config: ConfigSource,
      control: InputPlugin.Control
  ): ConfigDiff = {
    val task: PluginTask = PluginTask.load(config)
    if (isDeprecatedOperationRequired(task))
      return DeprecatedDynamodbInputPlugin.transaction(config, control)

    // TODO: Implement new operation
    val schema: Schema = task.getColumns.toSchema
    val taskCount: Int = 1

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

    // TODO: Implement new operation
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
