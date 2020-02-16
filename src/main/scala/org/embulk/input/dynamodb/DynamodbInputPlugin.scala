package org.embulk.input.dynamodb

import java.util.{List => JList}

import org.embulk.config._
import org.embulk.input.dynamodb.aws.Aws
import org.embulk.input.dynamodb.deprecated.ope.{QueryOperation, ScanOperation}
import org.embulk.spi._

class DynamodbInputPlugin extends InputPlugin {

  def transaction(
      config: ConfigSource,
      control: InputPlugin.Control
  ): ConfigDiff = {
    val task: PluginTask = PluginTask.load(config)
    val schema: Schema = task.getColumns.toSchema
    val taskCount: Int = 1

    resume(task.dump(), schema, taskCount, control)
  }

  def resume(
      taskSource: TaskSource,
      schema: Schema,
      taskCount: Int,
      control: InputPlugin.Control
  ): ConfigDiff = {
    control.run(taskSource, schema, taskCount)
    Exec.newConfigDiff()
  }

  def run(
      taskSource: TaskSource,
      schema: Schema,
      taskIndex: Int,
      output: PageOutput
  ): TaskReport = {
    val task: PluginTask = PluginTask.load(taskSource)

    Aws(task).withDynamodb { dynamodb =>
      task.getOperation.ifPresent { ope =>
        val o = ope.toLowerCase match {
          case "scan"  => new ScanOperation(dynamodb)
          case "query" => new QueryOperation(dynamodb)
        }
        o.execute(task, schema, output)
      }
    }

    Exec.newTaskReport()
  }

  def cleanup(
      taskSource: TaskSource,
      schema: Schema,
      taskCount: Int,
      successTaskReports: JList[TaskReport]
  ): Unit = {
    // TODO
  }

  def guess(config: ConfigSource): ConfigDiff = {
    // TODO
    null
  }
}
