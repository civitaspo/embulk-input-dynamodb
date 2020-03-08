package org.embulk.input.dynamodb

import java.util.{List => JList}

import org.embulk.config.{
  ConfigDiff,
  ConfigException,
  ConfigSource,
  TaskReport,
  TaskSource
}
import org.embulk.input.dynamodb.aws.Aws
import org.embulk.input.dynamodb.deprecated.ope.{QueryOperation, ScanOperation}
import org.embulk.spi.{Exec, InputPlugin, PageOutput, Schema}

@deprecated(since = "0.3.0")
object DeprecatedDynamodbInputPlugin extends InputPlugin {

  override def transaction(
      config: ConfigSource,
      control: InputPlugin.Control
  ): ConfigDiff = {
    val task: PluginTask = PluginTask.load(config)
    val schema: Schema = task.getColumns.toSchema
    if (schema.isEmpty)
      throw new ConfigException("\"columns\" option must be set.")
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
    throw new UnsupportedOperationException
  }

  override def run(
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
