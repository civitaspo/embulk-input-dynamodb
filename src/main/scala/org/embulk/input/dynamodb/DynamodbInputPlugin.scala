package org.embulk.input.dynamodb

import java.util.{List => JList}

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import org.embulk.config._
import org.embulk.spi._

class DynamodbInputPlugin extends InputPlugin {
  def transaction(config: ConfigSource, control: InputPlugin.Control): ConfigDiff = {
    val task: PluginTask = config.loadConfig(classOf[PluginTask])

    val schema: Schema = task.getColumns.toSchema
    val taskCount: Int = 1

    resume(task.dump(), schema, taskCount, control)
  }

  def resume(taskSource: TaskSource, schema: Schema, taskCount: Int, control: InputPlugin.Control): ConfigDiff = {
    control.run(taskSource, schema, taskCount)
    Exec.newConfigDiff()
  }

  def run(taskSource: TaskSource, schema: Schema, taskIndex: Int, output: PageOutput): TaskReport = {
    val task: PluginTask = taskSource.loadTask(classOf[PluginTask])

    implicit val client: AmazonDynamoDBClient = DynamoDBUtil.createClient(task)
    DynamoDBUtil.scan(task, schema, output)

    Exec.newTaskReport()
  }

  def cleanup(taskSource: TaskSource, schema: Schema, taskCount: Int, successTaskReports: JList[TaskReport]): Unit = {
    // TODO
  }

  def guess(config: ConfigSource): ConfigDiff = {
    // TODO
    null
  }
}
