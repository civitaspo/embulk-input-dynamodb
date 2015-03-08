package org.embulk.input

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

  def cleanup(taskSource: TaskSource, schema: Schema, taskCount: Int, successCommitReports: JList[CommitReport]): Unit = {
  }

  def run(taskSource: TaskSource, schema: Schema, taskIndex: Int, output: PageOutput): CommitReport = {
    val task: PluginTask = taskSource.loadTask(classOf[PluginTask])

    val client: AmazonDynamoDBClient = DynamoDBUtil.createClient(task)
    DynamoDBUtil.scan(client, task, schema, output)

    Exec.newCommitReport()
  }
}
