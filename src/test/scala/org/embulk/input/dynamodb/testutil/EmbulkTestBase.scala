package org.embulk.input.dynamodb.testutil

import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.dynamodbv2.{
  AmazonDynamoDB,
  AmazonDynamoDBClientBuilder
}
import org.embulk.config.{ConfigLoader, ConfigSource, TaskReport, TaskSource}
import org.embulk.input.dynamodb.DynamodbInputPlugin
import org.embulk.spi.Schema
import org.embulk.EmbulkTestRuntime
import org.embulk.spi.TestPageBuilderReader.MockPageOutput
import org.embulk.spi.util.Pages
import org.junit.Rule

import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}
import scala.util.chaining._

trait EmbulkTestBase {
  val dynamoDBHost: String = "localhost"
  val dynamoDBPort: Int = 8000

  def withDynamodb[A](f: AmazonDynamoDB => A): A = {
    AmazonDynamoDBClientBuilder
      .standard()
      .withEndpointConfiguration(
        new EndpointConfiguration(
          s"http://$dynamoDBHost:$dynamoDBPort",
          "us-east-1"
        )
      )
      .withCredentials(
        new AWSStaticCredentialsProvider(
          new BasicAWSCredentials("dummy", "dummy")
        )
      )
      .build()
      .pipe { client =>
        try f(client)
        finally client.shutdown()
      }
  }

  def cleanupTable(name: String): Unit = {
    withDynamodb { dynamodb =>
      Try(dynamodb.describeTable(name)) match {
        case Success(_) => dynamodb.deleteTable(name)
        case Failure(_) => // Do nothing.
      }
    }
  }

  @Rule
  def runtime: EmbulkTestRuntime = new EmbulkTestRuntime()

  def getEnvironmentVariableOrShowErrorMessage(name: String): String = {
    try {
      Option(System.getenv(name)) match {
        case Some(x) => x
        case None =>
          throw new IllegalStateException(
            s"Please set the environment variable: $name"
          )
      }
    }
    catch {
      case e: Throwable =>
        throw new IllegalStateException(
          s"Please set the environment variable: $name",
          e
        )
    }
  }

  def runInput(inConfig: ConfigSource, test: Seq[Seq[AnyRef]] => Unit): Unit = {
    val plugin = new DynamodbInputPlugin()
    plugin.transaction(
      inConfig,
      (taskSource: TaskSource, schema: Schema, taskCount: Int) => {
        val output: MockPageOutput = new MockPageOutput()
        val reports: Seq[TaskReport] = 0.until(taskCount).map { taskIndex =>
          plugin.run(taskSource, schema, taskIndex, output)
        }
        output.finish()

        test(Pages.toObjects(schema, output.pages).asScala.toSeq.map(_.toSeq))

        reports.asJava
      }
    )
  }

  def loadConfigSourceFromYamlString(yaml: String): ConfigSource = {
    new ConfigLoader(runtime.getModelManager).fromYamlString(yaml)
  }
}
