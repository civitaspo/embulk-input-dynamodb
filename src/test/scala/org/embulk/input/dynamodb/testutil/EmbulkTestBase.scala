package org.embulk.input.dynamodb.testutil

import org.embulk.input.dynamodb.DynamodbInputPlugin
import org.embulk.spi.InputPlugin
import org.embulk.test.TestingEmbulk
import org.junit.{After, Rule}

trait EmbulkTestBase {

  @Rule
  def embulk: TestingEmbulk =
    TestingEmbulk
      .builder()
      .registerPlugin(
        classOf[InputPlugin],
        "dynamodb",
        classOf[DynamodbInputPlugin]
      )
      .build()

  @After
  def destroyEmbulk(): Unit = {
    embulk.destroy()
  }

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

  def getEnvironmentVariable(name: String, default: String): String = {
    Option(System.getenv(name)).getOrElse(default)
  }
}
