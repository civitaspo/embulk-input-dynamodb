package org.embulk.input.dynamodb.testutil


import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.dynamodbv2.{AmazonDynamoDB, AmazonDynamoDBClientBuilder}
import org.embulk.input.dynamodb.DynamodbInputPlugin
import org.embulk.spi.InputPlugin
import org.embulk.test.TestingEmbulk
import org.junit.{After, Rule}

import scala.util.chaining._
import scala.util.{Failure, Success, Try}

trait EmbulkTestBase
{
    val dynamoDBHost: String = "localhost"
    val dynamoDBPort: Int = 8000

    def withDynamoDB[A](f: AmazonDynamoDB => A): A = {
        AmazonDynamoDBClientBuilder.standard()
            .withEndpointConfiguration(
                new EndpointConfiguration(s"http://$dynamoDBHost:$dynamoDBPort", "us-east-1")
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
        withDynamoDB { dynamodb =>
            Try(dynamodb.describeTable(name)) match {
                case Success(_) => dynamodb.deleteTable(name)
                case Failure(_) => // Do nothing.
            }
        }
    }

    @Rule
    def embulk: TestingEmbulk = TestingEmbulk.builder()
        .registerPlugin(classOf[InputPlugin], "dynamodb", classOf[DynamodbInputPlugin])
        .build()

    @After
    def destroyEmbulk(): Unit = {
        embulk.destroy()
    }

    def getEnvironmentVariableOrShowErrorMessage(name: String): String = {
        try {
            Option(System.getenv(name)) match {
                case Some(x) => x
                case None => throw new IllegalStateException(s"Please set the environment variable: $name")
            }
        }
        catch {
            case e: Throwable => throw new IllegalStateException(s"Please set the environment variable: $name", e)
        }
    }
}
