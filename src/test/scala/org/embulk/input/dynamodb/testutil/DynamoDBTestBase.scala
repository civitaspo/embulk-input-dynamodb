package org.embulk.input.dynamodb.testutil

import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.dynamodbv2.model.{
  CreateTableRequest,
  DeleteTableRequest
}
import com.amazonaws.services.dynamodbv2.util.TableUtils
import com.amazonaws.services.dynamodbv2.{
  AmazonDynamoDB,
  AmazonDynamoDBClientBuilder
}

import scala.util.chaining._

trait DynamoDBTestBase {
  def dynamoDB(): AmazonDynamoDB

  def withDynamoDB[A](f: AmazonDynamoDB => A): A = {
    dynamoDB().pipe { client =>
      try f(client)
      finally client.shutdown()
    }
  }

  def prepareTable(request: CreateTableRequest) = {
    withDynamoDB { dynamodb =>
      TableUtils.createTableIfNotExists(dynamodb, request)
      TableUtils.waitUntilActive(dynamodb, request.getTableName)
    }
  }

  def cleanupTable(name: String): Unit = {
    withDynamoDB { dynamodb =>
      TableUtils.deleteTableIfExists(
        dynamodb,
        new DeleteTableRequest().withTableName(name)
      )
    }
  }
}

trait DynamoDBLocal {
  val dynamoDBHost: String = "localhost"
  val dynamoDBPort: Int = 8000

  def dynamoDB(): AmazonDynamoDB = {
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
  }
}

trait AmazonDynamoDBClient {
  implicit val accessKey: String
  implicit val secretKey: String
  implicit val region: String

  def dynamoDB(): AmazonDynamoDB = {
    AmazonDynamoDBClientBuilder
      .standard()
      .withCredentials(
        new AWSStaticCredentialsProvider(
          new BasicAWSCredentials(accessKey, secretKey)
        )
      )
      .withRegion(region)
      .build()
  }
}
