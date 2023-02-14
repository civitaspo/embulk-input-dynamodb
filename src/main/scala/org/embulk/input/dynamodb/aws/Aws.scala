package org.embulk.input.dynamodb.aws

import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.dynamodbv2.{
  AmazonDynamoDB,
  AmazonDynamoDBClientBuilder
}
import org.embulk.util.config.{Task => EmbulkTask}

object Aws {

  trait Task
      extends EmbulkTask
      with AwsCredentials.Task
      with AwsEndpointConfiguration.Task
      with AwsClientConfiguration.Task
      with AwsDynamodbConfiguration.Task

  def apply(task: Task): Aws = {
    new Aws(task)
  }

}

class Aws(task: Aws.Task) {

  def withDynamodb[A](f: AmazonDynamoDB => A): A = {
    val builder: AmazonDynamoDBClientBuilder =
      AmazonDynamoDBClientBuilder.standard()
    AwsDynamodbConfiguration(task).configureAmazonDynamoDBClientBuilder(builder)
    val svc = createService(builder)
    try f(svc)
    finally svc.shutdown()

  }

  def createService[S <: AwsClientBuilder[S, T], T](
      builder: AwsClientBuilder[S, T]
  ): T = {
    AwsEndpointConfiguration(task).configureAwsClientBuilder(builder)
    AwsClientConfiguration(task).configureAwsClientBuilder(builder)
    builder.setCredentials(AwsCredentials(task).createAwsCredentialsProvider)

    builder.build()
  }
}
