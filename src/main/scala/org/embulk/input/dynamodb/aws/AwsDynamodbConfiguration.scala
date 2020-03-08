package org.embulk.input.dynamodb.aws

import java.util.Optional

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import org.embulk.config.{Config, ConfigDefault}
import org.embulk.input.dynamodb.aws.AwsDynamodbConfiguration.Task

object AwsDynamodbConfiguration {

  trait Task {

    @Config("enable_endpoint_discovery")
    @ConfigDefault("null")
    def getEnableEndpointDiscovery: Optional[Boolean]

  }

  def apply(task: Task): AwsDynamodbConfiguration = {
    new AwsDynamodbConfiguration(task)
  }
}

class AwsDynamodbConfiguration(task: Task) {

  def configureAmazonDynamoDBClientBuilder(
      builder: AmazonDynamoDBClientBuilder
  ): Unit = {
    task.getEnableEndpointDiscovery.ifPresent { v =>
      if (v) builder.enableEndpointDiscovery()
      else builder.disableEndpointDiscovery()
    }
  }

}
