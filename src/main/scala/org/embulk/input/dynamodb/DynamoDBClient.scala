package org.embulk.input.dynamodb

import com.amazonaws.ClientConfiguration
import com.amazonaws.regions.Regions
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import org.embulk.config.ConfigException

object DynamoDBClient {
  def create(task: PluginTask): AmazonDynamoDBClient = {
    val client = new AmazonDynamoDBClient(
      AwsCredentials.getCredentialsProvider(task),
      new ClientConfiguration()
        .withMaxConnections(50))  // SDK Default Value

    if (task.getEndPoint.isPresent) {
      client.withEndpoint(task.getEndPoint.get())
    } else if (task.getRegion.isPresent) {
      client.withRegion(Regions.fromName(task.getRegion.get()))
    } else {
      throw new ConfigException("At least one of EndPoint or Region must be set")
    }
  }
}
