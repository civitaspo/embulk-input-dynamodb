package org.embulk.input.dynamodb

import com.amazonaws.ClientConfiguration
import com.amazonaws.regions.Regions
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import org.embulk.config.ConfigException

object DynamoDBClient {
  def create(task: PluginTask): AmazonDynamoDBClient = {
    // TODO: Fix the deprecation warnings: Be careful as the behavior of the new SDK interface may change significantly.
    //   - constructor AmazonDynamoDBClient in class AmazonDynamoDBClient is deprecated: see corresponding Javadoc for more information.
    //   - method withEndpoint in class AmazonWebServiceClient is deprecated: see corresponding Javadoc for more information.
    //   - method withRegion in class AmazonWebServiceClient is deprecated: see corresponding Javadoc for more information.
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
