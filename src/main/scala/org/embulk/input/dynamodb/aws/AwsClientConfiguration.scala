package org.embulk.input.dynamodb.aws

import java.util.Optional

import com.amazonaws.ClientConfiguration
import com.amazonaws.client.builder.AwsClientBuilder
import org.embulk.util.config.{Config, ConfigDefault, Task => EmbulkTask}
import org.embulk.input.dynamodb.aws.AwsClientConfiguration.Task

object AwsClientConfiguration {

  trait Task extends EmbulkTask {

    @Config("http_proxy")
    @ConfigDefault("null")
    def getHttpProxy: Optional[HttpProxy.Task]

  }

  def apply(task: Task): AwsClientConfiguration = {
    new AwsClientConfiguration(task)
  }
}

class AwsClientConfiguration(task: Task) {

  def configureAwsClientBuilder[S <: AwsClientBuilder[S, T], T](
      builder: AwsClientBuilder[S, T]
  ): Unit = {
    task.getHttpProxy.ifPresent { v =>
      val cc = new ClientConfiguration
      HttpProxy(v).configureClientConfiguration(cc)
      builder.setClientConfiguration(cc)
    }
  }

}
