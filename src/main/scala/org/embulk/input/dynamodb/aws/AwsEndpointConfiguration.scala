package org.embulk.input.dynamodb.aws

import java.util.Optional

import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.regions.{DefaultAwsRegionProviderChain, Regions}
import org.embulk.config.{Config, ConfigDefault, ConfigException}
import org.embulk.input.dynamodb.aws.AwsEndpointConfiguration.Task
import org.embulk.input.dynamodb.logger

import scala.util.Try

object AwsEndpointConfiguration {

  trait Task {

    @deprecated(message = "Use #getEndpoint() instead.", since = "0.3.0")
    @Config("end_point")
    @ConfigDefault("null")
    def getEndPoint: Optional[String]

    @Config("endpoint")
    @ConfigDefault("null")
    def getEndpoint: Optional[String]

    @Config("region")
    @ConfigDefault("null")
    def getRegion: Optional[String]
  }

  case class TaskCompat(task: Task) extends Task {
    override def getEndPoint: Optional[String] = throw new NotImplementedError()

    override def getEndpoint: Optional[String] = {
      if (task.getEndpoint.isPresent && task.getEndPoint.isPresent)
        throw new ConfigException(
          "You cannot use both \"endpoint\" option and \"end_point\" option. Use \"endpoint\" option."
        )
      if (task.getEndPoint.isPresent) {
        logger.warn(
          "[Deprecated] \"end_point\" option is deprecated. Use \"endpoint\" option instead."
        )
        return task.getEndPoint
      }
      task.getEndpoint
    }

    override def getRegion: Optional[String] = task.getRegion
  }

  def apply(task: Task): AwsEndpointConfiguration = {
    new AwsEndpointConfiguration(TaskCompat(task))
  }
}

class AwsEndpointConfiguration(task: Task) {

  def configureAwsClientBuilder[S <: AwsClientBuilder[S, T], T](
      builder: AwsClientBuilder[S, T]
  ): Unit = {
    if (task.getRegion.isPresent && task.getEndpoint.isPresent) {
      val ec =
        new EndpointConfiguration(task.getEndpoint.get, task.getRegion.get)
      builder.setEndpointConfiguration(ec)
    }
    else if (task.getRegion.isPresent && !task.getEndpoint.isPresent) {
      builder.setRegion(task.getRegion.get)
    }
    else if (!task.getRegion.isPresent && task.getEndpoint.isPresent) {
      val r: String = Try(new DefaultAwsRegionProviderChain().getRegion)
        .getOrElse(Regions.DEFAULT_REGION.getName)
      val e: String = task.getEndpoint.get
      val ec = new EndpointConfiguration(e, r)
      builder.setEndpointConfiguration(ec)
    }
  }

}
