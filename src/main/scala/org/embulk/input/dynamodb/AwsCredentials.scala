package org.embulk.input.dynamodb

import com.amazonaws.auth._
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.google.common.base.Optional
import org.embulk.config.ConfigException

object AwsCredentials {
  def getCredentialsProvider(task: PluginTask): AWSCredentialsProvider = {
    if (!task.getAuthMethod.isPresent) {
      // backward compatibility
      if (task.getAccessKey.isPresent && task.getSecretKey.isPresent) {
        new AWSCredentialsProvider {
          override def refresh(): Unit = {}

          override def getCredentials: AWSCredentials = {
            new BasicAWSCredentials(
              task.getAccessKey.get(),
              task.getSecretKey.get())
          }
        }
      } else {
        new ProfileCredentialsProvider()
      }
    } else {
      val cred = task.getAuthMethod.get() match {
        case "basic" =>
          val accessKey = require(task.getAccessKey, "'access_key'")
          val secretKey = require(task.getSecretKey, "'secret_key'")

          new BasicAWSCredentials(accessKey, secretKey)

        case "env" =>
          new EnvironmentVariableCredentialsProvider().getCredentials

        case "instance" =>
          new InstanceProfileCredentialsProvider().getCredentials

        case "profile" =>
          val profileName = task.getProfileName.or("default")

          try {
            new ProfileCredentialsProvider(profileName).getCredentials
          } catch {
            case e: IllegalArgumentException =>
              throw new ConfigException(s"No AWS profile named $profileName")
          }

        case "properties" =>
          new SystemPropertiesCredentialsProvider().getCredentials

        case _ =>
          throw new ConfigException(s"Unknown 'auth_method' ${task.getAuthMethod.get()}")
      }

      new AWSCredentialsProvider {
        override def refresh(): Unit = {}

        override def getCredentials: AWSCredentials = { cred }
      }
    }
  }

  private def require[A](value: Optional[A], message: String): A = {
    if (value.isPresent) {
      value.get()
    } else {
      throw new ConfigException("Required option is not set: " + message)
    }
  }
}
