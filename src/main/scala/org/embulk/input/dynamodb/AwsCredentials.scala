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
        new AWSStaticCredentialsProvider(
          new BasicAWSCredentials(
            task.getAccessKey.get(),
            task.getSecretKey.get()
          )
        )
      }
      else {
        new ProfileCredentialsProvider()
      }
    }
    else {
      task.getAuthMethod.get() match {
        case "basic" =>
          val accessKey = require(task.getAccessKey, "'access_key'")
          val secretKey = require(task.getSecretKey, "'secret_key'")

          new AWSStaticCredentialsProvider(
            new BasicAWSCredentials(accessKey, secretKey)
          )

        case "env" =>
          new EnvironmentVariableCredentialsProvider()

        case "instance" =>
          InstanceProfileCredentialsProvider.getInstance()

        case "profile" =>
          val profileName = task.getProfileName.or("default")

          try {
            new ProfileCredentialsProvider(profileName)
          }
          catch {
            case e: IllegalArgumentException =>
              throw new ConfigException(s"No AWS profile named $profileName")
          }

        case "properties" =>
          new SystemPropertiesCredentialsProvider()

        case _ =>
          throw new ConfigException(
            s"Unknown 'auth_method' ${task.getAuthMethod.get()}"
          )
      }
    }
  }
}
