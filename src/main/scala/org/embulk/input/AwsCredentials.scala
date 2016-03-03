package org.embulk.input

import com.amazonaws.auth._
import com.amazonaws.auth.profile.ProfileCredentialsProvider
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
          new BasicAWSCredentials(
            task.getAccessKey.get(),
            task.getSecretKey.get())

        case "env" =>
          new EnvironmentVariableCredentialsProvider().getCredentials

        case "instance" =>
          new InstanceProfileCredentialsProvider().getCredentials

        case "profile" =>
          val profileName = task.getProfileName.or("default")
          new ProfileCredentialsProvider(profileName).getCredentials

        case "properties" =>
          new SystemPropertiesCredentialsProvider().getCredentials

        case _ =>
          throw new ConfigException(s"Unknown auth_method ${task.getAuthMethod.get()}")
      }

      new AWSCredentialsProvider {
        override def refresh(): Unit = {}

        override def getCredentials: AWSCredentials = { cred }
      }
    }
  }
}
