package org.embulk.input.dynamodb.aws

import java.util.Optional

import com.amazonaws.auth.{
  AnonymousAWSCredentials,
  AWSCredentialsProvider,
  AWSStaticCredentialsProvider,
  BasicAWSCredentials,
  BasicSessionCredentials,
  DefaultAWSCredentialsProviderChain,
  EC2ContainerCredentialsProviderWrapper,
  EnvironmentVariableCredentialsProvider,
  STSAssumeRoleSessionCredentialsProvider,
  SystemPropertiesCredentialsProvider,
  WebIdentityTokenCredentialsProvider
}
import com.amazonaws.auth.profile.{
  ProfileCredentialsProvider,
  ProfilesConfigFile
}
import org.embulk.config.{Config, ConfigDefault, ConfigException}
import org.embulk.input.dynamodb.aws.AwsCredentials.Task
import org.embulk.input.dynamodb.logger
import org.embulk.spi.unit.LocalFile
import zio.macros.annotation.delegate

object AwsCredentials {

  trait Task {

    @Config("auth_method")
    @ConfigDefault("\"default\"")
    def getAuthMethod: String

    @deprecated(message = "Use #getAccessKeyId() instead.", since = "0.3.0")
    @Config("access_key")
    @ConfigDefault("null")
    def getAccessKey: Optional[String]

    @Config("access_key_id")
    @ConfigDefault("null")
    def getAccessKeyId: Optional[String]

    @deprecated(message = "Use #getSecretAccessKey() instead.", since = "0.3.0")
    @Config("secret_key")
    @ConfigDefault("null")
    def getSecretKey: Optional[String]

    @Config("secret_access_key")
    @ConfigDefault("null")
    def getSecretAccessKey: Optional[String]

    @Config("session_token")
    @ConfigDefault("null")
    def getSessionToken: Optional[String]

    @Config("profile_file")
    @ConfigDefault("null")
    def getProfileFile: Optional[LocalFile]

    @Config("profile_name")
    @ConfigDefault("\"default\"")
    def getProfileName: String

    @Config("role_arn")
    @ConfigDefault("null")
    def getRoleArn: Optional[String]

    @Config("role_session_name")
    @ConfigDefault("null")
    def getRoleSessionName: Optional[String]

    @Config("role_external_id")
    @ConfigDefault("null")
    def getRoleExternalId: Optional[String]

    @Config("role_session_duration_seconds")
    @ConfigDefault("null")
    def getRoleSessionDurationSeconds: Optional[Int]

    @Config("scope_down_policy")
    @ConfigDefault("null")
    def getScopeDownPolicy: Optional[String]

    @Config("web_identity_token_file")
    @ConfigDefault("null")
    def getWebIdentityTokenFile: Optional[String]
  }

  def apply(task: Task): AwsCredentials = {
    new AwsCredentials(AwsCredentialsTaskCompat(task))
  }
}

case class AwsCredentialsTaskCompat(@delegate task: Task) extends Task {

  override def getAccessKey: Optional[String] = {
    throw new NotImplementedError()
  }

  override def getSecretKey: Optional[String] = {
    throw new NotImplementedError()
  }

  override def getAuthMethod: String = {
    if (getAccessKeyId.isPresent && getSecretAccessKey.isPresent) {
      if (task.getAuthMethod != "basic") {
        logger.warn(
          "[Deprecated] The default value of \"auth_method\" option is \"default\", " +
            "but currently use \"basic\" auth_method for backward compatibility " +
            "because you set \"access_key_id\" and \"secret_access_key\" options. " +
            "Please set \"basic\" to \"auth_method\" option expressly."
        )
        return "basic"
      }
    }
    task.getAuthMethod
  }

  override def getAccessKeyId: Optional[String] = {
    if (task.getAccessKeyId.isPresent && task.getAccessKey.isPresent)
      throw new ConfigException(
        "You cannot use both \"access_key_id\" option and \"access_key\" option. Use \"access_key_id\" option."
      )
    if (task.getAccessKey.isPresent) {
      logger.warn(
        "[Deprecated] \"access_key\" option is deprecated. Use \"access_key_id\" option instead."
      )
      return task.getAccessKey
    }
    task.getAccessKeyId
  }

  override def getSecretAccessKey: Optional[String] = {
    if (task.getSecretAccessKey.isPresent && task.getSecretKey.isPresent)
      throw new ConfigException(
        "You cannot use both \"secret_access_key\" option and \"secret_key\" option. Use \"secret_access_key\" option."
      )
    if (task.getSecretKey.isPresent) {
      logger.warn(
        "[Deprecated] \"secret_key\" option is deprecated. Use \"secret_access_key\" option instead."
      )
      return task.getSecretKey
    }
    task.getSecretAccessKey
  }
}

class AwsCredentials(task: Task) {

  def createAwsCredentialsProvider: AWSCredentialsProvider = {
    task.getAuthMethod match {
      case "basic" =>
        new AWSStaticCredentialsProvider(
          new BasicAWSCredentials(
            getRequiredOption(task.getAccessKeyId, "access_key_id"),
            getRequiredOption(task.getSecretAccessKey, "secret_access_key")
          )
        )

      case "env" =>
        new EnvironmentVariableCredentialsProvider

      case "instance" =>
        // NOTE: combination of InstanceProfileCredentialsProvider and ContainerCredentialsProvider
        new EC2ContainerCredentialsProviderWrapper

      case "profile" =>
        if (task.getProfileFile.isPresent) {
          val pf: ProfilesConfigFile = new ProfilesConfigFile(
            task.getProfileFile.get().getFile
          )
          new ProfileCredentialsProvider(pf, task.getProfileName)
        }
        else new ProfileCredentialsProvider(task.getProfileName)

      case "properties" =>
        new SystemPropertiesCredentialsProvider

      case "anonymous" =>
        new AWSStaticCredentialsProvider(new AnonymousAWSCredentials)

      case "session" =>
        new AWSStaticCredentialsProvider(
          new BasicSessionCredentials(
            getRequiredOption(task.getAccessKeyId, "access_key_id"),
            getRequiredOption(task.getSecretAccessKey, "secret_access_key"),
            getRequiredOption(task.getSessionToken, "session_token")
          )
        )

      case "assume_role" =>
        // NOTE: Are http_proxy, endpoint, region required when assuming role?
        val builder = new STSAssumeRoleSessionCredentialsProvider.Builder(
          getRequiredOption(task.getRoleArn, "role_arn"),
          getRequiredOption(task.getRoleSessionName, "role_session_name")
        )
        task.getRoleExternalId.ifPresent(v => builder.withExternalId(v))
        task.getRoleSessionDurationSeconds.ifPresent(v =>
          builder.withRoleSessionDurationSeconds(v)
        )
        task.getScopeDownPolicy.ifPresent(v => builder.withScopeDownPolicy(v))

        builder.build()

      case "web_identity_token" =>
        WebIdentityTokenCredentialsProvider
          .builder()
          .roleArn(getRequiredOption(task.getRoleArn, "role_arn"))
          .roleSessionName(
            getRequiredOption(task.getRoleSessionName, "role_session_name")
          )
          .webIdentityTokenFile(
            getRequiredOption(
              task.getWebIdentityTokenFile,
              "web_identity_token_file"
            )
          )
          .build()

      case "default" =>
        new DefaultAWSCredentialsProviderChain

      case am =>
        throw new ConfigException(
          s"'$am' is unsupported: `auth_method` must be one of ['basic', 'env', 'instance', 'profile', 'properties', 'anonymous', 'session', 'assume_role', 'default']."
        )
    }
  }

  private def getRequiredOption[A](o: Optional[A], name: String): A = {
    o.orElseThrow(() =>
      new ConfigException(
        s"`$name` must be set when `auth_method` is ${task.getAuthMethod}."
      )
    )
  }

}
