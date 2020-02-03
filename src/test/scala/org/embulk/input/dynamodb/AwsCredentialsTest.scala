package org.embulk.input.dynamodb

import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.auth.{
  AWSStaticCredentialsProvider,
  EnvironmentVariableCredentialsProvider
}
import org.embulk.config.{ConfigException, ConfigSource}
import org.embulk.input.dynamodb.testutil.EmbulkTestBase
import org.hamcrest.CoreMatchers._
import org.hamcrest.MatcherAssert.assertThat
import org.junit.{Assume, Test}

class AwsCredentialsTest extends EmbulkTestBase {

  def defaultInConfig: ConfigSource = {
    embulk.configLoader().fromYamlString(s"""
         |type: dynamodb
         |region: us-east-1
         |table: hoge
         |operation: scan
         |columns:
         |  - {name: key1,   type: string}
         |  - {name: key2,   type: long}
         |  - {name: value1, type: string}
         |""".stripMargin)
  }

  @Test
  def notSetAuthMethod_SetCredentials(): Unit = {
    val inConfig: ConfigSource = defaultInConfig
      .set("access_key", "access_key")
      .set("secret_key", "secret_key")

    val task: PluginTask = inConfig.loadConfig(classOf[PluginTask])

    val provider = AwsCredentials.getCredentialsProvider(task)
    assert(provider.isInstanceOf[AWSStaticCredentialsProvider])

    val credentials = provider.getCredentials
    assertThat(credentials.getAWSAccessKeyId, is("access_key"))
    assertThat(credentials.getAWSSecretKey, is("secret_key"))
  }

  @Test
  def setAuthMethod_Basic(): Unit = {
    val inConfig: ConfigSource = defaultInConfig
      .set("auth_method", "basic")
      .set("access_key", "access_key")
      .set("secret_key", "secret_key")

    val task: PluginTask = inConfig.loadConfig(classOf[PluginTask])

    val provider = AwsCredentials.getCredentialsProvider(task)
    assert(provider.isInstanceOf[AWSStaticCredentialsProvider])

    val credentials = provider.getCredentials
    assertThat(credentials.getAWSAccessKeyId, is("access_key"))
    assertThat(credentials.getAWSSecretKey, is("secret_key"))
  }

  @Test(expected = classOf[ConfigException])
  def setAuthMethod_Basic_NotSet(): Unit = {
    val inConfig: ConfigSource = defaultInConfig
      .set("auth_method", "basic")

    val task: PluginTask = inConfig.loadConfig(classOf[PluginTask])
    // throws ConfigException
    AwsCredentials.getCredentialsProvider(task)
  }

  @Test
  def setAuthMethod_Env(): Unit = {
    // NOTE: Requires to set the env vars like 'AWS_ACCESS_KEY_ID' and so on when testing.
    val inConfig: ConfigSource = defaultInConfig
      .set("auth_method", "env")

    val task: PluginTask = inConfig.loadConfig(classOf[PluginTask])

    val provider = AwsCredentials.getCredentialsProvider(task)
    assert(provider.isInstanceOf[EnvironmentVariableCredentialsProvider])
  }

  @Test
  def setAuthMethod_Profile(): Unit = {
    // NOTE: Requires to set credentials to '~/.aws' when testing.
    val inConfig: ConfigSource = defaultInConfig
      .set("auth_method", "profile")
      .set("profile_name", "default")

    val task: PluginTask = inConfig.loadConfig(classOf[PluginTask])

    val provider = AwsCredentials.getCredentialsProvider(task)
    assert(provider.isInstanceOf[ProfileCredentialsProvider])
  }
}
