package org.embulk.input.dynamodb

import java.nio.charset.Charset
import java.nio.file.Files

import org.embulk.config.{ConfigException, ConfigSource}
import org.embulk.exec.PartialExecutionException
import org.embulk.input.dynamodb.testutil.EmbulkTestBase
import org.hamcrest.CoreMatchers._
import org.hamcrest.MatcherAssert.assertThat
import org.junit.Assert.assertEquals
import org.junit.Test

class AwsCredentialsTest extends EmbulkTestBase {
  private val EMBULK_DYNAMODB_TEST_ACCESS_KEY = getEnvironmentVariableOrShowErrorMessage("EMBULK_DYNAMODB_TEST_ACCESS_KEY")
  private val EMBULK_DYNAMODB_TEST_SECRET_KEY = getEnvironmentVariableOrShowErrorMessage("EMBULK_DYNAMODB_TEST_SECRET_KEY")
  private val EMBULK_DYNAMODB_TEST_PROFILE_NAME = getEnvironmentVariableOrShowErrorMessage("EMBULK_DYNAMODB_TEST_PROFILE_NAME")

  def doTest(inConfig: ConfigSource): Unit = {
    val task: PluginTask = inConfig.loadConfig(classOf[PluginTask])
    val provider = AwsCredentials.getCredentialsProvider(task)
    val cred = provider.getCredentials
    assertThat(cred.getAWSAccessKeyId, notNullValue())
    assertThat(cred.getAWSSecretKey, notNullValue())
  }

  @Test
  def notSetAuthMethod_SetCredentials(): Unit = {
    val inConfig: ConfigSource = embulk.configLoader().fromYamlString(
      s"""
        |type: dynamodb
        |region: us-east-1
        |table: hoge
        |operation: scan
        |access_key: $EMBULK_DYNAMODB_TEST_ACCESS_KEY
        |secret_key: $EMBULK_DYNAMODB_TEST_SECRET_KEY
        |columns:
        |  - {name: key1,   type: string}
        |  - {name: key2,   type: long}
        |  - {name: value1, type: string}
        |""".stripMargin)

    doTest(inConfig)
  }

  @Test
  def setAuthMethod_Basic(): Unit = {
    val inConfig: ConfigSource = embulk.configLoader().fromYamlString(
      s"""
         |type: dynamodb
         |region: us-east-1
         |table: hoge
         |operation: scan
         |auth_method: basic
         |access_key: $EMBULK_DYNAMODB_TEST_ACCESS_KEY
         |secret_key: $EMBULK_DYNAMODB_TEST_SECRET_KEY
         |columns:
         |  - {name: key1,   type: string}
         |  - {name: key2,   type: long}
         |  - {name: value1, type: string}
         |""".stripMargin)

    doTest(inConfig)
  }

  @Test(expected = classOf[ConfigException])
  def setAuthMethod_Basic_NotSet(): Unit = {
    val inConfig: ConfigSource = embulk.configLoader().fromYamlString(
      s"""
         |type: dynamodb
         |region: us-east-1
         |table: hoge
         |operation: scan
         |auth_method: basic
         |columns:
         |  - {name: key1,   type: string}
         |  - {name: key2,   type: long}
         |  - {name: value1, type: string}
         |""".stripMargin)

    doTest(inConfig)
  }

  @Test
  def setAuthMethod_Env(): Unit = {
    // NOTE: Requires to set the env vars like 'AWS_ACCESS_KEY_ID' and so on when testing.
    val inConfig: ConfigSource = embulk.configLoader().fromYamlString(
      s"""
         |type: dynamodb
         |region: us-east-1
         |table: hoge
         |operation: scan
         |auth_method: env
         |columns:
         |  - {name: key1,   type: string}
         |  - {name: key2,   type: long}
         |  - {name: value1, type: string}
         |""".stripMargin)

    doTest(inConfig)
  }

  @Test
  def setAuthMethod_Profile(): Unit = {
    // NOTE: Requires to set credentials to '~/.aws' when testing.
    val inConfig: ConfigSource = embulk.configLoader().fromYamlString(
      s"""
         |type: dynamodb
         |region: us-east-1
         |table: hoge
         |operation: scan
         |auth_method: profile
         |profile_name: $EMBULK_DYNAMODB_TEST_PROFILE_NAME
         |columns:
         |  - {name: key1,   type: string}
         |  - {name: key2,   type: long}
         |  - {name: value1, type: string}
         |""".stripMargin)

    doTest(inConfig)
  }

  @Test(expected = classOf[ConfigException])
  def setAuthMethod_Profile_NotExistProfileName(): Unit = {
    val inConfig: ConfigSource = embulk.configLoader().fromYamlString(
      s"""
         |type: dynamodb
         |region: us-east-1
         |table: hoge
         |operation: scan
         |auth_method: profile
         |profile_name: DO_NOT_EXIST
         |columns:
         |  - {name: key1,   type: string}
         |  - {name: key2,   type: long}
         |  - {name: value1, type: string}
         |""".stripMargin)

    doTest(inConfig)
  }
}
