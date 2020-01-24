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
  private val EMBULK_DYNAMODB_TEST_REGION = getEnvironmentVariableOrShowErrorMessage("EMBULK_DYNAMODB_TEST_REGION")
  private val EMBULK_DYNAMODB_TEST_TABLE = getEnvironmentVariableOrShowErrorMessage("EMBULK_DYNAMODB_TEST_TABLE")
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
    val config = embulk.loadYamlResource("yaml/notSetAuthMethod.yml")

    config.getNested("in")
      .set("region", EMBULK_DYNAMODB_TEST_REGION)
      .set("table", EMBULK_DYNAMODB_TEST_TABLE)
      .set("access_key", EMBULK_DYNAMODB_TEST_ACCESS_KEY)
      .set("secret_key", EMBULK_DYNAMODB_TEST_SECRET_KEY)

    doTest(config.getNested("in"))
  }

  @Test
  def setAuthMethod_Basic(): Unit = {
    val config = embulk.loadYamlResource("yaml/authMethodBasic.yml")

    config.getNested("in")
      .set("region", EMBULK_DYNAMODB_TEST_REGION)
      .set("table", EMBULK_DYNAMODB_TEST_TABLE)
      .set("access_key", EMBULK_DYNAMODB_TEST_ACCESS_KEY)
      .set("secret_key", EMBULK_DYNAMODB_TEST_SECRET_KEY)

    doTest(config.getNested("in"))
  }

  @Test(expected = classOf[ConfigException])
  def setAuthMethod_Basic_NotSet(): Unit = {
    val config = embulk.loadYamlResource("yaml/authMethodBasic_Error.yml")

    config.getNested("in")
      .set("region", EMBULK_DYNAMODB_TEST_REGION)
      .set("table", EMBULK_DYNAMODB_TEST_TABLE)

    doTest(config.getNested("in"))
  }

  @Test
  def setAuthMethod_Env(): Unit = {
    val config = embulk.loadYamlResource("yaml/authMethodEnv.yml")

    config.getNested("in")
      .set("region", EMBULK_DYNAMODB_TEST_REGION)
      .set("table", EMBULK_DYNAMODB_TEST_TABLE)

    doTest(config.getNested("in"))
  }

  @Test
  def setAuthMethod_Profile(): Unit = {
    val config = embulk.loadYamlResource("yaml/authMethodProfile.yml")

    config.getNested("in")
      .set("region", EMBULK_DYNAMODB_TEST_REGION)
      .set("table", EMBULK_DYNAMODB_TEST_TABLE)
      .set("profile_name", EMBULK_DYNAMODB_TEST_PROFILE_NAME)

    doTest(config.getNested("in"))
  }

  @Test(expected = classOf[ConfigException])
  def setAuthMethod_Profile_NotExistProfileName(): Unit = {
    val config = embulk.loadYamlResource("yaml/authMethodProfile.yml")

    config.getNested("in")
      .set("region", EMBULK_DYNAMODB_TEST_REGION)
      .set("table", EMBULK_DYNAMODB_TEST_TABLE)
      .set("profile_name", "NotExistName")

    doTest(config.getNested("in"))
  }
}
