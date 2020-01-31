package org.embulk.input.dynamodb

import com.amazonaws.services.securitytoken.model.AWSSecurityTokenServiceException
import org.embulk.config.{ConfigException, ConfigSource}
import org.embulk.input.dynamodb.aws.AwsCredentials
import org.embulk.input.dynamodb.testutil.EmbulkTestBase
import org.hamcrest.CoreMatchers._
import org.hamcrest.MatcherAssert.assertThat
import org.junit.{Assume, Test}

class AwsCredentialsTest extends EmbulkTestBase {

  private val runAwsCredentialsTest: Boolean = Option(
    System.getenv("RUN_AWS_CREDENTIALS_TEST")
  ) match {
    case Some(x) =>
      if (x == "false") false
      else true
    case None => true
  }

  private lazy val EMBULK_DYNAMODB_TEST_ACCESS_KEY =
    getEnvironmentVariableOrShowErrorMessage("EMBULK_DYNAMODB_TEST_ACCESS_KEY")

  private lazy val EMBULK_DYNAMODB_TEST_SECRET_KEY =
    getEnvironmentVariableOrShowErrorMessage("EMBULK_DYNAMODB_TEST_SECRET_KEY")

  private lazy val EMBULK_DYNAMODB_TEST_PROFILE_NAME =
    getEnvironmentVariableOrShowErrorMessage(
      "EMBULK_DYNAMODB_TEST_PROFILE_NAME"
    )

  private lazy val EMBULK_DYNAMODB_TEST_ASSUME_ROLE_ROLE_ARN =
    getEnvironmentVariableOrShowErrorMessage(
      "EMBULK_DYNAMODB_TEST_ASSUME_ROLE_ROLE_ARN"
    )

  def doTest(inConfig: ConfigSource): Unit = {
    val task: PluginTask = inConfig.loadConfig(classOf[PluginTask])
    val provider = AwsCredentials(task).createAwsCredentialsProvider
    val cred = provider.getCredentials
    assertThat(cred.getAWSAccessKeyId, notNullValue())
    assertThat(cred.getAWSSecretKey, notNullValue())
  }

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

  @deprecated(since = "0.3.0")
  @Test
  def notSetAuthMethod_SetCredentials_deprecated(): Unit = {
    Assume.assumeTrue(runAwsCredentialsTest)
    val inConfig: ConfigSource = defaultInConfig
      .set("access_key", EMBULK_DYNAMODB_TEST_ACCESS_KEY)
      .set("secret_key", EMBULK_DYNAMODB_TEST_SECRET_KEY)

    doTest(inConfig)
  }

  @Test
  def notSetAuthMethod_SetCredentials(): Unit = {
    Assume.assumeTrue(runAwsCredentialsTest)
    val inConfig: ConfigSource = defaultInConfig
      .set("access_key_id", EMBULK_DYNAMODB_TEST_ACCESS_KEY)
      .set("secret_access_key", EMBULK_DYNAMODB_TEST_SECRET_KEY)

    doTest(inConfig)
  }

  @deprecated(since = "0.3.0")
  @Test
  def setAuthMethod_Basic_deprecated(): Unit = {
    Assume.assumeTrue(runAwsCredentialsTest)
    val inConfig: ConfigSource = defaultInConfig
      .set("auth_method", "basic")
      .set("access_key", EMBULK_DYNAMODB_TEST_ACCESS_KEY)
      .set("secret_key", EMBULK_DYNAMODB_TEST_SECRET_KEY)

    doTest(inConfig)
  }

  @Test
  def setAuthMethod_Basic(): Unit = {
    Assume.assumeTrue(runAwsCredentialsTest)
    val inConfig: ConfigSource = defaultInConfig
      .set("auth_method", "basic")
      .set("access_key_id", EMBULK_DYNAMODB_TEST_ACCESS_KEY)
      .set("secret_access_key", EMBULK_DYNAMODB_TEST_SECRET_KEY)

    doTest(inConfig)
  }

  @deprecated(since = "0.3.0")
  @Test(expected = classOf[ConfigException])
  def throwIfSetAccessKeyAndAccessKeyId(): Unit = {
    Assume.assumeTrue(runAwsCredentialsTest)
    val inConfig: ConfigSource = defaultInConfig
      .set("auth_method", "basic")
      .set("access_key", EMBULK_DYNAMODB_TEST_ACCESS_KEY)
      .set("access_key_id", EMBULK_DYNAMODB_TEST_ACCESS_KEY)
      .set("secret_key", EMBULK_DYNAMODB_TEST_SECRET_KEY)

    doTest(inConfig)
  }

  @deprecated(since = "0.3.0")
  @Test(expected = classOf[ConfigException])
  def throwIfSetSecretKeyAndSecretAccessKeyId(): Unit = {
    Assume.assumeTrue(runAwsCredentialsTest)
    val inConfig: ConfigSource = defaultInConfig
      .set("auth_method", "basic")
      .set("access_key", EMBULK_DYNAMODB_TEST_ACCESS_KEY)
      .set("secret_key", EMBULK_DYNAMODB_TEST_SECRET_KEY)
      .set("secret_access_key", EMBULK_DYNAMODB_TEST_SECRET_KEY)

    doTest(inConfig)
  }

  @Test(expected = classOf[ConfigException])
  def setAuthMethod_Basic_NotSet(): Unit = {
    val inConfig: ConfigSource = defaultInConfig
      .set("auth_method", "basic")

    doTest(inConfig)
  }

  @Test
  def setAuthMethod_Env(): Unit = {
    Assume.assumeTrue(runAwsCredentialsTest)
    // NOTE: Requires to set the env vars like 'AWS_ACCESS_KEY_ID' and so on when testing.
    val inConfig: ConfigSource = defaultInConfig
      .set("auth_method", "env")

    doTest(inConfig)
  }

  @Test
  def setAuthMethod_Profile(): Unit = {
    Assume.assumeTrue(runAwsCredentialsTest)
    // NOTE: Requires to set credentials to '~/.aws' when testing.
    val inConfig: ConfigSource = defaultInConfig
      .set("auth_method", "profile")
      .set("profile_name", EMBULK_DYNAMODB_TEST_PROFILE_NAME)

    doTest(inConfig)
  }

  @Test(expected = classOf[IllegalArgumentException])
  def setAuthMethod_Profile_NotExistProfileName(): Unit = {
    val inConfig: ConfigSource = defaultInConfig
      .set("auth_method", "profile")
      .set("profile_name", "DO_NOT_EXIST")

    doTest(inConfig)
  }

  @Test
  def setAuthMethod_assume_role(): Unit = {
    Assume.assumeTrue(runAwsCredentialsTest)
    val inConfig: ConfigSource = defaultInConfig
      .set("auth_method", "assume_role")
      .set("role_arn", EMBULK_DYNAMODB_TEST_ASSUME_ROLE_ROLE_ARN)
      .set("role_session_name", "dummy")

    doTest(inConfig)
  }

  @Test(expected = classOf[AWSSecurityTokenServiceException])
  def setAuthMethod_assume_role_NotExistRoleArn(): Unit = {
    Assume.assumeTrue(runAwsCredentialsTest)
    val inConfig: ConfigSource = defaultInConfig
      .set("auth_method", "assume_role")
      .set("role_arn", "DO_NOT_EXIST")
      .set("role_session_name", "dummy")

    doTest(inConfig)
  }
}
