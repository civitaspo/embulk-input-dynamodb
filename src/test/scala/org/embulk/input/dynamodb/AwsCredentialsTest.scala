package org.embulk.input.dynamodb

import java.io.File
import java.nio.charset.Charset
import java.nio.file.{FileSystems, Files}

import com.google.inject.{Binder, Module}
import org.embulk.config.ConfigSource
import org.embulk.exec.PartialExecutionException
import org.embulk.plugin.InjectedPluginSource
import org.embulk.spi.InputPlugin
import org.embulk.{EmbulkEmbed, EmbulkTestRuntime}
import org.junit.Assert.assertEquals
import org.junit.{Before, Rule, Test}

class AwsCredentialsTest {
  private var EMBULK_DYNAMODB_TEST_REGION: String = null
  private var EMBULK_DYNAMODB_TEST_TABLE: String = null
  private var EMBULK_DYNAMODB_TEST_ACCESS_KEY: String = null
  private var EMBULK_DYNAMODB_TEST_SECRET_KEY: String = null
  private var EMBULK_DYNAMODB_TEST_PROFILE_NAME: String = null

  private var embulk: EmbulkEmbed = null

  @Rule
  def runtime: EmbulkTestRuntime = new EmbulkTestRuntime

  @Before
  def createResources() {
    // Get Environments
    EMBULK_DYNAMODB_TEST_REGION = System.getenv("EMBULK_DYNAMODB_TEST_REGION")
    EMBULK_DYNAMODB_TEST_TABLE = System.getenv("EMBULK_DYNAMODB_TEST_TABLE")
    EMBULK_DYNAMODB_TEST_ACCESS_KEY = System.getenv("EMBULK_DYNAMODB_TEST_ACCESS_KEY")
    EMBULK_DYNAMODB_TEST_SECRET_KEY = System.getenv("EMBULK_DYNAMODB_TEST_SECRET_KEY")
    EMBULK_DYNAMODB_TEST_PROFILE_NAME = System.getenv("EMBULK_DYNAMODB_TEST_PROFILE_NAME")

    val bootstrap = new EmbulkEmbed.Bootstrap()
    bootstrap.addModules(new Module {
      def configure(binder: Binder): Unit = {
        InjectedPluginSource.registerPluginTo(binder,
          classOf[InputPlugin],
          "dynamodb",
          classOf[DynamodbInputPlugin])
      }
    })

    embulk = bootstrap.initializeCloseable()
  }

  def doTest(config: ConfigSource) {
    embulk.run(config)

    val fs = FileSystems.getDefault
    val lines = Files.readAllLines(fs.getPath("result000.00.tsv"), Charset.forName("UTF-8"))
    assertEquals("KEY-1\t1\tHogeHoge", lines.get(0))
  }

  @Test
  def notSetAuthMethod_SetCredentials() {
    val config = embulk.newConfigLoader().fromYamlFile(
      new File("src/test/resources/yaml/notSetAuthMethod.yml"))

    config.getNested("in")
      .set("region", EMBULK_DYNAMODB_TEST_REGION)
      .set("table", EMBULK_DYNAMODB_TEST_TABLE)
      .set("access_key", EMBULK_DYNAMODB_TEST_ACCESS_KEY)
      .set("secret_key", EMBULK_DYNAMODB_TEST_SECRET_KEY)

    doTest(config)
  }

  @Test
  def setAuthMethod_Basic() {
    val config = embulk.newConfigLoader().fromYamlFile(
      new File("src/test/resources/yaml/authMethodBasic.yml"))

    config.getNested("in")
      .set("region", EMBULK_DYNAMODB_TEST_REGION)
      .set("table", EMBULK_DYNAMODB_TEST_TABLE)
      .set("access_key", EMBULK_DYNAMODB_TEST_ACCESS_KEY)
      .set("secret_key", EMBULK_DYNAMODB_TEST_SECRET_KEY)

    doTest(config)
  }

  @Test(expected = classOf[PartialExecutionException])
  def setAuthMethod_Basic_NotSet() {
    val config = embulk.newConfigLoader().fromYamlFile(
      new File("src/test/resources/yaml/authMethodBasic_Error.yml"))

    config.getNested("in")
      .set("region", EMBULK_DYNAMODB_TEST_REGION)
      .set("table", EMBULK_DYNAMODB_TEST_TABLE)

    doTest(config)
  }

  @Test
  def setAuthMethod_Env() {
    val config = embulk.newConfigLoader().fromYamlFile(
      new File("src/test/resources/yaml/authMethodEnv.yml"))

    config.getNested("in")
      .set("region", EMBULK_DYNAMODB_TEST_REGION)
      .set("table", EMBULK_DYNAMODB_TEST_TABLE)

    doTest(config)
  }

  @Test
  def setAuthMethod_Profile() {
    val config = embulk.newConfigLoader().fromYamlFile(
      new File("src/test/resources/yaml/authMethodProfile.yml"))

    config.getNested("in")
      .set("region", EMBULK_DYNAMODB_TEST_REGION)
      .set("table", EMBULK_DYNAMODB_TEST_TABLE)
      .set("profile_name", EMBULK_DYNAMODB_TEST_PROFILE_NAME)

    doTest(config)
  }

  @Test(expected = classOf[PartialExecutionException])
  def setAuthMethod_Profile_NotExistProfileName() {
    val config = embulk.newConfigLoader().fromYamlFile(
      new File("src/test/resources/yaml/authMethodProfile.yml"))

    config.getNested("in")
      .set("region", EMBULK_DYNAMODB_TEST_REGION)
      .set("table", EMBULK_DYNAMODB_TEST_TABLE)
      .set("profile_name", "NotExistName")

    doTest(config)
  }
}
