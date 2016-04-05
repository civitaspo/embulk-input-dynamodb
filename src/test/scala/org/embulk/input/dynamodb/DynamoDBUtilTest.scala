package org.embulk.input.dynamodb

import java.io.File
import java.nio.charset.Charset
import java.nio.file.{FileSystems, Files}

import com.google.inject.{Binder, Module}
import org.embulk.EmbulkEmbed
import org.embulk.config.ConfigSource
import org.embulk.plugin.InjectedPluginSource
import org.embulk.spi.InputPlugin
import org.junit.Assert._
import org.junit.{Before, Test}

class DynamoDBUtilTest {
  private var embulk: EmbulkEmbed = null

  private var EMBULK_DYNAMODB_TEST_TABLE: String = null

  @Before
  def createResources() {
    // Get Environments
    EMBULK_DYNAMODB_TEST_TABLE = System.getenv("EMBULK_DYNAMODB_TEST_TABLE")

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
  def scanTest() {
    val config = embulk.newConfigLoader().fromYamlFile(
      new File("src/test/resources/yaml/dynamodb-local.yml"))

    config.getNested("in")
      .set("table", EMBULK_DYNAMODB_TEST_TABLE)

    doTest(config)
  }
}
