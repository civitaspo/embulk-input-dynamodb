package org.embulk.input.dynamodb.ope

import java.io.File
import java.nio.charset.Charset
import java.nio.file.{FileSystems, Files}

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.inject.{Binder, Module}
import org.embulk.EmbulkEmbed
import org.embulk.config.ConfigSource
import org.embulk.input.dynamodb.DynamodbInputPlugin
import org.embulk.plugin.InjectedPluginSource
import org.embulk.spi.InputPlugin
import org.hamcrest.CoreMatchers._
import org.junit.Assert._
import org.junit.{Before, Test}

class QueryOperationTest {
  private var embulk: EmbulkEmbed = null

  private var EMBULK_DYNAMODB_TEST_TABLE: String = null
  private var mapper: ObjectMapper = null

  @Before
  def createResources(): Unit = {
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

    mapper = new ObjectMapper()
  }


  def doTest(config: ConfigSource): Unit = {
    embulk.run(config)

    val fs = FileSystems.getDefault
    val lines = Files.readAllLines(fs.getPath("dynamodb-local-result000.00.tsv"), Charset.forName("UTF-8"))
    assertEquals(lines.size, 1)

    val head = lines.get(0)
    val values = head.split("\t")

    assertThat(values(0), is("key-1"))
    assertThat(values(1), is("0"))
    assertThat(values(2), is("42.195"))
    assertThat(values(3), is("true"))

    val listValue = mapper.readValue(values(4).replaceAll("\"(?!\")", ""), classOf[java.util.List[Object]])
    assertThat(listValue.size(), is(2))
    assertThat(listValue.get(0).asInstanceOf[String], is("list-value"))
    assertThat(listValue.get(1).asInstanceOf[Int], is(123))

    val mapValue = mapper.readValue(values(5).replaceAll("\"(?!\")", ""), classOf[java.util.Map[String, Object]])
    assert(mapValue.containsKey("map-key-1"))
    assertThat(mapValue.get("map-key-1").asInstanceOf[String], is("map-value-1"))
    assert(mapValue.containsKey("map-key-2"))
    assertThat(mapValue.get("map-key-2").asInstanceOf[Int], is(456))
  }

  @Test
  def queryTest(): Unit = {
    val config = embulk.newConfigLoader().fromYamlFile(
      new File("src/test/resources/yaml/dynamodb-local-query.yml"))

    config.getNested("in")
      .set("operation", "query")
      .set("table", EMBULK_DYNAMODB_TEST_TABLE)

    doTest(config)
  }
}
