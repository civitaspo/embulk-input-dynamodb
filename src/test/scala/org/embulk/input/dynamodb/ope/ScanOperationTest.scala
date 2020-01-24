package org.embulk.input.dynamodb.ope

import org.embulk.config.ConfigSource
import org.embulk.input.dynamodb.testutil.EmbulkTestBase
import org.embulk.spi.util.Pages
import org.hamcrest.CoreMatchers._
import org.hamcrest.MatcherAssert.assertThat
import org.junit.Test
import org.msgpack.value.Value

import scala.jdk.CollectionConverters._

class ScanOperationTest  extends EmbulkTestBase {
  private val EMBULK_DYNAMODB_TEST_TABLE: String = getEnvironmentVariableOrShowErrorMessage("EMBULK_DYNAMODB_TEST_TABLE")

  def doTest(inConfig: ConfigSource): Unit = {
    val path = embulk.createTempFile("csv")
    val result = embulk
        .inputBuilder()
        .in(inConfig)
        .outputPath(path)
        .preview()

    val pages = result.getPages
    val head = Pages.toObjects(result.getSchema, pages.get(0)).get(0)

    assertThat(head(0).toString, is("key-1"))
    assertThat(head(1).asInstanceOf[Long], is(0L))
    assertThat(head(2).asInstanceOf[Double], is(42.195))
    assertThat(head(3).asInstanceOf[Boolean], is(true))

    val arrayValue = head(4).asInstanceOf[Value].asArrayValue()
    assertThat(arrayValue.size(), is(2))
    assertThat(arrayValue.get(0).asStringValue().toString, is("list-value"))
    assertThat(arrayValue.get(1).asIntegerValue().asLong(), is(123L))

    val mapValue = head(5).asInstanceOf[Value].asMapValue()
    assert(mapValue.keySet().asScala.map(_.toString).contains("map-key-1"))
    assertThat(mapValue.entrySet().asScala.filter(_.getKey.toString.equals("map-key-1")).head.getValue.toString, is("map-value-1"))
    assert(mapValue.keySet().asScala.map(_.toString).contains("map-key-2"))
    assertThat(mapValue.entrySet().asScala.filter(_.getKey.toString.equals("map-key-2")).head.getValue.asIntegerValue().asLong(), is(456L))
  }

  @Test
  def scanTest(): Unit = {
    val config = embulk.loadYamlResource("yaml/dynamodb-local-scan.yml")

    config.getNested("in")
        .set("operation", "scan")
        .set("table", EMBULK_DYNAMODB_TEST_TABLE)

    doTest(config.getNested("in"))
  }
}
