package org.embulk.input.dynamodb


import java.io.File
import java.{util => JUtil}

import com.amazonaws.services.dynamodbv2.model.AttributeValue
import com.fasterxml.jackson.databind.ObjectMapper
import org.embulk.input.dynamodb.AttributeValueHelper._
import org.hamcrest.CoreMatchers._
import org.hamcrest.MatcherAssert.assertThat
import org.junit.Assert._
import org.junit.Test
import org.msgpack.value.ValueFactory

import scala.jdk.CollectionConverters._

class AttributeValueHelperTest {
  @Test
  def decodeTest(): Unit = {
    val stringValue = decodeToValue(new AttributeValue().withS("STR"))
    assertEquals(stringValue.asStringValue.asString, "STR")

    val intValue = decodeToValue(new AttributeValue().withN("123456789"))
    assertEquals(intValue.asNumberValue().toInt, 123456789)

    val doubleValue = decodeToValue(new AttributeValue().withN("-98765432.00000001"))
    assertEquals(doubleValue.asNumberValue().toDouble, -98765432.00000001, 0.0)

    val trueValue = decodeToValue(new AttributeValue().withBOOL(true))
    assertEquals(trueValue.asBooleanValue().getBoolean, true)

    val falseValue = decodeToValue(new AttributeValue().withBOOL(false))
    assertEquals(falseValue.asBooleanValue().getBoolean, false)

    val nilValue = decodeToValue(new AttributeValue().withNULL(true))
    assertEquals(nilValue.isNilValue, true)
  }


  @Test
  def listDecodeTest(): Unit = {
    val stringListValue = decodeToValue(new AttributeValue().withL(
      new AttributeValue().withS("ValueA"),
      new AttributeValue().withS("ValueB"),
      new AttributeValue().withS("ValueC")))

    assertTrue(stringListValue.isArrayValue)
    assertEquals(stringListValue.asArrayValue().size(), 3)

    assertTrue(stringListValue.asArrayValue().get(0).isStringValue)
    assertEquals(stringListValue.asArrayValue().get(0).asStringValue().asString(), "ValueA")
    assertEquals(stringListValue.asArrayValue().get(1).asStringValue().asString(), "ValueB")
    assertEquals(stringListValue.asArrayValue().get(2).asStringValue().asString(), "ValueC")


    val numberListValue = decodeToValue(new AttributeValue().withL(
      new AttributeValue().withN("123"),
      new AttributeValue().withN("-456"),
      new AttributeValue().withN("0.0000045679"),
      new AttributeValue().withN("-1234567890.123")))

    assertTrue(numberListValue.isArrayValue)
    assertEquals(numberListValue.asArrayValue().size(), 4)

    assertTrue(numberListValue.asArrayValue().get(0).isIntegerValue)
    assertEquals(numberListValue.asArrayValue().get(0).asNumberValue().toInt, 123)
    assertEquals(numberListValue.asArrayValue().get(1).asNumberValue().toInt, -456)

    assertTrue(numberListValue.asArrayValue().get(2).isFloatValue)
    assertEquals(numberListValue.asArrayValue().get(2).asNumberValue().toDouble, 0.0000045679, 0.0)
    assertEquals(numberListValue.asArrayValue().get(3).asNumberValue().toDouble, -1234567890.123, 0.0)


    val stringSetValue = decodeToValue(new AttributeValue().withSS(
      new JUtil.HashSet[String]() {
        add("ValueA")
        add("ValueB")
        add("ValueC")
      }))

    assertTrue(stringSetValue.isArrayValue)
    assertEquals(stringSetValue.asArrayValue().size(), 3)

    assertThat(List("ValueA", "ValueB", "ValueC").asJava,
      hasItems(
        equalTo(stringSetValue.asArrayValue().get(0).asStringValue().asString),
        equalTo(stringSetValue.asArrayValue().get(1).asStringValue().asString),
        equalTo(stringSetValue.asArrayValue().get(2).asStringValue().asString)))


    val numberSetValue = decodeToValue(new AttributeValue().withNS(
      new JUtil.HashSet[String]() {
        add("123")
        add("-456")
        add("0.0000045679")
        add("-1234567890.123")
      }))

    assertTrue(numberSetValue.isArrayValue)
    assertEquals(numberSetValue.asArrayValue().size(), 4)
  }


  @Test
  def mapDecodeTest(): Unit = {
    val stringMap = decodeToValue(new AttributeValue().withM(
      new JUtil.HashMap[String, AttributeValue]() {
        put("KeyA", new AttributeValue().withS("ValueA"))
        put("KeyB", new AttributeValue().withS("ValueB"))
        put("KeyC", new AttributeValue().withS("ValueC"))
      }))

    assertTrue(stringMap.isMapValue)
    assertEquals(stringMap.asMapValue().size(), 3)
    assertEquals(stringMap.asMapValue().map().get(ValueFactory.newString("KeyA")).asStringValue().asString(), "ValueA")
    assertEquals(stringMap.asMapValue().map().get(ValueFactory.newString("KeyB")).asStringValue().asString(), "ValueB")
    assertEquals(stringMap.asMapValue().map().get(ValueFactory.newString("KeyC")).asStringValue().asString(), "ValueC")


    val numberMap = decodeToValue(new AttributeValue().withM(
      new JUtil.HashMap[String, AttributeValue]() {
        put("KeyA", new AttributeValue().withN("123"))
        put("KeyB", new AttributeValue().withN("-456"))
        put("KeyC", new AttributeValue().withN("0.0000045679"))
        put("KeyD", new AttributeValue().withN("-1234567890.123"))
      }))

    assertTrue(numberMap.isMapValue)
    assertEquals(numberMap.asMapValue().size(), 4)

    assertTrue(numberMap.asMapValue().map().get(ValueFactory.newString("KeyA")).isIntegerValue)
    assertEquals(numberMap.asMapValue().map().get(ValueFactory.newString("KeyA")).asNumberValue().toInt, 123)
    assertEquals(numberMap.asMapValue().map().get(ValueFactory.newString("KeyB")).asNumberValue().toInt, -456)

    assertTrue(numberMap.asMapValue().map().get(ValueFactory.newString("KeyC")).isFloatValue)
    assertEquals(numberMap.asMapValue().map().get(ValueFactory.newString("KeyC")).asFloatValue().toDouble, 0.0000045679, 0.0)
    assertEquals(numberMap.asMapValue().map().get(ValueFactory.newString("KeyD")).asFloatValue().toDouble, -1234567890.123, 0.0)
  }

  def attr[A](value: A)(implicit f: A=> AttributeValue): AttributeValue = f(value)
  implicit def StringAttributeValue(value: String): AttributeValue
    = new AttributeValue().withS(value)
  implicit def IntegerAttributeValue(value: Int): AttributeValue
    = new AttributeValue().withN(value.toString)
  implicit def LongAttributeValue(value: Long): AttributeValue
    = new AttributeValue().withN(value.toString)
  implicit def FloatAttributeValue(value: Float): AttributeValue
    = new AttributeValue().withN(value.toString)
  implicit def DoubleAttributeValue(value: Double): AttributeValue
    = new AttributeValue().withN(value.toString)
  implicit def BooleanAttributeValue(value: Boolean): AttributeValue
    = new AttributeValue().withBOOL(value)
  implicit def MapAttributeValue(value: Map[String, AttributeValue]): AttributeValue
    = new AttributeValue().withM(value.asJava)
  implicit def ListAttributeValue(value: List[AttributeValue]): AttributeValue
    = new AttributeValue().withL(value.asJava)

  @Test
  def nestedDecodeTest(): Unit = {
    // TODO: Json -> AttributeValue...
    val testData = decodeToValue(attr(Map(
      "_id"        -> attr("56d8e1377a72374918f73bd2"),
      "index"      -> attr(0),
      "guid"       -> attr("5309640c-499a-43f6-801d-3076c810892b"),
      "isActive"   -> attr(true),
      "age"        -> attr(37),
      "name"       -> attr("Battle Lancaster"),
      "email"      -> attr("battlelancaster@zytrac.com"),
      "registered" -> attr("2014-07-16T04:40:58 -09:00"),
      "latitude"   -> attr(45.574906),
      "longitude"  -> attr(36.596302),
      "tags"       -> attr(List(
        attr("veniam"),
        attr("exercitation"),
        attr("velit"),
        attr("pariatur"),
        attr("sit"),
        attr("non"),
        attr("dolore"))),
      "friends" -> attr(List(
        attr(Map("id" -> attr(0), "name" -> attr("Mejia Montgomery"),
          "tags" -> attr(List(attr("duis"), attr("proident"), attr("et"))))),
        attr(Map("id" -> attr(1), "name" -> attr("Carpenter Reed"),
          "tags" -> attr(List(attr("labore"), attr("nisi"), attr("ipsum"))))),
        attr(Map("id" -> attr(2), "name" -> attr("Gamble Watts"),
          "tags" -> attr(List(attr("occaecat"), attr("voluptate"), attr("eu")))))
      ))
      )
    ))

    val testA = new ObjectMapper().readValue(
      testData.toJson, classOf[JUtil.Map[String, Any]])
    val testB = new ObjectMapper().readValue(
      new File("src/test/resources/json/test.json"), classOf[JUtil.Map[String, Any]])

    assertThat(testA, is(testB))
  }
}
