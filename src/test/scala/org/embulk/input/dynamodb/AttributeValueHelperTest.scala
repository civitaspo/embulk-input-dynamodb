package org.embulk.input.dynamodb


import java.{util => JUtil}

import com.amazonaws.services.dynamodbv2.model.AttributeValue
import com.amazonaws.util.json.JSONObject
import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.Test
import org.junit.Assert._
import org.hamcrest.CoreMatchers._
import AttributeValueHelper._
import org.msgpack.value.ValueFactory

import scala.collection.JavaConverters._

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

  @Test
  def nestedDecodeTest(): Unit = {
    val nestedList = decodeToValue(new AttributeValue().withL(
      new JUtil.ArrayList[AttributeValue]() {
        // Depth:0
        add(new AttributeValue().withS("ValueA"))
        add(new AttributeValue().withN("123"))
        add(new AttributeValue().withL(
          new JUtil.ArrayList[AttributeValue]() {
            // Depth:1
            add(new AttributeValue().withS("ValueB"))
            add(new AttributeValue().withL(
              new JUtil.ArrayList[AttributeValue]() {
                // Depth:2A
                add(new AttributeValue().withS("ValueC"))
              }))
            add(new AttributeValue().withM(
              new JUtil.TreeMap[String, AttributeValue]() {
                // Depth:2B
                put("KeyA", new AttributeValue().withS("ValueD"))
                put("KeyB", new AttributeValue().withS("ValueE"))
              }
            ))
          }))
        // Depth:0
        add(new AttributeValue().withM(
          new JUtil.TreeMap[String, AttributeValue]() {
            // Depth:1
            put("KeyC", new AttributeValue().withBOOL(true))
            put("KeyD", new AttributeValue().withM(
              new JUtil.TreeMap[String, AttributeValue]() {
                // Depth:2
                put("KeyE", new AttributeValue().withM(
                  new JUtil.TreeMap[String, AttributeValue] {
                    // Depth:3A
                    put("KeyF", new AttributeValue().withN("456"))
                    put("KeyG", new AttributeValue().withN("0.000456"))
                  }))
                put("KeyH", new AttributeValue().withM( {
                  new JUtil.TreeMap[String, AttributeValue]() {
                    // Depth:3B
                    put("KeyI", new AttributeValue().withN("789"))
                    put("KeyJ", new AttributeValue().withN("-0.000789"))
                  }
                }))
                put("KeyK", new AttributeValue().withL(
                  new JUtil.ArrayList[AttributeValue]() {
                    // Depth:3C
                    add(new AttributeValue().withS("ValueF"))
                    add(new AttributeValue().withS("ValueG"))
                  }))
                put("KeyL", new AttributeValue().withSS(
                  new JUtil.TreeSet[String]() {
                    // Depth:3D
                    add("ValueH")
                    add("ValueI")
                  }))
                put("KeyM", new AttributeValue().withNS(
                  new JUtil.TreeSet[String]() {
                    // Depth:3E
                    add("123.0")
                    add("-456.789")
                  }))
              }))
          }))
      }))

    assertThat(nestedList.asArrayValue().get(0).asStringValue().asString(), is("ValueA"))
    assertThat(nestedList.asArrayValue().get(1).asNumberValue().toInt, is(123))

    val item = new JUtil.ArrayList[Any]() {
      add("ValueA")
      add(123)
      add(new JUtil.ArrayList[Any]() {
        add("ValueB")
        add(new JUtil.ArrayList[Any]() {
          add("ValueC")
        })
        add(new JUtil.TreeMap[String, Any]() {
          put("KeyA", "ValueD")
          put("KeyB", "ValueE")
        })
      })
      add(new JUtil.TreeMap[String, Any](){
        put("KeyC", true)
        put("KeyD", new JUtil.TreeMap[String, Any](){
          put("KeyE", new JUtil.TreeMap[String, Any](){
            put("KeyF", 456)
            put("KeyG", 0.000456)
          })
          put("KeyH", new JUtil.TreeMap[String, Any](){
            put("KeyF", 789)
            put("KeyG", -0.000789)
          })
          put("KeyK", new JUtil.ArrayList[Any](){
            add("ValueF")
            add("ValueG")
          })
          put("KeyL", new JUtil.TreeSet[Any](){
            add("ValueH")
            add("ValueI")
          })
          put("KeyM", new JUtil.TreeSet[Any](){
            add(-456.789)
            add(123.0)
          })
        })
      })
    }
    // compare
  }
}
