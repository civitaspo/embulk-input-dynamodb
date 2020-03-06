package org.embulk.input.dynamodb

import com.amazonaws.services.dynamodbv2.model.{
  AttributeDefinition,
  AttributeValue,
  CreateTableRequest,
  KeySchemaElement,
  KeyType,
  ProvisionedThroughput,
  PutItemRequest,
  ScalarAttributeType
}
import org.embulk.config.ConfigSource
import org.embulk.input.dynamodb.testutil.EmbulkTestBase
import org.embulk.spi.util.Pages
import org.hamcrest.CoreMatchers._
import org.hamcrest.MatcherAssert.assertThat
import org.junit.Test
import org.msgpack.value.Value

import scala.jdk.CollectionConverters._

class DynamodbQueryOperationTest extends EmbulkTestBase {

  private def testBackwardCompatibility(embulkInConfig: ConfigSource): Unit = {
    cleanupTable("EMBULK_DYNAMODB_TEST_TABLE")
    withDynamodb { dynamodb =>
      dynamodb.createTable(
        new CreateTableRequest()
          .withTableName("EMBULK_DYNAMODB_TEST_TABLE")
          .withAttributeDefinitions(
            new AttributeDefinition()
              .withAttributeName("pri-key")
              .withAttributeType(ScalarAttributeType.S),
            new AttributeDefinition()
              .withAttributeName("sort-key")
              .withAttributeType(ScalarAttributeType.N)
          )
          .withKeySchema(
            new KeySchemaElement()
              .withAttributeName("pri-key")
              .withKeyType(KeyType.HASH),
            new KeySchemaElement()
              .withAttributeName("sort-key")
              .withKeyType(KeyType.RANGE)
          )
          .withProvisionedThroughput(
            new ProvisionedThroughput()
              .withReadCapacityUnits(5L)
              .withWriteCapacityUnits(5L)
          )
      )

      dynamodb.putItem(
        new PutItemRequest()
          .withTableName("EMBULK_DYNAMODB_TEST_TABLE")
          .withItem(
            Map
              .newBuilder[String, AttributeValue]
              .addOne("pri-key", new AttributeValue().withS("key-1"))
              .addOne("sort-key", new AttributeValue().withN("0"))
              .addOne("doubleValue", new AttributeValue().withN("42.195"))
              .addOne("boolValue", new AttributeValue().withBOOL(true))
              .addOne(
                "listValue",
                new AttributeValue().withL(
                  new AttributeValue().withS("list-value"),
                  new AttributeValue().withN("123")
                )
              )
              .addOne(
                "mapValue",
                new AttributeValue().withM(
                  Map
                    .newBuilder[String, AttributeValue]
                    .addOne(
                      "map-key-1",
                      new AttributeValue().withS("map-value-1")
                    )
                    .addOne("map-key-2", new AttributeValue().withN("456"))
                    .result()
                    .asJava
                )
              )
              .result()
              .asJava
          )
      )

      val path = embulk.createTempFile("csv")
      val result = embulk
        .inputBuilder()
        .in(embulkInConfig)
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
      assertThat(
        mapValue
          .entrySet()
          .asScala
          .filter(_.getKey.toString.equals("map-key-1"))
          .head
          .getValue
          .toString,
        is("map-value-1")
      )
      assert(mapValue.keySet().asScala.map(_.toString).contains("map-key-2"))
      assertThat(
        mapValue
          .entrySet()
          .asScala
          .filter(_.getKey.toString.equals("map-key-2"))
          .head
          .getValue
          .asIntegerValue()
          .asLong(),
        is(456L)
      )

    }
  }

  @Test
  def deprecatedQueryOperationTest(): Unit = {
    val inConfig: ConfigSource = embulk.configLoader().fromYamlString(s"""
         |type: dynamodb
         |end_point: http://${dynamoDBHost}:${dynamoDBPort}/
         |table: EMBULK_DYNAMODB_TEST_TABLE
         |auth_method: basic
         |access_key: dummy
         |secret_key: dummy
         |operation: query
         |filters:
         |  - {name: pri-key, type: string, condition: EQ, value: key-1}
         |columns:
         |  - {name: pri-key,     type: string}
         |  - {name: sort-key,    type: long}
         |  - {name: doubleValue, type: double}
         |  - {name: boolValue,   type: boolean}
         |  - {name: listValue,   type: json}
         |  - {name: mapValue,    type: json}
         |""".stripMargin)

    testBackwardCompatibility(inConfig)
  }

  @Test
  def keepTheSameBehaviourAsDeprecatedQueryOperationTest(): Unit = {
    val inConfig: ConfigSource = embulk.configLoader().fromYamlString(s"""
         |type: dynamodb
         |endpoint: http://${dynamoDBHost}:${dynamoDBPort}/
         |table: EMBULK_DYNAMODB_TEST_TABLE
         |auth_method: basic
         |access_key: dummy
         |secret_key: dummy
         |query:
         |  key_condition_expression: "#x = :v"
         |  expression_attribute_names:
         |    "#x": pri-key
         |  expression_attribute_values:
         |    ":v": {S: key-1}
         |columns:
         |  - {name: pri-key,     type: string}
         |  - {name: sort-key,    type: long}
         |  - {name: doubleValue, type: double}
         |  - {name: boolValue,   type: boolean}
         |  - {name: listValue,   type: json}
         |  - {name: mapValue,    type: json}
         |""".stripMargin)

    testBackwardCompatibility(inConfig)
  }
}
