package org.embulk.input.dynamodb

import com.amazonaws.services.dynamodbv2.model._
import org.embulk.config.ConfigSource
import org.embulk.input.dynamodb.testutil.{
  AmazonDynamoDBClient,
  DynamoDBTestBase,
  EmbulkTestBase
}
import org.embulk.spi.util.Pages
import org.hamcrest.CoreMatchers._
import org.hamcrest.MatcherAssert.assertThat
import org.junit.{Assume, Test}
import org.msgpack.value.Value

import scala.jdk.CollectionConverters._

class IntegrationTest
    extends EmbulkTestBase
    with DynamoDBTestBase
    with AmazonDynamoDBClient {

  implicit val accessKey: String =
    getEnvironmentVariableOrShowErrorMessage("EMBULK_DYNAMODB_TEST_ACCESS_KEY")

  implicit val secretKey: String =
    getEnvironmentVariableOrShowErrorMessage("EMBULK_DYNAMODB_TEST_SECRET_KEY")

  implicit val region: String =
    getEnvironmentVariable("EMBULK_DYNAMODB_TEST_REGION", "ap-northeast-1")

  implicit val tableName: String =
    getEnvironmentVariable(
      "EMBULK_DYNAMODB_TEST_TABLE_NAME",
      "EMBULK_DYNAMODB_TEST_TABLE"
    )

  private val runAwsCredentialsTest: Boolean = Option(
    System.getenv("RUN_AWS_CREDENTIALS_TEST")
  ) match {
    case Some(x) =>
      if (x == "false") false
      else true
    case None => true
  }

  @Test
  def queryTest(): Unit = {
    Assume.assumeTrue(runAwsCredentialsTest)

    cleanupTable(tableName)

    prepareTable(
      new CreateTableRequest()
        .withTableName(tableName)
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
            .withReadCapacityUnits(1L)
            .withWriteCapacityUnits(1L)
        )
    )

    withDynamoDB { dynamodb =>
      dynamodb.putItem(
        new PutItemRequest()
          .withTableName(tableName)
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
    }

    val inConfig: ConfigSource = embulk.configLoader().fromYamlString(s"""
         |type: dynamodb
         |table: $tableName
         |region: $region
         |auth_method: basic
         |access_key: $accessKey
         |secret_key: $secretKey
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

    cleanupTable(tableName)
  }
}
