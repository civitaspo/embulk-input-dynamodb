package org.embulk.input.dynamodb

import com.amazonaws.services.dynamodbv2.model.{
  AttributeDefinition,
  AttributeValue,
  BillingMode,
  CreateTableRequest,
  KeySchemaElement,
  KeyType,
  PutItemRequest,
  ScalarAttributeType
}
import org.embulk.config.ConfigSource
import org.embulk.input.dynamodb.testutil.EmbulkTestBase
import org.junit.Test

import scala.jdk.CollectionConverters._

class DynamodbOperationTest extends EmbulkTestBase {

  @Test
  def limitTest(): Unit = {
    val tableName = "limit_test"
    cleanupTable(tableName)
    withDynamodb { dynamodb =>
      dynamodb.createTable(
        new CreateTableRequest()
          .withTableName(tableName)
          .withAttributeDefinitions(
            new AttributeDefinition()
              .withAttributeName("pk")
              .withAttributeType(ScalarAttributeType.S)
          )
          .withKeySchema(
            new KeySchemaElement()
              .withAttributeName("pk")
              .withKeyType(KeyType.HASH)
          )
          .withBillingMode(BillingMode.PAY_PER_REQUEST)
      )
      dynamodb.putItem(
        new PutItemRequest()
          .withTableName(tableName)
          .withItem(
            Map
              .newBuilder[String, AttributeValue]
              .addOne("pk", new AttributeValue().withS("a"))
              .result()
              .asJava
          )
      )
      dynamodb.putItem(
        new PutItemRequest()
          .withTableName(tableName)
          .withItem(
            Map
              .newBuilder[String, AttributeValue]
              .addOne("pk", new AttributeValue().withS("a"))
              .result()
              .asJava
          )
      )
      dynamodb.putItem(
        new PutItemRequest()
          .withTableName(tableName)
          .withItem(
            Map
              .newBuilder[String, AttributeValue]
              .addOne("pk", new AttributeValue().withS("a"))
              .result()
              .asJava
          )
      )
      dynamodb.putItem(
        new PutItemRequest()
          .withTableName(tableName)
          .withItem(
            Map
              .newBuilder[String, AttributeValue]
              .addOne("pk", new AttributeValue().withS("b"))
              .result()
              .asJava
          )
      )
      dynamodb.putItem(
        new PutItemRequest()
          .withTableName(tableName)
          .withItem(
            Map
              .newBuilder[String, AttributeValue]
              .addOne("pk", new AttributeValue().withS("b"))
              .result()
              .asJava
          )
      )
    }

    val inScanConfig: ConfigSource = loadConfigSourceFromYamlString(s"""
           |type: dynamodb
           |table: $tableName
           |endpoint: http://$dynamoDBHost:$dynamoDBPort/
           |auth_method: basic
           |access_key_id: dummy
           |secret_access_key: dummy
           |scan:
           |  limit: 1
           |""".stripMargin)
    runInput(
      inScanConfig,
      { result =>
        assert(result.size.equals(1))
      }
    )

    val inQueryConfig: ConfigSource = loadConfigSourceFromYamlString(s"""
             |type: dynamodb
             |table: $tableName
             |endpoint: http://$dynamoDBHost:$dynamoDBPort/
             |auth_method: basic
             |access_key_id: dummy
             |secret_access_key: dummy
             |query:
             |  key_condition_expression: "#x = :v"
             |  expression_attribute_names:
             |    "#x": pk
             |  expression_attribute_values:
             |    ":v": {S: a}
             |  limit: 1
             |""".stripMargin)
    runInput(
      inQueryConfig,
      { result =>
        assert(result.size.equals(1))
      }
    )
  }
}
