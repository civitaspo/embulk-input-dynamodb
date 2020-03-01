package org.embulk.input.dynamodb.item

import org.embulk.spi.Column
import org.embulk.spi.time.Timestamp
import org.msgpack.value.Value

case class DynamodbItemReader(
    schema: DynamodbItemSchema,
    ite: DynamodbItemIterator,
    currentItem: Map[String, DynamodbAttributeValue] = _
) {

  val converter: DynamodbAttributeValueEmbulkConverter =
    DynamodbAttributeValueEmbulkConverter(schema)

  def nextItem: Option[DynamodbItemReader] =
    ite.nextOption().map(i => copy(currentItem = i))

  def getSchema: DynamodbItemSchema = schema

  def getBoolean(column: Column): Option[Boolean] =
    currentItem
      .get(column.getName)
      .flatMap(v => converter.withAttribute(column.getName, v).asBoolean)

  def getString(column: Column): Option[String] =
    currentItem
      .get(column.getName)
      .flatMap(v => converter.withAttribute(column.getName, v).asString)

  def getLong(column: Column): Option[Long] =
    currentItem
      .get(column.getName)
      .flatMap(v => converter.withAttribute(column.getName, v).asLong)

  def getDouble(column: Column): Option[Double] =
    currentItem
      .get(column.getName)
      .flatMap(v => converter.withAttribute(column.getName, v).asDouble)

  def getTimestamp(column: Column): Option[Timestamp] =
    currentItem
      .get(column.getName)
      .flatMap(v => converter.withAttribute(column.getName, v).asTimestamp)

  def getJson(column: Column): Option[Value] =
    currentItem
      .get(column.getName)
      .flatMap(v => converter.withAttribute(column.getName, v).asMessagePack)
}
