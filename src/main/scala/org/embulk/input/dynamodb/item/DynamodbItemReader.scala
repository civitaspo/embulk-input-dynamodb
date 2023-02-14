package org.embulk.input.dynamodb.item

import org.embulk.spi.Column
import org.embulk.spi.time.Timestamp
import org.msgpack.value.Value
import java.time.Instant

case class DynamodbItemReader(
    private val schema: DynamodbItemSchema,
    private val ite: DynamodbItemIterator
) {
  private var currentItem: Map[String, DynamodbAttributeValue] = _

  def nextItem: Boolean = {
    ite.nextOption() match {
      case Some(v) =>
        currentItem = v
        true
      case None => false
    }
  }

  def getSchema: DynamodbItemSchema = schema

  def getTransformable(
      name: String,
      value: DynamodbAttributeValue
  ): DynamodbAttributeValueEmbulkTypeTransformable = {
    DynamodbAttributeValueEmbulkTypeTransformable(
      value,
      typeEnforcer = schema.getAttributeType(name),
      timestampFormatter = schema.getTimestampFormatter(name)
    )
  }

  def getBoolean(column: Column): Option[Boolean] =
    currentItem
      .get(column.getName)
      .flatMap(v => getTransformable(column.getName, v).asBoolean)

  def getString(column: Column): Option[String] =
    currentItem
      .get(column.getName)
      .flatMap(v => getTransformable(column.getName, v).asString)

  def getLong(column: Column): Option[Long] =
    currentItem
      .get(column.getName)
      .flatMap(v => getTransformable(column.getName, v).asLong)

  def getDouble(column: Column): Option[Double] =
    currentItem
      .get(column.getName)
      .flatMap(v => getTransformable(column.getName, v).asDouble)

  def getTimestamp(column: Column): Option[Instant] =
    currentItem
      .get(column.getName)
      .flatMap(v => getTransformable(column.getName, v).asTimestamp)

  def getJson(column: Column): Option[Value] =
    currentItem
      .get(column.getName)
      .flatMap(v => getTransformable(column.getName, v).asMessagePack)
}
