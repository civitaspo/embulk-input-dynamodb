package org.embulk.input.dynamodb.item

import java.util.{Optional, List => JList}

import com.amazonaws.services.dynamodbv2.model.AttributeValue
import com.fasterxml.jackson.annotation.{JsonCreator, JsonValue}
import org.embulk.util.config.{Config, ConfigDefault, Task => EmbulkTask}
import org.embulk.spi.{Column, PageBuilder, Schema}
import org.embulk.spi.`type`.{Type, Types}
import org.embulk.util.timestamp.TimestampFormatter

import scala.jdk.CollectionConverters._
import scala.util.chaining._
import scala.util.Try

object DynamodbItemSchema {

  trait ColumnTask extends EmbulkTask {

    @Config("name")
    def getName: String

    @Config("type")
    def getType: Type

    @Config("attribute_type")
    @ConfigDefault("null")
    def getAttributeType: Optional[String]

    @Config("timezone")
    @ConfigDefault("null")
    def getTimeZoneId: Optional[String]

    @Config("format")
    @ConfigDefault("null")
    def getFormat: Optional[String]

    @Config("date")
    @ConfigDefault("null")
    def getDate: Optional[String]
  }

  trait Task extends EmbulkTask {

    @Config("json_column_name")
    @ConfigDefault("\"record\"")
    def getJsonColumnName: String

    @Config("columns")
    @ConfigDefault("[]")
    def getColumns: JList[ColumnTask]

    @Config("default_timezone")
    @ConfigDefault("\"UTC\"")
    def getDefaultTimeZoneId: String

    @Config("default_timestamp_format")
    @ConfigDefault("\"%Y-%m-%d %H:%M:%S.%6N %z\"")
    def getDefaultTimestampFormat: String

    @Config("default_date")
    @ConfigDefault("\"1970-01-01\"")
    def getDefaultDate: String
  }

}

case class DynamodbItemSchema(task: DynamodbItemSchema.Task) {

  private lazy val embulkSchema: Schema =
    Schema
      .builder()
      .tap { b =>
        if (isItemAsJson) b.add(task.getJsonColumnName, Types.JSON)
        else
          task.getColumns.asScala.foreach { t =>
            b.add(t.getName, t.getType)
          }
      }
      .build()

  private lazy val timestampFormatters: Map[String, TimestampFormatter] =
    task.getColumns.asScala.map { columnTask =>
      columnTask.getName -> TimestampFormatter
        .builder(
          columnTask.getFormat.orElse(task.getDefaultTimestampFormat),
          true
        )
        .setDefaultZoneFromString(
          columnTask.getTimeZoneId.orElse(task.getDefaultTimeZoneId)
        )
        .setDefaultDateFromString(
          columnTask.getDate.orElse(task.getDefaultDate)
        )
        .build()
    }.toMap

  private lazy val attributeTypes: Map[String, DynamodbAttributeValueType] =
    task.getColumns.asScala
      .filter(_.getAttributeType.isPresent)
      .map { columnTask =>
        columnTask.getName -> DynamodbAttributeValueType(
          columnTask.getAttributeType.get()
        )
      }
      .toMap

  private lazy val embulkColumns: Map[String, Column] =
    getEmbulkSchema.getColumns.asScala
      .map(column => column.getName -> column)
      .toMap

  def getEmbulkSchema: Schema = embulkSchema

  def getTimestampFormatter(column: Column): Option[TimestampFormatter] =
    timestampFormatters.get(column.getName)

  def getTimestampFormatter(columnName: String): Option[TimestampFormatter] =
    getEmbulkColumn(columnName).flatMap(getTimestampFormatter)

  def getAttributeType(column: Column): Option[DynamodbAttributeValueType] =
    attributeTypes.get(column.getName)

  def getAttributeType(
      columnName: String
  ): Option[DynamodbAttributeValueType] =
    getEmbulkColumn(columnName).flatMap(getAttributeType)

  def getEmbulkColumn(columnName: String): Option[Column] =
    embulkColumns.get(columnName)

  def getEmbulkColumn(columnIndex: Int): Option[Column] =
    Try(getEmbulkSchema.getColumn(columnIndex)).toOption

  def isItemAsJson: Boolean = task.getColumns.asScala.isEmpty

  def visitColumns(visitor: DynamodbItemColumnVisitor): Unit =
    getEmbulkSchema.visitColumns(visitor)

  def getItemsConsumer(
      pageBuilder: PageBuilder
  ): Seq[Map[String, AttributeValue]] => Unit = {
    if (isItemAsJson) DynamodbItemConsumer.consumeItemsAsJson(this, pageBuilder)
    else DynamodbItemConsumer.consumeItemsByEmbulkSchema(this, pageBuilder)
  }
}
