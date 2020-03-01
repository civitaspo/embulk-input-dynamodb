package org.embulk.input.dynamodb.item

import java.util.{Optional, List => JList}

import com.fasterxml.jackson.annotation.{JsonCreator, JsonValue}
import org.embulk.config.{Config, ConfigDefault, Task => EmbulkTask}
import org.embulk.spi.{Column, Schema}
import org.embulk.spi.`type`.Type
import org.embulk.spi.time.TimestampParser

import scala.jdk.CollectionConverters._
import scala.util.chaining._
import scala.util.Try

object DynamodbItemSchema {

  trait ColumnTask
      extends EmbulkTask
      with TimestampParser.TimestampColumnOption {

    @Config("name")
    def getName: String

    @Config("type")
    def getType: Type

    @Config("attribute_type")
    @ConfigDefault("null")
    def getAttributeType: Optional[String]
  }

  @deprecated(
    message = "for DeprecatedDynamodbInputPlugin",
    since = "0.3.0"
  )
  case class SchemaConfigCompat(columnTasks: Seq[ColumnTask]) {
    @JsonCreator
    def this(columnTasks: JList[ColumnTask]) =
      this(columnTasks.asScala.toSeq)

    @JsonValue
    def getColumnTasks: JList[ColumnTask] = columnTasks.asJava

    def toSchema: Schema =
      Schema
        .builder()
        .tap { b =>
          columnTasks.foreach { t =>
            b.add(t.getName, t.getType)
          }
        }
        .build()
  }

  trait Task extends EmbulkTask with TimestampParser.Task {

    @Config("columns")
    @ConfigDefault("[]")
    def getColumns: SchemaConfigCompat
  }

}

case class DynamodbItemSchema(task: DynamodbItemSchema.Task) {

  // TODO: build in this class after removing SchemaConfigCompat.
  private lazy val embulkSchema: Schema = task.getColumns.toSchema

  private lazy val timestampParsers: Map[String, TimestampParser] =
    task.getColumns.columnTasks.map { columnTask =>
      columnTask.getName -> TimestampParser.of(task, columnTask)
    }.toMap

  private lazy val attributeTypes: Map[String, DynamodbAttributeValueType] =
    task.getColumns.columnTasks
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

  def getTimestampParser(column: Column): Option[TimestampParser] =
    timestampParsers.get(column.getName)

  def getTimestampParser(columnName: String): Option[TimestampParser] =
    getEmbulkColumn(columnName).flatMap(getTimestampParser)

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
}
