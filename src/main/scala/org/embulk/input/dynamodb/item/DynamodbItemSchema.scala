package org.embulk.input.dynamodb.item

import java.util.{Optional, List => JList}

import com.fasterxml.jackson.annotation.{JsonCreator, JsonValue}
import org.embulk.config.{Config, ConfigDefault, Task => EmbulkTask}
import org.embulk.spi.time.TimestampParser
import org.embulk.spi.Schema

import scala.jdk.CollectionConverters._
import scala.util.chaining._

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

case class DynamodbItemSchema(task: DynamodbItemSchema.Task) {}
