package org.embulk.input.dynamodb.ope

import com.amazonaws.services.dynamodbv2.model.{AttributeValue, Condition}
import org.embulk.input.dynamodb.{AttributeValueHelper, PluginTask}
import org.embulk.spi._
import org.embulk.spi.`type`.Types
import org.msgpack.value.{Value, ValueFactory}

import scala.jdk.CollectionConverters._

abstract class AbstractOperation {
  def execute(task: PluginTask, schema: Schema, output: PageOutput): Unit

  def getLimit(limit: Long, recordLimit: Long, recordCount: Long): Int = {
    if (limit > 0 && recordLimit > 0) {
      math.min(limit, recordLimit - recordCount).toInt
    } else if (limit > 0 || recordLimit > 0) {
      math.max(limit, recordLimit).toInt
    } else { 0 }
  }

  def createFilters(task: PluginTask): Map[String, Condition] = {
    val filterMap = collection.mutable.HashMap[String, Condition]()

    Option(task.getFilters.orNull).map { filters =>
      filters.getFilters.asScala.map { filter =>
        val attributeValueList = collection.mutable.ArrayBuffer[AttributeValue]()
        attributeValueList += createAttributeValue(filter.getType, filter.getValue)
        Option(filter.getValue2).map { value2 =>
          attributeValueList+= createAttributeValue(filter.getType, value2) }

        filterMap += filter.getName -> new Condition()
          .withComparisonOperator(filter.getCondition)
          .withAttributeValueList(attributeValueList.asJava)
      }
    }

    filterMap.toMap
  }

  def createAttributeValue(t: String, v: String): AttributeValue = {
    t match {
      case "string" =>
        new AttributeValue().withS(v)
      case "long" | "double" =>
        new AttributeValue().withN(v)
      case "boolean" =>
        new AttributeValue().withBOOL(v.toBoolean)
    }
  }

  def write(pageBuilder: PageBuilder, schema: Schema, items: Seq[Map[String, AttributeValue]]): Long = {
    var count = 0

    items.foreach { item =>
      schema.getColumns.asScala.foreach { column =>
        val value = item.get(column.getName)
        column.getType match {
          case Types.STRING =>
            convert(column, value, pageBuilder.setString)
          case Types.LONG =>
            convert(column, value, pageBuilder.setLong)
          case Types.DOUBLE =>
            convert(column, value, pageBuilder.setDouble)
          case Types.BOOLEAN =>
            convert(column, value, pageBuilder.setBoolean)
          case Types.JSON =>
            convert(column, value, pageBuilder.setJson)
          case _ => /* Do nothing */
        }
      }
      pageBuilder.addRecord()
      count += 1
    }

    count
  }

  def convert[A](column: Column,
                   value: Option[AttributeValue],
                   f: (Column, A) => Unit)(implicit f1: Option[AttributeValue] => A): Unit =
    f(column, f1(value))

  implicit def StringConvert(value: Option[AttributeValue]): String =
    value.map(_.getS).getOrElse("")

  implicit def LongConvert(value: Option[AttributeValue]): Long =
    value.map(_.getN.toLong).getOrElse(0L)

  implicit def DoubleConvert(value: Option[AttributeValue]): Double =
    value.map(_.getN.toDouble).getOrElse(0D)

  implicit def BooleanConvert(value: Option[AttributeValue]): Boolean =
    value.exists(_.getBOOL)

  implicit def JsonConvert(value: Option[AttributeValue]): Value = {
    value.map { attr =>
      AttributeValueHelper.decodeToValue(attr)
    }.getOrElse(ValueFactory.newNil())
  }
}
