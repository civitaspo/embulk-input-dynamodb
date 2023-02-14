package org.embulk.input.dynamodb.item

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

import org.embulk.input.dynamodb.logger
import org.embulk.spi.time.Timestamp
import org.embulk.util.timestamp.TimestampFormatter
import org.msgpack.value.{Value, ValueFactory}

import scala.jdk.CollectionConverters._
import scala.util.chaining._
import java.time.Instant

object DynamodbAttributeValueEmbulkTypeTransformable {

  val TRUTHY_STRINGS: Set[String] = Set(
    "true",
    "True",
    "TRUE",
    "yes",
    "Yes",
    "YES",
    "t",
    "T",
    "y",
    "Y",
    "on",
    "On",
    "ON",
    "1"
  )

  val FALSY_STRINGS: Set[String] = Set(
    "false",
    "False",
    "FALSE",
    "no",
    "No",
    "NO",
    "f",
    "F",
    "n",
    "N",
    "off",
    "Off",
    "OFF",
    "0"
  )
}

case class DynamodbAttributeValueEmbulkTypeTransformable(
    attributeValue: DynamodbAttributeValue,
    typeEnforcer: Option[DynamodbAttributeValueType] = None,
    timestampFormatter: Option[TimestampFormatter] = None
) {

  private def fromAttributeValueType: DynamodbAttributeValueType =
    typeEnforcer.getOrElse(attributeValue.getType)

  private def convertNAsLongOrDouble(n: String): Either[Long, Double] = {
    n.toLongOption match {
      case Some(l) => Left(l)
      case None    => Right(n.toDouble)
    }
  }

  private def convertBAsString(b: ByteBuffer): String = {
    new String(b.array(), StandardCharsets.UTF_8)
  }

  private def hasAttributeValueType: Boolean = {
    fromAttributeValueType.equals(attributeValue.getType)
  }

  def asMessagePack: Option[Value] = {
    if (!hasAttributeValueType) return None
    if (attributeValue.isNull) return None

    Option(fromAttributeValueType match {
      case DynamodbAttributeValueType.S =>
        ValueFactory.newString(attributeValue.getS)
      case DynamodbAttributeValueType.N =>
        convertNAsLongOrDouble(attributeValue.getN) match {
          case Left(v)  => ValueFactory.newInteger(v)
          case Right(v) => ValueFactory.newFloat(v)
        }
      case DynamodbAttributeValueType.B =>
        ValueFactory.newBinary(attributeValue.getB.array())
      case DynamodbAttributeValueType.SS =>
        ValueFactory.newArray(
          attributeValue.getSS.asScala.map(ValueFactory.newString).asJava
        )
      case DynamodbAttributeValueType.NS =>
        ValueFactory.newArray(
          attributeValue.getNS.asScala
            .map(convertNAsLongOrDouble(_) match {
              case Left(v)  => ValueFactory.newInteger(v)
              case Right(v) => ValueFactory.newFloat(v)
            })
            .asJava
        )
      case DynamodbAttributeValueType.BS =>
        ValueFactory.newArray(
          attributeValue.getBS.asScala
            .map(b => ValueFactory.newBinary(b.array()))
            .asJava
        )
      case DynamodbAttributeValueType.M =>
        ValueFactory
          .newMapBuilder()
          .tap { builder =>
            attributeValue.getM.asScala.foreach { x =>
              builder.put(
                ValueFactory.newString(x._1),
                DynamodbAttributeValueEmbulkTypeTransformable(
                  DynamodbAttributeValue(x._2)
                ).asMessagePack.getOrElse(ValueFactory.newNil())
              )
            }
          }
          .build()
      case DynamodbAttributeValueType.L =>
        ValueFactory.newArray(
          attributeValue.getL.asScala.map { av =>
            DynamodbAttributeValueEmbulkTypeTransformable(
              DynamodbAttributeValue(av)
            ).asMessagePack.getOrElse(ValueFactory.newNil())
          }.asJava
        )
      case DynamodbAttributeValueType.BOOL =>
        ValueFactory.newBoolean(attributeValue.getBOOL)
      case _ =>
        logger.warn(
          s"Unsupported AttributeValue: ${attributeValue.getOriginal.toString}"
        )
        return None
    })
  }

  def asBoolean: Option[Boolean] = {
    if (!hasAttributeValueType) return None
    if (attributeValue.isNull) return None

    Option(fromAttributeValueType match {
      case DynamodbAttributeValueType.S =>
        if (
          DynamodbAttributeValueEmbulkTypeTransformable.TRUTHY_STRINGS
            .contains(attributeValue.getS)
        ) true
        else if (
          DynamodbAttributeValueEmbulkTypeTransformable.FALSY_STRINGS
            .contains(attributeValue.getS)
        ) false
        else return None
      case DynamodbAttributeValueType.N =>
        convertNAsLongOrDouble(attributeValue.getN) match {
          case Left(v)  => v > 0
          case Right(v) => v > 0.0
        }
      case DynamodbAttributeValueType.B =>
        val s = convertBAsString(attributeValue.getB)
        if (
          DynamodbAttributeValueEmbulkTypeTransformable.TRUTHY_STRINGS
            .contains(s)
        )
          true
        else if (
          DynamodbAttributeValueEmbulkTypeTransformable.FALSY_STRINGS
            .contains(s)
        ) false
        else return None
      case DynamodbAttributeValueType.BOOL => attributeValue.getBOOL
      case unsupported =>
        logger.debug(s"cannot convert ${unsupported.toString} as boolean.")
        return None
    })
  }

  def asLong: Option[Long] = {
    if (!hasAttributeValueType) return None
    if (attributeValue.isNull) return None

    Option(fromAttributeValueType match {
      case DynamodbAttributeValueType.S =>
        convertNAsLongOrDouble(attributeValue.getS) match {
          case Left(v)  => v
          case Right(v) => v.toLong
        }
      case DynamodbAttributeValueType.N =>
        convertNAsLongOrDouble(attributeValue.getN) match {
          case Left(v)  => v
          case Right(v) => v.toLong
        }
      case DynamodbAttributeValueType.B =>
        convertNAsLongOrDouble(convertBAsString(attributeValue.getB)) match {
          case Left(v)  => v
          case Right(v) => v.toLong
        }
      case DynamodbAttributeValueType.BOOL =>
        if (attributeValue.getBOOL) 1L
        else 0L
      case unsupported =>
        logger.debug(s"cannot convert ${unsupported.toString} as long.")
        return None
    })
  }

  def asDouble: Option[Double] = {
    if (!hasAttributeValueType) return None
    if (attributeValue.isNull) return None

    fromAttributeValueType match {
      case DynamodbAttributeValueType.S => attributeValue.getS.toDoubleOption
      case DynamodbAttributeValueType.N => attributeValue.getN.toDoubleOption
      case DynamodbAttributeValueType.B =>
        convertBAsString(attributeValue.getB).toDoubleOption
      case DynamodbAttributeValueType.BOOL =>
        Option(
          if (attributeValue.getBOOL) 1.0d
          else 0.0d
        )
      case unsupported =>
        logger.debug(s"cannot convert ${unsupported.toString} as double.")
        None
    }
  }

  def asString: Option[String] = {
    if (!hasAttributeValueType) return None
    if (attributeValue.isNull) return None

    Option(fromAttributeValueType match {
      case DynamodbAttributeValueType.S => attributeValue.getS
      case DynamodbAttributeValueType.N => attributeValue.getN
      case DynamodbAttributeValueType.B => convertBAsString(attributeValue.getB)
      case DynamodbAttributeValueType.SS   => asMessagePack.map(_.toJson).get
      case DynamodbAttributeValueType.NS   => asMessagePack.map(_.toJson).get
      case DynamodbAttributeValueType.BS   => asMessagePack.map(_.toJson).get
      case DynamodbAttributeValueType.M    => asMessagePack.map(_.toJson).get
      case DynamodbAttributeValueType.L    => asMessagePack.map(_.toJson).get
      case DynamodbAttributeValueType.BOOL => attributeValue.getBOOL.toString
      case _ =>
        logger.warn(
          s"Unsupported AttributeValue: ${attributeValue.getOriginal.toString}"
        )
        return None
    })
  }

  def asTimestamp: Option[Instant] = {
    timestampFormatter.flatMap(p => asString.map(p.parse))
  }

}
