package org.embulk.input.dynamodb.item

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.{Optional, List => JList, Map => JMap}

import com.amazonaws.services.dynamodbv2.model.AttributeValue
import org.embulk.config.{Config, ConfigDefault, Task => EmbulkTask}
import org.embulk.input.dynamodb.logger
import org.embulk.spi.time.Timestamp
import org.msgpack.value.{Value, ValueFactory}

import scala.jdk.CollectionConverters._
import scala.util.chaining._

/**
  * TODO: I want to bind directly `org.embulk.config.Config`` to `com.amazonaws.services.dynamodbv2.model.AttributeValue`.
  * Should I implement `com.amazonaws.transform.JsonUnmarshallerContext`?
 **/
object DynamodbAttributeValue {

  trait Task extends EmbulkTask {

    @Config("S")
    @ConfigDefault("null")
    def getS: Optional[String]

    @Config("N")
    @ConfigDefault("null")
    def getN: Optional[String]

    @Config("B")
    @ConfigDefault("null")
    def getB: Optional[String]

    @Config("SS")
    @ConfigDefault("null")
    def getSS: Optional[JList[String]]

    @Config("NS")
    @ConfigDefault("null")
    def getNS: Optional[JList[String]]

    @Config("BS")
    @ConfigDefault("null")
    def getBS: Optional[JList[String]]

    @Config("M")
    @ConfigDefault("null")
    def getM: Optional[JMap[String, DynamodbAttributeValue.Task]]

    @Config("L")
    @ConfigDefault("null")
    def getL: Optional[JList[DynamodbAttributeValue.Task]]

    @Config("NULL")
    @ConfigDefault("null")
    def getNULL: Optional[Boolean]

    @Config("BOOL")
    @ConfigDefault("null")
    def getBOOL: Optional[Boolean]
  }

  sealed abstract class ValueType

  object ValueType {
    final case object S extends ValueType
    final case object N extends ValueType
    final case object B extends ValueType
    final case object SS extends ValueType
    final case object NS extends ValueType
    final case object BS extends ValueType
    final case object M extends ValueType
    final case object L extends ValueType
    final case object NULL extends ValueType
    final case object BOOL extends ValueType
    final case object UNKNOWN extends ValueType
  }

  def apply(task: Task): DynamodbAttributeValue = {
    val original = new AttributeValue()
      .tap(a => task.getS.ifPresent(v => a.setS(v)))
      .tap(a => task.getN.ifPresent(v => a.setN(v)))
      .tap { a =>
        task.getB.ifPresent { v =>
          a.setB(ByteBuffer.wrap(v.getBytes(StandardCharsets.UTF_8)))
        }
      }
      .tap(a => task.getSS.ifPresent(v => a.setSS(v)))
      .tap(a => task.getNS.ifPresent(v => a.setNS(v)))
      .tap { a =>
        task.getBS.ifPresent { v =>
          a.setBS(
            v.asScala
              .map(e => ByteBuffer.wrap(e.getBytes(StandardCharsets.UTF_8)))
              .asJava
          )
        }
      }
      .tap { a =>
        task.getM.ifPresent { v =>
          a.setM(v.asScala.map(x => (x._1, apply(x._2).getOriginal)).asJava)
        }
      }
      .tap(a =>
        task.getL.ifPresent(v =>
          a.setL(v.asScala.map(apply).map(_.getOriginal).asJava)
        )
      )
      .tap(a => task.getNULL.ifPresent(v => a.setNULL(v)))
      .tap(a => task.getBOOL.ifPresent(v => a.setBOOL(v)))
    new DynamodbAttributeValue(original)
  }

  def apply(original: AttributeValue): DynamodbAttributeValue = {
    new DynamodbAttributeValue(original)
  }
}

class DynamodbAttributeValue(original: AttributeValue) {
  def getOriginal: AttributeValue = original
  def isNull: Boolean = Option[Boolean](getOriginal.getNULL).getOrElse(false)
  def hasS: Boolean = Option(getOriginal.getS).isDefined
  def hasN: Boolean = Option(getOriginal.getN).isDefined
  def hasB: Boolean = Option(getOriginal.getB).isDefined
  def hasSS: Boolean = Option(getOriginal.getSS).isDefined
  def hasNS: Boolean = Option(getOriginal.getNS).isDefined
  def hasBS: Boolean = Option(getOriginal.getBS).isDefined
  def hasM: Boolean = Option(getOriginal.getM).isDefined
  def hasL: Boolean = Option(getOriginal.getL).isDefined
  def hasNULL: Boolean = Option(getOriginal.getNULL).isDefined
  def hasBOOL: Boolean = Option(getOriginal.getBOOL).isDefined
  def getS: String = getOriginal.getS
  def getN: String = getOriginal.getN
  def getB: ByteBuffer = getOriginal.getB
  def getSS: JList[String] = getOriginal.getSS
  def getNS: JList[String] = getOriginal.getNS
  def getBS: JList[ByteBuffer] = getOriginal.getBS
  def getM: JMap[String, AttributeValue] = getOriginal.getM
  def getL: JList[AttributeValue] = getOriginal.getL
  def getNULL: Boolean = getOriginal.getNULL
  def getBOOL: Boolean = getOriginal.getBOOL

  def getValueType: DynamodbAttributeValue.ValueType = {
    if (hasS) return DynamodbAttributeValue.ValueType.S
    if (hasN) return DynamodbAttributeValue.ValueType.N
    if (hasB) return DynamodbAttributeValue.ValueType.B
    if (hasSS) return DynamodbAttributeValue.ValueType.SS
    if (hasNS) return DynamodbAttributeValue.ValueType.NS
    if (hasBS) return DynamodbAttributeValue.ValueType.BS
    if (hasM) return DynamodbAttributeValue.ValueType.M
    if (hasL) return DynamodbAttributeValue.ValueType.L
    if (hasNULL) return DynamodbAttributeValue.ValueType.NULL
    if (hasBOOL) return DynamodbAttributeValue.ValueType.BOOL
    DynamodbAttributeValue.ValueType.UNKNOWN
  }

}
