package org.embulk.input.dynamodb.item

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.{Optional, List => JList, Map => JMap}

import com.amazonaws.services.dynamodbv2.model.AttributeValue
import org.embulk.util.config.{Config, ConfigDefault, Task => EmbulkTask}

import scala.jdk.CollectionConverters._
import scala.util.chaining._

/** TODO: I want to bind directly `org.embulk.util.config.Config`` to
  * `com.amazonaws.services.dynamodbv2.model.AttributeValue`. Should I implement
  * `com.amazonaws.transform.JsonUnmarshallerContext`?
  */
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

  def apply(item: Map[String, AttributeValue]): DynamodbAttributeValue = {
    val original = new AttributeValue().withM(item.asJava)
    new DynamodbAttributeValue(original)
  }
}

class DynamodbAttributeValue(original: AttributeValue) {

  require(
    message =
      s"Invalid AttributeValue: ${original} which must have 1 attribute value.",
    requirement = {
      Seq(hasS, hasN, hasB, hasSS, hasNS, hasBS, hasM, hasL, hasNULL, hasBOOL)
        .count(has => has) == 1
    }
  )

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

  def getType: DynamodbAttributeValueType = {
    if (hasS) return DynamodbAttributeValueType.S
    if (hasN) return DynamodbAttributeValueType.N
    if (hasB) return DynamodbAttributeValueType.B
    if (hasSS) return DynamodbAttributeValueType.SS
    if (hasNS) return DynamodbAttributeValueType.NS
    if (hasBS) return DynamodbAttributeValueType.BS
    if (hasM) return DynamodbAttributeValueType.M
    if (hasL) return DynamodbAttributeValueType.L
    if (hasNULL) return DynamodbAttributeValueType.NULL
    if (hasBOOL) return DynamodbAttributeValueType.BOOL
    DynamodbAttributeValueType.UNKNOWN
  }
}
