package org.embulk.input.dynamodb.model

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.{Optional, List => JList, Map => JMap}

import com.amazonaws.services.dynamodbv2.model.AttributeValue
import org.embulk.config.{Config, ConfigDefault, Task => EmbulkTask}

import scala.jdk.CollectionConverters._
import scala.util.chaining._

/**
  * TODO: I want to bind directly `org.embulk.config.Config`` to `com.amazonaws.services.dynamodbv2.model.AttributeValue`.
  * Should I implement `com.amazonaws.transform.JsonUnmarshallerContext`?
 **/
object AttributeValue {

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
    def getM: Optional[JMap[String, AttributeValue.Task]]

    @Config("L")
    @ConfigDefault("null")
    def getL: Optional[JList[AttributeValue.Task]]

    @Config("NULL")
    @ConfigDefault("null")
    def getNULL: Optional[Boolean]

    @Config("BOOL")
    @ConfigDefault("null")
    def getBOOL: Optional[Boolean]
  }

  def apply(task: Task): AttributeValue = {
    new AttributeValue()
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
          a.setM(
            v.asScala.map { case (_k: String, _v: Task) => (_k, apply(_v)) }.asJava
          )
        }
      }
      .tap(a => task.getL.ifPresent(v => a.setL(v.asScala.map(apply).asJava)))
      .tap(a => task.getNULL.ifPresent(v => a.setNULL(v)))
      .tap(a => task.getBOOL.ifPresent(v => a.setBOOL(v)))
  }
}
