package org.embulk.input.dynamodb

import com.amazonaws.services.dynamodbv2.model.AttributeValue
import org.msgpack.value.{Value, ValueFactory}

import scala.util.Try

object AttributeValueHelper {

  // referring aws-scala
  def decodeToValue(value: AttributeValue): Value = {
    import scala.collection.JavaConverters._

    // FIXME: Need Encode?
    lazy val _bin  = Option(value.getB).map(v => ValueFactory.newBinary(v.array))
    lazy val _bool = Option(value.getBOOL).map(v => ValueFactory.newBoolean(v))
    lazy val _num  = Option(value.getN).map(v =>
      Try(v.toLong).map(ValueFactory.newInteger).getOrElse(ValueFactory.newFloat(v.toDouble)))
    lazy val _str  = Option(value.getS).map(v => ValueFactory.newString(v))
    lazy val _nil  = Option(value.getNULL).map(v => ValueFactory.newNil)

    lazy val _list = Option(value.getL).map(l =>
      ValueFactory.newArray(l.asScala.map(v => decodeToValue(v)).asJava))
    lazy val _ss   = Option(value.getSS).map(l =>
      ValueFactory.newArray(l.asScala.map(v => ValueFactory.newString(v)).asJava))
    lazy val _ns   = Option(value.getNS).map(l =>
      ValueFactory.newArray(l.asScala.map(v =>
        Try(v.toLong).map(ValueFactory.newInteger).getOrElse(ValueFactory.newFloat(v.toDouble))).asJava))
    // FIXME: Need Encode?
    lazy val _bs   = Option(value.getBS).map(l =>
      ValueFactory.newArray(l.asScala.map(v => ValueFactory.newBinary(v.array)).asJava))
    lazy val _map  = Option(value.getM).map(m =>
      ValueFactory.newMap(m.asScala.map(v => ValueFactory.newString(v._1) -> decodeToValue(v._2)).asJava))

    _bin.orElse(_bool).orElse(_num).orElse(_str).orElse(_nil)
      .orElse(_list).orElse(_ss).orElse(_ns).orElse(_bs).orElse(_map) match {
      case None => ValueFactory.newNil
      case Some(j) => j
    }
  }
}
