package org.embulk.input.dynamodb

import com.fasterxml.jackson.annotation.JsonProperty
import com.google.common.base.Objects

class FilterConfig {
  private var _name: String = _
  private var _type: String = _
  private var _condition: String = _
  private var _value: String = _
  private var _value2: String = _

  def this(
      @JsonProperty("name") _name: String,
      @JsonProperty("type") _type: String,
      @JsonProperty("condition") _condition: String,
      @JsonProperty("value") _value: String,
      @JsonProperty("value2") _value2: String
  ) {
    this()
    this._name = _name
    this._type = _type
    this._condition = _condition
    this._value = _value
    this._value2 = _value2
  }

  @JsonProperty("name")
  def getName = _name

  @JsonProperty("type")
  def getType = _type

  @JsonProperty("condition")
  def getCondition = _condition

  @JsonProperty("value")
  def getValue = _value

  @JsonProperty("value2")
  def getValue2 = _value2

  override def equals(obj: Any): Boolean = {
    if (this == obj) return true

    if (!obj.isInstanceOf[FilterConfig]) return false

    val other: FilterConfig = obj.asInstanceOf[FilterConfig]
    Objects.equal(this._name, other._name) &&
    Objects.equal(this._type, other._type) &&
    Objects.equal(this._condition, other._condition) &&
    Objects.equal(this._value, other._value) &&
    Objects.equal(this._value2, other._value2)
  }
}
