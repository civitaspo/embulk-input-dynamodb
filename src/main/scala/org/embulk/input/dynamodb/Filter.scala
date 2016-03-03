package org.embulk.input.dynamodb

import java.util.{List => JList}

import com.fasterxml.jackson.annotation.{JsonCreator, JsonValue}
import com.google.common.base.Objects

class Filter {
  private var filters: JList[FilterConfig] = _

  @JsonCreator
  def this(filters: JList[FilterConfig]) {
    this()
    this.filters = filters
  }

  @JsonValue
  def getFilters: JList[FilterConfig] = filters

  override def equals(obj: Any): Boolean = {
    if(this == obj) return true

    if(!obj.isInstanceOf[Filter]) return false

    val other: Filter = obj.asInstanceOf[Filter]
    Objects.equal(filters, other.filters)
  }

  override def hashCode: Int = {
    Objects.hashCode(filters)
  }
}
