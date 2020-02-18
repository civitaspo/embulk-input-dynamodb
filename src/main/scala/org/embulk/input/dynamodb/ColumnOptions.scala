package org.embulk.input.dynamodb

import java.util.{Optional, Map => JMap}

import org.embulk.config.{Config, ConfigDefault, Task => EmbulkTask}
import org.embulk.spi.`type`.Type
import org.embulk.spi.time.TimestampParser

object ColumnOptions {

  trait ColumnOption
      extends EmbulkTask
      with TimestampParser.TimestampColumnOption {

    @Config("attribute_type")
    @ConfigDefault("null")
    def getAttributeType: Optional[String]

    @Config("type")
    @ConfigDefault("null")
    def getType: Optional[Type]
  }

  trait Task extends EmbulkTask with TimestampParser.Task {

    @Config("column_options")
    @ConfigDefault("{}")
    def getColumnOptions: JMap[String, ColumnOption]
  }
}
