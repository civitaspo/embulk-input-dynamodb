package org.embulk.input.dynamodb

import java.util.{Optional, Map => JMap}

import org.embulk.config.{Config, ConfigDefault, Task => EmbulkTask}
import org.embulk.spi.`type`.Type
import org.embulk.spi.time.TimestampParser

object TypeOptions {

  trait TypeOption
      extends EmbulkTask
      with TimestampParser.TimestampColumnOption {

    @Config("type")
    @ConfigDefault("null")
    def getType: Optional[Type]
  }

  trait Task extends EmbulkTask with TimestampParser.Task {

    @Config("type_options")
    @ConfigDefault("{}")
    def getTypeOptions: JMap[String, TypeOption]
  }
}
