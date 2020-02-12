package org.embulk.input.dynamodb

import com.google.common.base.Optional
import org.embulk.config.{Config, ConfigDefault, ConfigInject, Task}
import org.embulk.input.dynamodb.aws.Aws
import org.embulk.spi.{BufferAllocator, SchemaConfig}

trait PluginTask extends Task with Aws.Task {

  @Config("operation")
  def getOperation: String

  @Config("limit")
  @ConfigDefault("0")
  def getLimit: Long

  @Config("scan_limit")
  @ConfigDefault("0")
  def getScanLimit: Long

  @Config("record_limit")
  @ConfigDefault("0")
  def getRecordLimit: Long

  @Config("table")
  def getTable: String

  @Config("columns")
  def getColumns: SchemaConfig

  @Config("filters")
  @ConfigDefault("null")
  def getFilters: Optional[Filter]

  @ConfigInject
  def getBufferAllocator: BufferAllocator
}
