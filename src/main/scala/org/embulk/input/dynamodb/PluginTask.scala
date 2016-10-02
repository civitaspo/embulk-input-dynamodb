package org.embulk.input.dynamodb

import com.google.common.base.Optional
import org.embulk.config.{Config, ConfigDefault, ConfigInject, Task}
import org.embulk.spi.{BufferAllocator, SchemaConfig}

trait PluginTask extends Task {
  @Config("auth_method")
  @ConfigDefault("null")
  def getAuthMethod: Optional[String]

  @Config("access_key")
  @ConfigDefault("null")
  def getAccessKey: Optional[String]

  @Config("secret_key")
  @ConfigDefault("null")
  def getSecretKey: Optional[String]

  @Config("profile_name")
  @ConfigDefault("null")
  def getProfileName: Optional[String]

  @Config("region")
  @ConfigDefault("null")
  def getRegion: Optional[String]

  @Config("end_point")
  @ConfigDefault("null")
  def getEndPoint: Optional[String]

  @Config("operation")
  def getOperation: String

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
