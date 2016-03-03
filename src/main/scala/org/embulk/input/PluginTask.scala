package org.embulk.input

import com.google.common.base.Optional
import org.embulk.config.{ConfigInject, ConfigDefault, Config, Task}
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
  @ConfigDefault("ap-northeast-1")
  def getRegion: String

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
