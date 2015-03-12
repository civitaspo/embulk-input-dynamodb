package org.embulk.input

import com.google.common.base.Optional
import org.embulk.config.{ConfigInject, ConfigDefault, Config, Task}
import org.embulk.spi.{BufferAllocator, SchemaConfig}

trait PluginTask extends Task {
  @Config("access_key")
  @ConfigDefault("null")
  def getAccessKey: Optional[String]

  @Config("secret_key")
  @ConfigDefault("null")
  def getSecretKey: Optional[String]

  @Config("region")
  @ConfigDefault("ap-northeast-1")
  def getRegion: String

  @Config("table")
  def getTable: String

  @Config("limit")
  @ConfigDefault("100")
  def getLimit: Int

  @Config("columns")
  def getColumns: SchemaConfig

  @ConfigInject
  def getBufferAllocator: BufferAllocator
}
