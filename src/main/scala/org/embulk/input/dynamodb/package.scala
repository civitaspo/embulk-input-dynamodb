package org.embulk.input

import com.google.common.base.Optional
import org.embulk.config.ConfigException
import org.slf4j.{Logger, LoggerFactory}

package object dynamodb {

  val logger: Logger = LoggerFactory.getLogger(classOf[DynamodbInputPlugin])

  def require[A](value: Optional[A], message: String): A = {
    if (value.isPresent) {
      value.get()
    }
    else {
      throw new ConfigException("Required option is not set: " + message)
    }
  }
}
