package org.embulk.input

import com.google.common.base.Optional
import org.embulk.config.ConfigException

package object dynamodb {

  def require[A](value: Optional[A], message: String): A = {
    if (value.isPresent) {
      value.get()
    }
    else {
      throw new ConfigException("Required option is not set: " + message)
    }
  }
}
