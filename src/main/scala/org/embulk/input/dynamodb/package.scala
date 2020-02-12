package org.embulk.input

import org.slf4j.{Logger, LoggerFactory}

package object dynamodb {

  val logger: Logger = LoggerFactory.getLogger(classOf[DynamodbInputPlugin])

}
