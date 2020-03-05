package org.embulk.input.dynamodb.operation

import java.util.Optional

import com.amazonaws.services.dynamodbv2.model.ScanRequest
import org.embulk.config.{Config, ConfigDefault, ConfigException}

import scala.util.chaining._

object DynamodbScanOperation {

  trait Task extends DynamodbOperationCommonOptions.Task {

    // ref. https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Scan.html#Scan.ParallelScan
    @Config("segment")
    @ConfigDefault("null")
    def getSegment: Optional[Int]

    def setSegment(segment: Optional[Int]): Unit

    // ref. https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Scan.html#Scan.ParallelScan
    @Config("total_segment")
    @ConfigDefault("null")
    def getTotalSegment: Optional[Int]

  }
}

case class DynamodbScanOperation(task: DynamodbScanOperation.Task) {

  def getEmbulkTaskCount: Int = {
    if (task.getTotalSegment.isPresent && task.getSegment.isPresent) 1
    else if (task.getTotalSegment.isPresent && !task.getSegment.isPresent)
      task.getTotalSegment.get()
    else if (!task.getTotalSegment.isPresent && !task.getSegment.isPresent) 1
    else // if (!task.getTotalSegment.isPresent && task.getSegment.isPresent)
      throw new ConfigException(
        "\"segment\" option must be set with \"total_segment\" option."
      )
  }

  def newRequest: ScanRequest = {
    new ScanRequest()
      .tap(r => DynamodbOperationCommonOptions.configureRequest(r, task))
      .tap(r => task.getSegment.ifPresent(v => r.setSegment(v)))
      .tap(r => task.getTotalSegment.ifPresent(v => r.setTotalSegments(v)))
  }
}
