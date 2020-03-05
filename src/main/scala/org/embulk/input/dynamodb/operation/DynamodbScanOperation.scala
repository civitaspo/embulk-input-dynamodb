package org.embulk.input.dynamodb.operation

import java.util.Optional

import com.amazonaws.services.dynamodbv2.model.{AttributeValue, ScanRequest}
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import org.embulk.config.{Config, ConfigDefault, ConfigException}

import scala.util.chaining._

object DynamodbScanOperation {

  trait Task extends AbstractDynamodbOperation.Task {

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

case class DynamodbScanOperation(task: DynamodbScanOperation.Task)
    extends AbstractDynamodbOperation(task) {

  override def getEmbulkTaskCount: Int = {
    if (task.getTotalSegment.isPresent && task.getSegment.isPresent) 1
    else if (task.getTotalSegment.isPresent && !task.getSegment.isPresent)
      task.getTotalSegment.get()
    else if (!task.getTotalSegment.isPresent && !task.getSegment.isPresent) 1
    else // if (!task.getTotalSegment.isPresent && task.getSegment.isPresent)
      throw new ConfigException(
        "\"segment\" option must be set with \"total_segment\" option."
      )
  }

  private def newRequest: ScanRequest = {
    new ScanRequest()
      .tap(configureRequest)
      .tap(r => task.getSegment.ifPresent(v => r.setSegment(v)))
      .tap(r => task.getTotalSegment.ifPresent(v => r.setTotalSegments(v)))
  }

  override def run(
      dynamodb: AmazonDynamoDB,
      embulkTaskIndex: Int,
      f: List[Map[String, AttributeValue]] => Unit
  ): Unit = {}
}
