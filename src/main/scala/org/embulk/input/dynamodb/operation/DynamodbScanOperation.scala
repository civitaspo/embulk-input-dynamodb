package org.embulk.input.dynamodb.operation

import java.util.Optional

import org.embulk.config.{Config, ConfigDefault}

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

class DynamodbScanOperation(task: DynamodbScanOperation.Task)
    extends AbstractDynamodbOperation(task) {}
