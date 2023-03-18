package org.embulk.input.dynamodb.operation

import java.util.Optional

import com.amazonaws.services.dynamodbv2.model.{AttributeValue, ScanRequest}
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import org.embulk.config.ConfigException
import org.embulk.util.config.{Config, ConfigDefault}
import org.embulk.input.dynamodb.logger

import scala.jdk.CollectionConverters._
import scala.util.chaining._
import scala.annotation.tailrec

object DynamodbScanOperation {

  trait Task extends AbstractDynamodbOperation.Task {

    // ref. https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Scan.html#Scan.ParallelScan
    @Config("segment")
    @ConfigDefault("null")
    def getSegment: Optional[Int]

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

  private def newRequest(
      embulkTaskIndex: Int,
      lastEvaluatedKey: Option[Map[String, AttributeValue]]
  ): ScanRequest = {
    new ScanRequest()
      .tap(configureRequest(_, lastEvaluatedKey))
      .tap { r =>
        task.getTotalSegment.ifPresent { totalSegment =>
          r.setTotalSegments(totalSegment)
          r.setSegment(task.getSegment.orElse(embulkTaskIndex))
        }
      }
  }

  @tailrec
  private def runInternal(
      dynamodb: AmazonDynamoDB,
      embulkTaskIndex: Int,
      f: Seq[Map[String, AttributeValue]] => Unit,
      lastEvaluatedKey: Option[Map[String, AttributeValue]] = None,
      loadedRecords: Long = 0
  ): Unit = {
    val loadableRecords: Option[Long] = calculateLoadableRecords(loadedRecords)

    val result =
      dynamodb.scan(newRequest(embulkTaskIndex, lastEvaluatedKey).tap { req =>
        logger.info(s"Call DynamodbScanRequest: ${req.toString}")
      })
    loadableRecords match {
      case Some(v) if (result.getCount > v) =>
        f(result.getItems.asScala.take(v.toInt).map(_.asScala.toMap).toSeq)
      case _ =>
        f(result.getItems.asScala.map(_.asScala.toMap).toSeq)
        Option(result.getLastEvaluatedKey) match {
          case Some(v) =>
            runInternal(
              dynamodb,
              embulkTaskIndex,
              f,
              lastEvaluatedKey = Option(v.asScala.toMap),
              loadedRecords = loadedRecords + result.getCount
            )
          case _ => // do nothing
        }
    }
  }

  override def run(
      dynamodb: AmazonDynamoDB,
      embulkTaskIndex: Int,
      f: Seq[Map[String, AttributeValue]] => Unit
  ): Unit = runInternal(dynamodb, embulkTaskIndex, f)
}
