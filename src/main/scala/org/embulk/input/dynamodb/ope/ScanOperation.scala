package org.embulk.input.dynamodb.ope

import java.util.{List => JList, Map => JMap}

import com.amazonaws.services.dynamodbv2.{AmazonDynamoDB, AmazonDynamoDBClient}
import com.amazonaws.services.dynamodbv2.model.{
  AttributeValue,
  Condition,
  ScanRequest,
  ScanResult
}
import org.embulk.input.dynamodb.PluginTask
import org.embulk.spi.{BufferAllocator, PageBuilder, PageOutput, Schema}

import scala.jdk.CollectionConverters._

class ScanOperation(client: AmazonDynamoDB) extends AbstractOperation {

  override def execute(
      task: PluginTask,
      schema: Schema,
      output: PageOutput
  ): Unit = {
    val allocator: BufferAllocator = task.getBufferAllocator
    val pageBuilder: PageBuilder = new PageBuilder(allocator, schema, output)

    val attributes: JList[String] =
      schema.getColumns.asScala.map(_.getName).asJava
    val scanFilter: JMap[String, Condition] = createFilters(task).asJava
    var evaluateKey: JMap[String, AttributeValue] = null

    val scanLimit: Long = task.getScanLimit
    val recordLimit: Long = task.getRecordLimit
    var recordCount: Long = 0

    do {
      val batchSize = getLimit(scanLimit, recordLimit, recordCount)

      val request: ScanRequest = new ScanRequest()
        .withTableName(task.getTable)
        .withAttributesToGet(attributes)
        .withScanFilter(scanFilter)
        .withExclusiveStartKey(evaluateKey)

      if (batchSize > 0) {
        request.setLimit(batchSize)
      }

      val result: ScanResult = client.scan(request)
      evaluateKey = result.getLastEvaluatedKey

      val items = result.getItems.asScala.map(_.asScala.toMap).toSeq
      recordCount += write(pageBuilder, schema, items)
    } while (evaluateKey != null && (recordLimit == 0 || recordLimit > recordCount))

    pageBuilder.finish()
  }
}
