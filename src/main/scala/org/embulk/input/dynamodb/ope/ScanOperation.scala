package org.embulk.input.dynamodb.ope

import java.util.{ArrayList => JArrayList, List => JList, Map => JMap}

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import com.amazonaws.services.dynamodbv2.model.{AttributeValue, Condition, ScanRequest, ScanResult}
import org.embulk.input.dynamodb.PluginTask
import org.embulk.spi.`type`.Types
import org.embulk.spi.{BufferAllocator, PageBuilder, PageOutput, Schema}

import scala.collection.JavaConverters._

class ScanOperation(client: AmazonDynamoDBClient) extends AbstractOperation {
  override def execute(
            task: PluginTask,
            schema: Schema,
            output: PageOutput): Unit =
  {
    val allocator: BufferAllocator = task.getBufferAllocator
    val pageBuilder: PageBuilder = new PageBuilder(allocator, schema, output)

    val attributes: JList[String] = new JArrayList[String]()

    schema.getColumns.asScala.foreach { column =>
      attributes.add(column.getName)
    }
    val scanFilter: JMap[String, Condition] = createScanFilter(task).asJava
    var evaluateKey: JMap[String, AttributeValue] = null

    val scanLimit: Long   = task.getScanLimit
    val recordLimit: Long = task.getRecordLimit
    var recordCount: Long = 0

    do {
      val batchSize = getScanLimit(scanLimit, recordLimit, recordCount)

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

      result.getItems.asScala.foreach { item =>
        schema.getColumns.asScala.foreach { column =>
          val value = item.asScala.get(column.getName)
          column.getType match {
            case Types.STRING =>
              convert(column, value, pageBuilder.setString)
            case Types.LONG =>
              convert(column, value, pageBuilder.setLong)
            case Types.DOUBLE =>
              convert(column, value, pageBuilder.setDouble)
            case Types.BOOLEAN =>
              convert(column, value, pageBuilder.setBoolean)
            case Types.JSON =>
              convert(column, value, pageBuilder.setJson)
            case _ => /* Do nothing */
          }
        }
        pageBuilder.addRecord()
        recordCount += 1
      }
    } while(evaluateKey != null && (recordLimit == 0 || recordLimit > recordCount))

    pageBuilder.finish()
  }
}
