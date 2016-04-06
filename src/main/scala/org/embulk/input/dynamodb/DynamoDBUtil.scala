package org.embulk.input.dynamodb

import java.util.{ArrayList => JArrayList, List => JList, Map => JMap}

import com.amazonaws.ClientConfiguration
import com.amazonaws.regions.Regions
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import com.amazonaws.services.dynamodbv2.model.{AttributeValue, Condition, ScanRequest, ScanResult}
import org.embulk.config.ConfigException
import org.embulk.spi._
import org.embulk.spi.`type`.Types
import org.msgpack.value.{Value, ValueFactory}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

object DynamoDBUtil {
  def createClient(task: PluginTask): AmazonDynamoDBClient = {
    val client = new AmazonDynamoDBClient(
      AwsCredentials.getCredentialsProvider(task),
      new ClientConfiguration()
        .withMaxConnections(50))  // SDK Default Value

    if (task.getEndPoint.isPresent) {
      client.withEndpoint(task.getEndPoint.get())
    } else if (task.getRegion.isPresent) {
      client.withRegion(Regions.fromName(task.getRegion.get()))
    } else {
      throw new ConfigException("At least one of EndPoint or Region must be set")
    }
  }


  def scan(
            task: PluginTask,
            schema: Schema,
            output: PageOutput)
          (implicit client: AmazonDynamoDBClient): Unit =
  {
    val allocator: BufferAllocator = task.getBufferAllocator
    val pageBuilder: PageBuilder = new PageBuilder(allocator, schema, output)

    val attributes: JList[String] = new JArrayList[String]()

    schema.getColumns.foreach { column =>
      attributes.add(column.getName)
    }
    val scanFilter: JMap[String, Condition] = createScanFilter(task)
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

      result.getItems.foreach { item =>
        schema.getColumns.foreach { column =>
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

  private def getScanLimit(scanLimit: Long, recordLimit: Long, recordCount: Long): Int = {
    if (scanLimit > 0 && recordLimit > 0) {
      math.min(scanLimit, recordLimit - recordCount).toInt
    } else if (scanLimit > 0 || recordLimit > 0) {
      math.max(scanLimit, recordLimit).toInt
    } else { 0 }
  }

  private def createScanFilter(task: PluginTask): Map[String, Condition] = {
    val filterMap = collection.mutable.HashMap[String, Condition]()

    Option(task.getFilters.orNull).map { filters =>
      filters.getFilters.map { filter =>
        val attributeValueList = collection.mutable.ArrayBuffer[AttributeValue]()
        attributeValueList += createAttributeValue(filter.getType, filter.getValue)
        Option(filter.getValue2).map { value2 =>
          attributeValueList+= createAttributeValue(filter.getType, value2) }

        filterMap += filter.getName -> new Condition()
          .withComparisonOperator(filter.getCondition)
          .withAttributeValueList(attributeValueList)
      }
    }

    filterMap.toMap
  }

  private def createAttributeValue(t: String, v: String): AttributeValue = {
    t match {
      case "string" =>
        new AttributeValue().withS(v)
      case "long" | "double" =>
        new AttributeValue().withN(v)
      case "boolean" =>
        new AttributeValue().withBOOL(v.toBoolean)
    }
  }

  private def convert[A](column: Column,
                   value: Option[AttributeValue],
                   f: (Column, A) => Unit)(implicit f1: Option[AttributeValue] => A): Unit =
    f(column, f1(value))

  implicit private def StringConvert(value: Option[AttributeValue]): String =
    value.map(_.getS).getOrElse("")

  implicit private def LongConvert(value: Option[AttributeValue]): Long =
    value.map(_.getN.toLong).getOrElse(0L)

  implicit private def DoubleConvert(value: Option[AttributeValue]): Double =
    value.map(_.getN.toDouble).getOrElse(0D)

  implicit private def BooleanConvert(value: Option[AttributeValue]): Boolean =
    value.exists(_.getBOOL)

  implicit private def JsonConvert(value: Option[AttributeValue]): Value = {
    value.map { attr =>
      AttributeValueHelper.decodeToValue(attr)
    }.getOrElse(ValueFactory.newNil())
  }
}
