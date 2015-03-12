package org.embulk.input

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.{AWSCredentials, BasicAWSCredentials, AWSCredentialsProvider}
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.regions.Regions
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import com.amazonaws.services.dynamodbv2.model.{ScanResult, ListTablesResult, ScanRequest}
import org.embulk.spi._

import java.util.{ArrayList => JArrayList, List => JList}
import scala.collection.JavaConversions._

object DynamoDBUtil {
  private def getCredentialsProvider(task: PluginTask): AWSCredentialsProvider = {
   {for {
      accessKey <- Option(task.getAccessKey.orNull)
      secretKey <- Option(task.getSecretKey.orNull)
    } yield {
     new AWSCredentialsProvider {
       override def refresh(): Unit = { }
       override def getCredentials: AWSCredentials = {
         new BasicAWSCredentials(accessKey, secretKey)
       }
     }
    }}.getOrElse{
      new ProfileCredentialsProvider()
    }
  }

  def createClient(task: PluginTask): AmazonDynamoDBClient = {
    val credentialsProvider: AWSCredentialsProvider = getCredentialsProvider(task)
    val client: AmazonDynamoDBClient = new AmazonDynamoDBClient(
      credentialsProvider,
      new ClientConfiguration().withMaxConnections(10))
      .withRegion(Regions.fromName(task.getRegion))

    client
  }

  def listTables(client: AmazonDynamoDBClient): JList[String] = {
    val result: ListTablesResult = client.listTables()
    result.getTableNames
  }

  def scan(client: AmazonDynamoDBClient, task: PluginTask, schema: Schema, output: PageOutput): Unit = {
    val allocator: BufferAllocator = task.getBufferAllocator
    val pageBuilder: PageBuilder = new PageBuilder(allocator, schema, output)

    val limit: Int = task.getLimit
    val attributes: JList[String] = new JArrayList[String]()
    schema.getColumns.foreach { column =>
      attributes.add(column.getName)
    }

    val request: ScanRequest = new ScanRequest()
      .withTableName(task.getTable)
      .withAttributesToGet(attributes)
      .withLimit(limit)

    val result: ScanResult = client.scan(request)
    result.getItems.foreach { item =>
      schema.getColumns.foreach { column =>
        val value = item.get(column.getName)
        column.getType.getName match {
          case "string" =>
            pageBuilder.setString(column, Option(value) map { _.getS } getOrElse { "" })
          case "long" =>
            pageBuilder.setLong(column, Option(value) map { _.getN.toLong } getOrElse { 0L })
          case "double" =>
            pageBuilder.setDouble(column, Option(value) map { _.getN.toDouble } getOrElse { 0D })
          case "boolean" =>
            pageBuilder.setBoolean(column, Option(value) map { _.getBOOL == true } getOrElse { false })
          case _ => /* Do nothing */
        }
      }
      pageBuilder.addRecord()
    }

    pageBuilder.finish()
  }
}
