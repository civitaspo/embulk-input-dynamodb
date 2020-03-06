package org.embulk.input.dynamodb.item

import com.amazonaws.services.dynamodbv2.model.AttributeValue
import org.embulk.spi.PageBuilder

object DynamodbItemConsumer {

  def consumeItemsAsJson(
      schema: DynamodbItemSchema,
      pageBuilder: PageBuilder
  ): Seq[Map[String, AttributeValue]] => Unit = {
    val column = schema.getEmbulkSchema.getColumn(0)
    items: Seq[Map[String, AttributeValue]] =>
      items.foreach { item =>
        val transformable = DynamodbAttributeValueEmbulkTypeTransformable(
          DynamodbAttributeValue(item)
        )
        transformable.asMessagePack match {
          case Some(v) => pageBuilder.setJson(column, v)
          case None    => pageBuilder.setNull(column)
        }
        pageBuilder.addRecord()
      }
  }

  def consumeItemsByEmbulkSchema(
      schema: DynamodbItemSchema,
      pageBuilder: PageBuilder
  ): Seq[Map[String, AttributeValue]] => Unit = {
    items: Seq[Map[String, AttributeValue]] =>
      val itemReader = DynamodbItemReader(schema, DynamodbItemIterator(items))
      val visitor = DynamodbItemColumnVisitor(itemReader, pageBuilder)

      while (itemReader.nextItem) {
        schema.visitColumns(visitor)
        pageBuilder.addRecord()
      }
  }

}
