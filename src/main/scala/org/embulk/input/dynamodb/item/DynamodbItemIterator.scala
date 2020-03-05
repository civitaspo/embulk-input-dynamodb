package org.embulk.input.dynamodb.item

import com.amazonaws.services.dynamodbv2.model.AttributeValue

object DynamodbItemIterator {

  def apply(data: Seq[Map[String, AttributeValue]]): DynamodbItemIterator =
    new DynamodbItemIterator {

      private val delegated: Iterator[Map[String, AttributeValue]] =
        data.iterator
      override def hasNext: Boolean = delegated.hasNext

      override def next(): Map[String, DynamodbAttributeValue] =
        delegated.next().map(x => (x._1, DynamodbAttributeValue(x._2)))
    }
}

trait DynamodbItemIterator extends Iterator[Map[String, DynamodbAttributeValue]]
