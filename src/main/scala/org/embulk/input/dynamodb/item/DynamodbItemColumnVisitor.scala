package org.embulk.input.dynamodb.item

import org.embulk.spi.{Column, ColumnVisitor, PageBuilder}

case class DynamodbItemColumnVisitor(
    itemReader: DynamodbItemReader,
    pageBuilder: PageBuilder
) extends ColumnVisitor {

  override def booleanColumn(column: Column): Unit = {
    itemReader.getBoolean(column) match {
      case Some(v) => pageBuilder.setBoolean(column, v)
      case None    => pageBuilder.setNull(column)
    }
  }

  override def longColumn(column: Column): Unit = {
    itemReader.getLong(column) match {
      case Some(v) => pageBuilder.setLong(column, v)
      case None    => pageBuilder.setNull(column)
    }
  }

  override def doubleColumn(column: Column): Unit =
    itemReader.getDouble(column) match {
      case Some(v) => pageBuilder.setDouble(column, v)
      case None    => pageBuilder.setNull(column)
    }

  override def stringColumn(column: Column): Unit = {
    itemReader.getString(column) match {
      case Some(v) => pageBuilder.setString(column, v)
      case None    => pageBuilder.setNull(column)
    }
  }

  override def timestampColumn(column: Column): Unit = {
    itemReader.getTimestamp(column) match {
      case Some(v) => pageBuilder.setTimestamp(column, v)
      case None    => pageBuilder.setNull(column)
    }
  }

  override def jsonColumn(column: Column): Unit =
    itemReader.getJson(column) match {
      case Some(v) => pageBuilder.setJson(column, v)
      case None    => pageBuilder.setNull(column)
    }

}
