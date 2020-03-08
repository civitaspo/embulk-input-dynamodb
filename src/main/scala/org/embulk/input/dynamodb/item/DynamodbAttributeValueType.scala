package org.embulk.input.dynamodb.item

sealed abstract class DynamodbAttributeValueType

object DynamodbAttributeValueType {
  final case object S extends DynamodbAttributeValueType
  final case object N extends DynamodbAttributeValueType
  final case object B extends DynamodbAttributeValueType
  final case object SS extends DynamodbAttributeValueType
  final case object NS extends DynamodbAttributeValueType
  final case object BS extends DynamodbAttributeValueType
  final case object M extends DynamodbAttributeValueType
  final case object L extends DynamodbAttributeValueType
  final case object NULL extends DynamodbAttributeValueType
  final case object BOOL extends DynamodbAttributeValueType
  final case object UNKNOWN extends DynamodbAttributeValueType

  def apply(typeName: String): DynamodbAttributeValueType = {
    typeName match {
      case "S"    => S
      case "N"    => N
      case "B"    => B
      case "SS"   => SS
      case "NS"   => NS
      case "BS"   => BS
      case "M"    => M
      case "L"    => L
      case "NULL" => NULL
      case "BOOL" => BOOL
      case _      => UNKNOWN
    }
  }
}
