package com.github.traviscrawford.spark.dynamodb

import com.amazonaws.services.dynamodbv2.document.Item
import org.apache.spark.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

class ItemConverter(schema: StructType) extends Logging {

  /** Convert the item into a Row using the given schema. */
  def toRow(item: Item): Row = {
    val itemKeys = item.asMap().keySet()
    val rowValues = schema.fields.map(field => {
      itemKeys.contains(field.name) match {
        case true => // item contains this struct field
          field.dataType match {
            case DoubleType => item.getDouble(field.name)
            case IntegerType => item.getInt(field.name)
            case LongType => item.getLong(field.name)
            case StringType => item.getString(field.name)
            case _ => throw new RuntimeException(s"Unsupported type ${field.dataType}")
          }
        case false => null // item does not contain this struct field
      }
    })

    Row.fromSeq(rowValues)
  }
}
