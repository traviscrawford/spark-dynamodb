package com.github.traviscrawford.spark.dynamodb

import com.amazonaws.services.dynamodbv2.document.Item
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import scala.collection.JavaConverters.asScalaBufferConverter

/** Simple DynamoDB [[Item]] to Spark [[Row]] converter.
  *
  * This converter handles common string and numeric conversions, and simple arrays.
  * As DynamoDB stores everything internally as [[BigDecimal]], we cast into the data
  * type defined in the given schema.
  */
private[dynamodb] object ItemConverter {

  def toRow(item: Item, schema: StructType): Row = {
    val values: Seq[Any] = schema.map(field => {
      if (item.hasAttribute(field.name)) {
        field.dataType match {
          case StringType => item.getString(field.name)
          case IntegerType => item.getInt(field.name)
          case LongType => item.getLong(field.name)
          case FloatType => item.getFloat(field.name)
          case DoubleType => item.getDouble(field.name)
          case BooleanType => item.getBoolean(field.name)
          case ArrayType(_, _) => getArrayValue(field, item)
          case _ => throw new IllegalArgumentException(
            s"Unexpected data type ${field.dataType.typeName} field: $field item: $item")
        }
      } else {
        // scalastyle:off null
        null
        // scalastyle:on null
      }
    })

    Row.fromSeq(values)
  }

  private def getArrayValue(field: StructField, item: Item): Any = {
    def getList(fieldName: String): List[java.math.BigDecimal] = {
      item.getList[java.math.BigDecimal](field.name).asScala.toList
    }

    field.dataType.asInstanceOf[ArrayType].elementType match {
      case StringType => item.getList[String](field.name).asScala.toList
      case BooleanType => item.getList[Boolean](field.name).asScala.toList
      case IntegerType => getList(field.name).map(_.intValue())
      case LongType => getList(field.name).map(_.longValue())
      case FloatType => getList(field.name).map(_.floatValue())
      case DoubleType => getList(field.name).map(_.doubleValue())
      case _ => throw new IllegalArgumentException(
        s"Unexpected array element type ${field.dataType.typeName} field: $field item: $item")
    }
  }
}
