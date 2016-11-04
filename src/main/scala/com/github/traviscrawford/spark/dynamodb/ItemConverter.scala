package com.github.traviscrawford.spark.dynamodb

import com.amazonaws.services.dynamodbv2.document.Item
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.annotation.tailrec

private[dynamodb] object ItemConverter {
  private implicit val Formats = DefaultFormats

  /** Item to Row converter.
    *
    * Note this is a simple converter for use in tests while determining the overall structure,
    * and does not focus on efficiency or handling all data types at this time.
    */
  def toRow(item: Item, schema: StructType, requiredColumns: Seq[String]): Row = {
    val json = parse(item.toJSON)

    val values: Seq[Any] = requiredColumns.map(field => {
      val jsonFieldValue = json \ field

      extractValue(schema(field).dataType, jsonFieldValue)

    })

    Row.fromSeq(values)
  }

  private def extractValue(schemaType: DataType, field: JValue): Any = {
    field match {
      case JNothing => null
      case _ => schemaType match {
        case IntegerType => field.extract[Int]
        case LongType => field.extract[Long]
        case DoubleType => field.extract[Double]
        case StringType => field.extract[String]
        case BooleanType => field.extract[Boolean]
        case ArrayType(elementType, _) => elementType match {
          case IntegerType => field.extract[List[Int]]
          case LongType => field.extract[List[Long]]
          case DoubleType => field.extract[List[Double]]
          case StringType => field.extract[List[String]]
          case BooleanType => field.extract[List[Boolean]]
          case StructType(fields) => field match {
            case JArray(fields) => fields.map(extractValue(elementType, _))
          }
        }
        case StructType(fields) => Row.fromSeq(fields.map(f => extractValue(f.dataType, field \ f.name)))
      }
    }
  }
}
