package com.github.traviscrawford.spark.dynamodb

import com.amazonaws.services.dynamodbv2.document.Item
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import scala.collection.JavaConverters._

class ItemConverterSpec extends FlatSpec with Matchers {
  "ItemConverter" should "correctly transform an Item into a Row" in {
    val item = new Item()
      .withString("testString", "a")
      .withList("testStringList", Seq("a").asJava)
      .withInt("testInt", 1)
      .withList("testIntList", Seq(1).asJava)
      .withLong("testLong", 2L)
      .withList("testLongList", Seq(2L).asJava)
      .withFloat("testFloat", 3.3f)
      .withList("testFloatList", Seq(3.3f).asJava)
      .withDouble("testDouble", 4.4d)
      .withList("testDoubleList", Seq(4.4d).asJava)
      .withBoolean("testBoolean", true)
      .withList("testDoubleListList", Seq(true, false, true).asJava)

    val schema = StructType(Seq(
      StructField("testString", StringType),
      StructField("testStringList", ArrayType(StringType)),
      StructField("testInt", IntegerType),
      StructField("testIntList", ArrayType(IntegerType)),
      StructField("testLong", LongType),
      StructField("testLongList", ArrayType(LongType)),
      StructField("testFloat", FloatType),
      StructField("testFloatList", ArrayType(FloatType)),
      StructField("testDouble", DoubleType),
      StructField("testDoubleList", ArrayType(DoubleType)),
      StructField("testBoolean", BooleanType),
      StructField("testDoubleListList", ArrayType(BooleanType))
    ))

    ItemConverter.toRow(item, schema) shouldBe
      Row("a", Seq("a"), 1, Seq(1), 2L, Seq(2L), 3.3f, Seq(3.3f), 4.4d, Seq(4.4d), true, Seq(true, false, true))
  }
}
