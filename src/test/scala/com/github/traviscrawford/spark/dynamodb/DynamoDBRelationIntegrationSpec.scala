package com.github.traviscrawford.spark.dynamodb

import org.apache.spark.sql._
import org.apache.spark.sql.types._

class DynamoDBRelationIntegrationSpec() extends BaseIntegrationSpec {

  private val EndpointKey = "endpoint"
  private val TestUsersTableSchema = StructType(Seq(
    StructField(UserIdKey, LongType),
    StructField(UsernameKey, StringType)))

  "A DynamoDBRelation" should "infer the correct schema" in {
    val usersDF = spark.read
      .option(EndpointKey, LocalDynamoDBEndpoint)
      .dynamodb(TestUsersTableName)

    usersDF.schema shouldEqual TestUsersTableSchema
  }

  it should "get attributes in the inferred schema" in {
    val usersDF = spark.read
      .option(EndpointKey, LocalDynamoDBEndpoint)
      .option("rate_limit_per_segment", "1")
      .dynamodb(TestUsersTableName)

    usersDF.collect() should contain theSameElementsAs Seq(Row(1, "a"), Row(2, "b"), Row(3, "c"))
  }

  it should "get attributes in the user-provided schema" in {
    val usersDF = spark.read
      .schema(TestUsersTableSchema)
      .option(EndpointKey, LocalDynamoDBEndpoint)
      .dynamodb(TestUsersTableName)

    usersDF.collect() should contain theSameElementsAs Seq(Row(1, "a"), Row(2, "b"), Row(3, "c"))
  }

  it should "support EqualTo filters" in {
    val df = spark.read
      .schema(TestUsersTableSchema)
      .option(EndpointKey, LocalDynamoDBEndpoint)
      .dynamodb(TestUsersTableName)

    df.createOrReplaceTempView("users")

    spark.sql("select * from users where username = 'a'").collect() should
      contain theSameElementsAs Seq(Row(1, "a"))
  }

  it should "support GreaterThan filters" in {
    val df = spark.read
      .schema(TestUsersTableSchema)
      .option(EndpointKey, LocalDynamoDBEndpoint)
      .dynamodb(TestUsersTableName)

    df.createOrReplaceTempView("users")

    spark.sql("select * from users where username > 'b'").collect() should
      contain theSameElementsAs Seq(Row(3, "c"))
  }

  it should "support LessThan filters" in {
    val df = spark.read
      .schema(TestUsersTableSchema)
      .option(EndpointKey, LocalDynamoDBEndpoint)
      .dynamodb(TestUsersTableName)

    df.createOrReplaceTempView("users")

    spark.sql("select * from users where username < 'b'").collect() should
      contain theSameElementsAs Seq(Row(1, "a"))
  }
}
