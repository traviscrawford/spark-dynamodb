package com.github.traviscrawford.spark.dynamodb

import org.apache.spark.sql._
import org.apache.spark.sql.types._

class DynamoDBRelationIntegrationSpec() extends BaseIntegrationSpec {

  private val EndpointKey = "endpoint"
  private val TestUsersTableSchema = StructType(Seq(
    StructField(CreatedAtKey, LongType),
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

    usersDF.collect() should contain theSameElementsAs
      Seq(Row(11, 1, "a"), Row(22, 2, "b"), Row(33, 3, "c"), Row(44, 4, "modifier:f058b7e1-689f-42af-a9a9-94c5cecc035e"))
  }

  it should "get attributes in the user-provided schema" in {
    val usersDF = spark.read
      .schema(TestUsersTableSchema)
      .option(EndpointKey, LocalDynamoDBEndpoint)
      .dynamodb(TestUsersTableName)

    usersDF.collect() should contain theSameElementsAs
      Seq(Row(11, 1, "a"), Row(22, 2, "b"), Row(33, 3, "c"), Row(44, 4, "modifier:f058b7e1-689f-42af-a9a9-94c5cecc035e"))
  }

  it should "support EqualTo filters" in {
    val df = spark.read
      .schema(TestUsersTableSchema)
      .option(EndpointKey, LocalDynamoDBEndpoint)
      .dynamodb(TestUsersTableName)

    df.createOrReplaceTempView("users")

    spark.sql("select * from users where username = 'a'").collect() should
      contain theSameElementsAs Seq(Row(11, 1, "a"))
  }

  it should "support GreaterThan filters" in {
    val df = spark.read
      .schema(TestUsersTableSchema)
      .option(EndpointKey, LocalDynamoDBEndpoint)
      .dynamodb(TestUsersTableName)

    df.createOrReplaceTempView("users")

    spark.sql("select * from users where username > 'b'").collect() should
      contain theSameElementsAs Seq(Row(33, 3, "c"), Row(44, 4, "modifier:f058b7e1-689f-42af-a9a9-94c5cecc035e"))
  }

  it should "support LessThan filters" in {
    val df = spark.read
      .schema(TestUsersTableSchema)
      .option(EndpointKey, LocalDynamoDBEndpoint)
      .dynamodb(TestUsersTableName)

    df.createOrReplaceTempView("users")

    spark.sql("select * from users where username < 'b'").collect() should
      contain theSameElementsAs Seq(Row(11, 1, "a"))
  }

  it should "apply server side filter_expressions" in {
    val df = spark.read
      .schema(TestUsersTableSchema)
      .option(EndpointKey, LocalDynamoDBEndpoint)
      .option("filter_expression", "username <> b")
      .dynamodb(TestUsersTableName)

    df.createOrReplaceTempView("users")

    spark.sql("select * from users where username <> 'c'").collect() should
      contain theSameElementsAs Seq(Row(11, 1, "a"), Row(44, 4, "modifier:f058b7e1-689f-42af-a9a9-94c5cecc035e"))
  }

  it should "apply server side filter_expressions equals" in {
    val df = spark.read
      .schema(TestUsersTableSchema)
      .option(EndpointKey, LocalDynamoDBEndpoint)
      .option("filter_expression", "username = a")
      .dynamodb(TestUsersTableName)

    df.createOrReplaceTempView("users")

    spark.sql("select * from users").collect() should
      contain theSameElementsAs Seq(Row(11, 1, "a"))
  }

  it should "apply server side filter_expressions equals for strings with colons" in {
    val df = spark.read
      .schema(TestUsersTableSchema)
      .option(EndpointKey, LocalDynamoDBEndpoint)
      .option("filter_expression", "username = modifier:f058b7e1-689f-42af-a9a9-94c5cecc035e")
      .dynamodb(TestUsersTableName)

    df.explain()

    df.createOrReplaceTempView("users")

    spark.sql("select * from users").collect() should
      contain theSameElementsAs Seq(Row(44, 4, "modifier:f058b7e1-689f-42af-a9a9-94c5cecc035e"))
  }

  it should "apply server side filter_expressions begins with for strings with colons" in {
    val df = spark.read
      .schema(TestUsersTableSchema)
      .option(EndpointKey, LocalDynamoDBEndpoint)
      .option("filter_expression", "begins_with(username, modifier:f058b7e1-689f-42af-a9a9-94c5cecc035e)")
      .dynamodb(TestUsersTableName)

    df.explain()

    df.createOrReplaceTempView("users")

    spark.sql("select * from users").collect() should
      contain theSameElementsAs Seq(Row(44, 4, "modifier:f058b7e1-689f-42af-a9a9-94c5cecc035e"))
  }
}
