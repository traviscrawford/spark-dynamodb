package com.github.traviscrawford.spark.dynamodb

import org.apache.spark.sql._
import org.apache.spark.sql.types._

/** Test Spark's DynamoDB integration.
  *
  * At this time you must manually start the local DynamoDB server before
  * running tests. This should be automatic in the future.
  *
  * {{{
  *   java -Djava.library.path=./DynamoDBLocal_lib -jar DynamoDBLocal.jar -inMemory
  * }}}
  *
  * @see http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Tools.DynamoDBLocal.html
  */
class DynamoDBRelationSpec() extends BaseSpec {

  private val EndpointKey = "endpoint"
  private val TestUsersTableSchema = StructType(Seq(
    StructField(UserIdKey, LongType),
    StructField(UsernameKey, StringType)))


  "A DynamoDBRelation" should "infer the correct schema" in {
    val usersDF = sqlContext.read
      .option(EndpointKey, LocalDynamoDBEndpoint)
      .dynamodb(TestUsersTableName)

    usersDF.schema shouldEqual TestUsersTableSchema
  }

  it should "get attributes in the inferred schema" in {
    val usersDF = sqlContext.read
      .option(EndpointKey, LocalDynamoDBEndpoint)
      .option("rate_limit_per_segment", "1")
      .dynamodb(TestUsersTableName)

    usersDF.collect() should contain theSameElementsAs Seq(Row(1, "a"), Row(2, "b"), Row(3, "c"))
  }

  it should "get attributes in the user-provided schema" in {
    val usersDF = sqlContext.read
      .schema(TestUsersTableSchema)
      .option(EndpointKey, LocalDynamoDBEndpoint)
      .dynamodb(TestUsersTableName)

    usersDF.collect() should contain theSameElementsAs Seq(Row(1, "a"), Row(2, "b"), Row(3, "c"))
  }

  it should "support EqualTo filters" in {
    val df = sqlContext.read
      .schema(TestUsersTableSchema)
      .option(EndpointKey, LocalDynamoDBEndpoint)
      .dynamodb(TestUsersTableName)

    df.registerTempTable("users")

    sqlContext.sql("select * from users where username = 'a'").collect() should
      contain theSameElementsAs Seq(Row(1, "a"))
  }

  it should "support GreaterThan filters" in {
    val df = sqlContext.read
      .schema(TestUsersTableSchema)
      .option(EndpointKey, LocalDynamoDBEndpoint)
      .dynamodb(TestUsersTableName)

    df.registerTempTable("users")

    sqlContext.sql("select * from users where username > 'b'").collect() should
      contain theSameElementsAs Seq(Row(3, "c"))
  }

  it should "support LessThan filters" in {
    val df = sqlContext.read
      .schema(TestUsersTableSchema)
      .option(EndpointKey, LocalDynamoDBEndpoint)
      .dynamodb(TestUsersTableName)

    df.registerTempTable("users")

    sqlContext.sql("select * from users where username < 'b'").collect() should
      contain theSameElementsAs Seq(Row(1, "a"))
  }
}
