package com.github.traviscrawford.spark.dynamodb

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import com.amazonaws.services.dynamodbv2.document.DynamoDB
import com.amazonaws.services.dynamodbv2.document.Item
import com.amazonaws.services.dynamodbv2.model._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.scalatest._

import scala.collection.JavaConversions._

/** Test Spark's DynamoDB integration.
  *
  * This runs during Maven's integration-test phase.
  *
  * {{{
  *   mvn integration-test
  * }}}
  *
  * @see http://dynamodb.jcabi.com/
  */
class DynamoDBRelationIntegrationSpec() extends FlatSpec with Matchers {

  private val LocalDynamoDBPort = System.getProperty("dynamodb.port")
  private val LocalDynamoDBEndpoint = s"http://localhost:$LocalDynamoDBPort"
  private val EndpointKey = "endpoint"
  private val TestUsersTableName = "test_users"
  private val UserIdKey = "user_id"
  private val UsernameKey = "username"
  private val TestUsersTableSchema = StructType(Seq(
    StructField(UserIdKey, LongType),
    StructField(UsernameKey, StringType)))

  private val spark = SparkSession.builder
    .master("local")
    .appName("DynamoDBRelationSpec")
    .getOrCreate()

  override def withFixture(test: NoArgTest): Outcome = {
    initializeTestUsersTable()
    super.withFixture(test)
  }

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

  private def initializeTestUsersTable(): Unit = {
    val amazonDynamoDBClient = new AmazonDynamoDBClient()
    amazonDynamoDBClient.setEndpoint(LocalDynamoDBEndpoint)

    val dynamodb = new DynamoDB(amazonDynamoDBClient)

    try {
      dynamodb.getTable(TestUsersTableName).delete()
    } catch {
      case _: ResourceNotFoundException => // pass
    }

    val createTableRequest = new CreateTableRequest()
      .withTableName(TestUsersTableName)
      .withAttributeDefinitions(Seq(new AttributeDefinition(UserIdKey, "N")))
      .withKeySchema(Seq(new KeySchemaElement(UserIdKey, "HASH")))
      .withProvisionedThroughput(new ProvisionedThroughput(10L, 10L))

    val table = dynamodb.createTable(createTableRequest)

    assert(table.getTableName == TestUsersTableName)

    val items = Seq(
      new Item().withNumber(UserIdKey, 1).withString(UsernameKey, "a"),
      new Item().withNumber(UserIdKey, 2).withString(UsernameKey, "b"),
      new Item().withNumber(UserIdKey, 3).withString(UsernameKey, "c"))

    items.foreach(table.putItem)
  }
}
