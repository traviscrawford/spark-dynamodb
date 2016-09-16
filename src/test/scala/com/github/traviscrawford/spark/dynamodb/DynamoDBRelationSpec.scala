package com.github.traviscrawford.spark.dynamodb

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import com.amazonaws.services.dynamodbv2.document.DynamoDB
import com.amazonaws.services.dynamodbv2.document.Item
import com.amazonaws.services.dynamodbv2.model._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.scalatest._

import scala.collection.JavaConversions._

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
class DynamoDBRelationSpec() extends FlatSpec with Matchers {

  private val LocalDynamoDBEndpoint = "http://localhost:8000"
  private val EndpointKey = "endpoint"
  private val TestUsersTableName = "test_users"
  private val UserIdKey = "user_id"
  private val UsernameKey = "username"
  private val TestUsersTableSchema = StructType(Seq(
    StructField(UserIdKey, LongType),
    StructField(UsernameKey, StringType)))

  private val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local")
  private val sc = new SparkContext(conf)
  private val sqlContext = new SQLContext(sc)

  override def withFixture(test: NoArgTest): Outcome = {
    initializeTestUsersTable()
    super.withFixture(test)
  }

  "A DynamoDBRelation" should "infer the correct schema" in {
    val usersDF = sqlContext.read
      .option(EndpointKey, LocalDynamoDBEndpoint)
      .dynamodb(TestUsersTableName)

    usersDF.schema shouldEqual TestUsersTableSchema
  }

  it should "get attributes in the inferred schema" in {
    val usersDF = sqlContext.read
      .option(EndpointKey, LocalDynamoDBEndpoint)
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
