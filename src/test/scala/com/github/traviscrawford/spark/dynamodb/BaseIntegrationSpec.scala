package com.github.traviscrawford.spark.dynamodb

import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.amazonaws.services.dynamodbv2.document.DynamoDB
import com.amazonaws.services.dynamodbv2.document.Item
import com.amazonaws.services.dynamodbv2.model._
import org.apache.spark.sql.SparkSession
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
trait BaseIntegrationSpec extends FlatSpec with Matchers {
  protected val spark = BaseIntegrationSpec.spark

  protected val LocalDynamoDBPort = System.getProperty("dynamodb.port")
  protected val LocalDynamoDBEndpoint = s"http://localhost:$LocalDynamoDBPort"
  protected val TestUsersTableName = "test_users"
  protected val UserIdKey = "user_id"
  protected val UsernameKey = "username"
  protected val CreatedAtKey = "__createdAt"

  override def withFixture(test: NoArgTest): Outcome = {
    initializeTestUsersTable()
    super.withFixture(test)
  }

  private def initializeTestUsersTable(): Unit = {
    val amazonDynamoDBClient = AmazonDynamoDBClientBuilder.standard()
      .withEndpointConfiguration(new EndpointConfiguration(LocalDynamoDBEndpoint, "us-west-2"))
      .build()

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
      new Item().withNumber(UserIdKey, 1).withString(UsernameKey, "a").withNumber(CreatedAtKey, 11),
      new Item().withNumber(UserIdKey, 2).withString(UsernameKey, "b").withNumber(CreatedAtKey, 22),
      new Item().withNumber(UserIdKey, 3).withString(UsernameKey, "c").withNumber(CreatedAtKey, 33))

    items.foreach(table.putItem)
  }
}

object BaseIntegrationSpec {
  private val spark = SparkSession.builder
    .master("local")
    .appName(this.getClass.getName)
    .getOrCreate()
}
