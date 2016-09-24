package com.github.traviscrawford.spark.dynamodb

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import com.amazonaws.services.dynamodbv2.document.DynamoDB
import com.amazonaws.services.dynamodbv2.document.Item
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.scalatest._

import scala.collection.JavaConversions._

trait BaseSpec extends FlatSpec with Matchers {
  protected val sc = BaseSpec.sc
  protected val sqlContext = BaseSpec.sqlContext

  protected val LocalDynamoDBEndpoint = "http://localhost:8000"
  protected val TestUsersTableName = "test_users"
  protected val UserIdKey = "user_id"
  protected val UsernameKey = "username"

  override def withFixture(test: NoArgTest): Outcome = {
    initializeTestUsersTable()
    super.withFixture(test)
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

object BaseSpec {
  private val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
}
