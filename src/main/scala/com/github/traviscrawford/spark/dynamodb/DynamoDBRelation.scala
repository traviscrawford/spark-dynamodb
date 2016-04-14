package com.github.traviscrawford.spark.dynamodb

import com.amazonaws.auth.AWSCredentialsProviderChain
import com.amazonaws.regions.Region
import com.amazonaws.regions.Regions
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import com.amazonaws.services.dynamodbv2.document.DynamoDB
import com.amazonaws.services.dynamodbv2.document.Table
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec
import com.amazonaws.services.dynamodbv2.model.ReturnConsumedCapacity
import com.amazonaws.services.dynamodbv2.xspec.ExpressionSpecBuilder
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

import scala.collection.JavaConversions.asScalaIterator
import scala.util.control.NonFatal

private[dynamodb] case class DynamoDBRelation(
  tableName: String,
  pageSize: Int,
  maybeRegion: Option[String],
  maybeEndpoint: Option[String],
  maybeSchema: Option[StructType],
  maybeCredentials: Option[AWSCredentialsProviderChain] = None)
  (@transient val sqlContext: SQLContext)
  extends BaseRelation with PrunedScan with Logging {

  private val Table = DynamoDBRelation.getTable(
    tableName, maybeCredentials, maybeRegion, maybeEndpoint)

  // Infer schema with JSONRelation for simplicity. A future improvement would be
  // directly processing the scan result set.
  private val TableSchema = maybeSchema.getOrElse({
    val scanSpec = new ScanSpec().withMaxPageSize(pageSize)
    val result = Table.scan(scanSpec)
    val json = result.firstPage().iterator().map(_.toJSON)
    val jsonRDD = sqlContext.sparkContext.parallelize(json.toSeq)
    val jsonDF = sqlContext.read.json(jsonRDD)
    jsonDF.schema
  })

  /** Get the relation schema.
    *
    * The user-defined schema will be used, if provided. Otherwise the schema is inferred
    * from one page of items scanned from the DynamoDB tableName.
    */
  override def schema: StructType = TableSchema

  override def buildScan(requiredColumns: Array[String]): RDD[Row] = {
    // TODO(travis): Support user-provided credentials provider chain.
    // TODO(travis): Add rate limiter back in.
    // TODO(travis): Add items scanned logging back in.

    val segments = 0 until 3 // TODO(travis): Configuration option for number of scan segments.
    val scanConfigs = segments.map(idx => {
      ScanConfig(schema, requiredColumns, tableName, segment = idx,
        totalSegments = segments.length, pageSize, maybeRegion, maybeEndpoint)
    })

    val tableDesc = Table.describe()

    logInfo(s"Table ${tableDesc.getTableName} contains ${tableDesc.getItemCount} items " +
      s"using ${tableDesc.getTableSizeBytes} bytes.")

    logInfo(s"Schema for tableName ${tableDesc.getTableName}: $schema")

    sqlContext.sparkContext
      .parallelize(scanConfigs, scanConfigs.length)
      .flatMap(DynamoDBRelation.scan)
  }
}

private object DynamoDBRelation extends Logging {

  def getTable(
      tableName: String,
      maybeCredentials: Option[AWSCredentialsProviderChain],
      maybeRegion: Option[String],
      maybeEndpoint: Option[String])
    : Table = {

    val amazonDynamoDBClient = maybeCredentials match {
      case Some(credentials) => new AmazonDynamoDBClient(credentials)
      case None => new AmazonDynamoDBClient()
    }

    maybeRegion.foreach(r => amazonDynamoDBClient.setRegion(Region.getRegion(Regions.fromName(r))))
    maybeEndpoint.foreach(amazonDynamoDBClient.setEndpoint) // for tests
    new DynamoDB(amazonDynamoDBClient).getTable(tableName)
  }

  def scan(config: ScanConfig): Iterator[Row] = {
    val expressionSpecBuilder = new ExpressionSpecBuilder()
      .addProjections(config.requiredColumns: _*)

    val scanSpec = new ScanSpec()
      .withMaxPageSize(config.pageSize)
      .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
      .withExpressionSpec(expressionSpecBuilder.buildForScan())
      .withTotalSegments(config.totalSegments)
      .withSegment(config.segment)

    val amazonDynamoDBClient = new AmazonDynamoDBClient()

    config.maybeRegion.foreach(r =>
      amazonDynamoDBClient.setRegion(Region.getRegion(Regions.fromName(r))))

    config.maybeEndpoint.foreach(amazonDynamoDBClient.setEndpoint) // for tests

    val dynamodb = new DynamoDB(amazonDynamoDBClient)
    val table = dynamodb.getTable(config.tableName)
    val result = table.scan(scanSpec)

    // Each `pages.next` call results in a DynamoDB network call.
    result.pages().iterator().flatMap(page => {
      // This result set resides in local memory.
      page.iterator().flatMap(item => {
        try {
          Some(ItemConverter.toRow(item, config.schema))
        } catch {
          case NonFatal(err) =>
            // TODO(travis): Do not spam the logs.
            logError(s"Failed converting item to row: ${item.toJSON}", err)
            None
        }
      })
    })
  }
}

private case class ScanConfig(
  schema: StructType,
  requiredColumns: Array[String],
  tableName: String,
  segment: Int,
  totalSegments: Int,
  pageSize: Int,
  maybeRegion: Option[String],
  maybeEndpoint: Option[String])
