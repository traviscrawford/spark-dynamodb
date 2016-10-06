package com.github.traviscrawford.spark.dynamodb

import java.util.concurrent.atomic.AtomicLong

import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.regions.Region
import com.amazonaws.regions.Regions
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import com.amazonaws.services.dynamodbv2.document.DynamoDB
import com.amazonaws.services.dynamodbv2.document.Table
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec
import com.amazonaws.services.dynamodbv2.model.ReturnConsumedCapacity
import com.amazonaws.services.dynamodbv2.xspec.ExpressionSpecBuilder
import com.google.common.util.concurrent.RateLimiter
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.sources.PrunedScan
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory
import Logger.log

import scala.collection.JavaConversions.asScalaIterator
import scala.util.control.NonFatal

/** Scan a DynamoDB table for use as a [[org.apache.spark.sql.DataFrame]].
  *
  * @param tableName Name of the DynamoDB table to scan.
  * @param maybePageSize DynamoDB request page size.
  * @param maybeSegments Number of segments to scan the table with.
  * @param maybeRateLimit Max number of read capacity units per second each scan segment will consume from
  *   the DynamoDB table.
  * @param maybeRegion AWS region of the table to scan.
  * @param maybeSchema Schema of the DynamoDB table.
  * @param maybeCredentials By default, [[com.amazonaws.auth.DefaultAWSCredentialsProviderChain]]
  *   will be used, which, which will work for most users. If you have a custom credentials
  *   provider it can be provided here.
  * @param maybeEndpoint Endpoint to connect to DynamoDB on. This is intended for tests.
  * @see http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/QueryAndScanGuidelines.html
  */
private[dynamodb] case class DynamoDBRelation(
  tableName: String,
  maybePageSize: Option[String],
  maybeSegments: Option[String],
  maybeRateLimit: Option[Int],
  maybeRegion: Option[String],
  maybeSchema: Option[StructType],
  maybeCredentials: Option[String] = None,
  maybeEndpoint: Option[String])
  (@transient val sqlContext: SQLContext)
  extends BaseRelation with PrunedScan {

  private val Table = DynamoDBRelation.getTable(
    tableName, maybeCredentials, maybeRegion, maybeEndpoint)

  private val pageSize = Integer.parseInt(maybePageSize.getOrElse("1000"))

  private val Segments = Integer.parseInt(maybeSegments.getOrElse("1"))

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
    // TODO(travis): Add items scanned logging back in.

    val segments = 0 until Segments
    val scanConfigs = segments.map(idx => {
      ScanConfig(schema, requiredColumns, tableName, segment = idx,
        totalSegments = segments.length, pageSize, maybeRateLimit = maybeRateLimit,
        maybeCredentials, maybeRegion, maybeEndpoint)
    })

    val tableDesc = Table.describe()

    log.info(s"Table ${tableDesc.getTableName} contains ${tableDesc.getItemCount} items " +
      s"using ${tableDesc.getTableSizeBytes} bytes.")

    log.info(s"Schema for tableName ${tableDesc.getTableName}: $schema")

    sqlContext.sparkContext
      .parallelize(scanConfigs, scanConfigs.length)
      .flatMap(DynamoDBRelation.scan)
  }
}

private object DynamoDBRelation {

  def getTable(
      tableName: String,
      maybeCredentials: Option[String],
      maybeRegion: Option[String],
      maybeEndpoint: Option[String])
    : Table = {

    val amazonDynamoDBClient = maybeCredentials match {
      case Some(credentialsClassName) =>
        logInfo(s"Using AWSCredentialsProvider $credentialsClassName")
        val credentials = Class.forName(credentialsClassName)
          .newInstance().asInstanceOf[AWSCredentialsProvider]
        new AmazonDynamoDBClient(credentials)
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

    val table = getTable(
      tableName = config.tableName,
      maybeCredentials = config.maybeCredentials,
      maybeRegion = config.maybeRegion,
      maybeEndpoint = config.maybeEndpoint)

    val result = table.scan(scanSpec)

    val failureCounter = new AtomicLong()

    val maybeRateLimiter = config.maybeRateLimit.map(rateLimit => {
      log.info(s"Segment ${config.segment} using rate limit of $rateLimit")
      RateLimiter.create(rateLimit)
    })

    // Each `pages.next` call results in a DynamoDB network call.
    result.pages().iterator().flatMap(page => {
      // This result set resides in local memory.
      val rows = page.iterator().flatMap(item => {
        try {
          Some(ItemConverter.toRow(item, config.schema))
        } catch {
          case NonFatal(err) =>
            // Log some example conversion failures but do not spam the logs.
            if (failureCounter.incrementAndGet() < 3) {
              log.error(s"Failed converting item to row: ${item.toJSON}", err)
            }
            None
        }
      })

      // Blocks until rate limit is available.
      maybeRateLimiter.foreach(rateLimiter => {
        // DynamoDBLocal.jar does not implement consumed capacity
        val maybeConsumedCapacityUnits = Option(page.getLowLevelResult.getScanResult.getConsumedCapacity)
          .map(_.getCapacityUnits)
          .map(math.ceil(_).toInt)

        maybeConsumedCapacityUnits.foreach(consumedCapacityUnits => {
          rateLimiter.acquire(consumedCapacityUnits)
        })
      })

      rows
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
  maybeRateLimit: Option[Int],
  maybeCredentials: Option[String],
  maybeRegion: Option[String],
  maybeEndpoint: Option[String])

object Logger extends Serializable {
  @transient lazy val log = LoggerFactory.getLogger(DynamoDBRelation.getClass())
}
