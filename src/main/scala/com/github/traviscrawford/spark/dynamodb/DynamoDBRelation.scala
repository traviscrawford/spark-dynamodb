package com.github.traviscrawford.spark.dynamodb

import java.util.concurrent.atomic.AtomicLong

import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec
import com.amazonaws.services.dynamodbv2.xspec.ExpressionSpecBuilder
import com.google.common.util.concurrent.RateLimiter
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.sources.PrunedScan
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions.asScalaIterator
import scala.util.control.NonFatal

/** Scan a DynamoDB table for use as a [[org.apache.spark.sql.DataFrame]].
  *
  * @param tableName        Name of the DynamoDB table to scan.
  * @param maybePageSize    DynamoDB request page size.
  * @param maybeSegments    Number of segments to scan the table with.
  * @param maybeRateLimit   Max number of read capacity units per second each scan segment will consume from
  *                         the DynamoDB table.
  * @param maybeRegion      AWS region of the table to scan.
  * @param maybeSchema      Schema of the DynamoDB table.
  * @param maybeCredentials By default, [[com.amazonaws.auth.DefaultAWSCredentialsProviderChain]]
  *                         will be used, which, which will work for most users. If you have a custom credentials
  *                         provider it can be provided here.
  * @param maybeEndpoint    Endpoint to connect to DynamoDB on. This is intended for tests.
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
  (@transient val spark: SparkSession)
  extends BaseRelation with PrunedScan with BaseScanner {

  private val log = LoggerFactory.getLogger(this.getClass)

  @transient private lazy val Table = getTable(
    tableName, maybeCredentials, maybeRegion, maybeEndpoint)

  private val pageSize = Integer.parseInt(maybePageSize.getOrElse("1000"))

  private val Segments = Integer.parseInt(maybeSegments.getOrElse("1"))

  // Infer schema with JSONRelation for simplicity. A future improvement would be
  // directly processing the scan result set.
  private val TableSchema = maybeSchema.getOrElse({
    val scanSpec = new ScanSpec().withMaxPageSize(pageSize)
    val result = Table.scan(scanSpec)
    val json = result.firstPage().iterator().map(_.toJSON)
    val jsonRDD = spark.sparkContext.parallelize(json.toSeq)
    val jsonDF = spark.read.json(jsonRDD)
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
      ScanConfig(
        table = tableName,
        segment = idx,
        totalSegments = segments.length,
        pageSize = pageSize,
        maybeSchema = Some(schema),
        maybeRequiredColumns = Some(requiredColumns),
        maybeRateLimit = maybeRateLimit,
        maybeCredentials = maybeCredentials,
        maybeRegion = maybeRegion,
        maybeEndpoint = maybeEndpoint)
    })

    val tableDesc = Table.describe()

    log.info(s"Table ${tableDesc.getTableName} contains ${tableDesc.getItemCount} items " +
      s"using ${tableDesc.getTableSizeBytes} bytes.")

    log.info(s"Schema for tableName ${tableDesc.getTableName}: $schema")

    spark.sparkContext
      .parallelize(scanConfigs, scanConfigs.length)
      .flatMap(scan)
  }

  override def getScanSpec(config: ScanConfig): ScanSpec = {
    config.maybeRequiredColumns match {
      case Some(requiredColumns) =>
        val expressionSpecBuilder =
          new ExpressionSpecBuilder().addProjections(requiredColumns: _*)
        super
          .getScanSpec(config)
          .withExpressionSpec(expressionSpecBuilder.buildForScan())
      case None => super.getScanSpec(config)
    }
  }

  def scan(config: ScanConfig): Iterator[Row] = {
    val scanSpec = getScanSpec(config)
    val table = getTable(config)
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
          Some(ItemConverter.toRow(item, config.maybeSchema.get))
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
