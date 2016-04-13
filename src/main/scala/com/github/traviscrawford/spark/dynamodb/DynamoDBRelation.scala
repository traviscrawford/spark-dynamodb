package com.github.traviscrawford.spark.dynamodb

import java.util.concurrent.atomic.AtomicLong

import com.amazonaws.auth.AWSCredentialsProviderChain
import com.amazonaws.regions.Region
import com.amazonaws.regions.Regions
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import com.amazonaws.services.dynamodbv2.document.DynamoDB
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec
import com.amazonaws.services.dynamodbv2.model.ReturnConsumedCapacity
import com.amazonaws.services.dynamodbv2.xspec.ExpressionSpecBuilder
import com.google.common.util.concurrent.RateLimiter
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

import scala.collection.JavaConversions.asScalaIterator

private[dynamodb] class DynamoDBRelation(
  table: String,
  pageSize: Int,
  readCapacityPct: Int,
  maybeRegion: Option[String],
  maybeEndpoint: Option[String],
  maybeSchema: Option[StructType],
  credentials: Option[AWSCredentialsProviderChain] = None)
  (@transient val sqlContext: SQLContext)
  extends BaseRelation with PrunedScan with Logging {

  val amazonDynamoDBClient = credentials match {
    case Some(userProvidedCredentials) => new AmazonDynamoDBClient(userProvidedCredentials)
    case None => new AmazonDynamoDBClient()
  }

  maybeRegion.foreach(r => amazonDynamoDBClient.setRegion(Region.getRegion(Regions.fromName(r))))

  maybeEndpoint.foreach(amazonDynamoDBClient.setEndpoint) // for tests

  val dynamodb = new DynamoDB(amazonDynamoDBClient)
  val dynamodbTable = dynamodb.getTable(table)
  val tableDesc = dynamodbTable.describe()

  private val TableSchema = maybeSchema.getOrElse({
    val scanSpec = new ScanSpec().withMaxPageSize(pageSize)
    val result = dynamodbTable.scan(scanSpec)
    val json = result.firstPage().iterator().map(_.toJSON)
    val jsonRDD = sqlContext.sparkContext.parallelize(json.toSeq)
    val jsonDF = sqlContext.read.json(jsonRDD)
    jsonDF.schema
  })

  logInfo(s"Table ${tableDesc.getTableName} contains ${tableDesc.getItemCount} items " +
    s"using ${tableDesc.getTableSizeBytes} bytes.")

  logInfo(s"Schema for table ${tableDesc.getTableName}: $schema")

  /** Get the relation schema.
    *
    * The user-defined schema will be used, if provided. Otherwise the schema is inferred
    * from one page of items scanned from the DynamoDB table.
    */
  override def schema: StructType = TableSchema

  override def buildScan(requiredColumns: Array[String]): RDD[Row] = {
    // Configure the scan. This does not retrieve items from the table.

    val expressionSpecBuilder = new ExpressionSpecBuilder().addProjections(requiredColumns: _*)

    val scanSpec = new ScanSpec()
      .withMaxPageSize(pageSize)
      .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
      .withExpressionSpec(expressionSpecBuilder.buildForScan())

    scan(scanSpec)
  }

  private def scan(scanSpec: ScanSpec): RDD[Row] = {
    // Configure the DynamoDB table scan.
    val readCapacityUnits = tableDesc.getProvisionedThroughput.getReadCapacityUnits
    val permitsPerSecond = readCapacityUnits / 100.0 * readCapacityPct
    val rateLimiter = RateLimiter.create(permitsPerSecond)
    val itemCounter = new AtomicLong()
    val scannedCounter = new AtomicLong()

    logInfo(s"Scanning ${dynamodbTable.getTableName} with rate limit of $permitsPerSecond " +
      s"read capacity units per second ($readCapacityPct% of $readCapacityUnits " +
      "provisioned read capacity)")

    // Always a reference to the RDD that contains the union of all previously scanned items.
    // scalastyle:off
    var allRows: RDD[Row] = sqlContext.sparkContext.parallelize(Seq[Row]())
    // scalastyle:on

    val result = dynamodbTable.scan(scanSpec)

    // Each `pages.next` call results in a DynamoDB network call.
    result.pages().iterator().foreach(page => {
      itemCounter.addAndGet(page.size())
      scannedCounter.addAndGet(page.getLowLevelResult.getScanResult.getScannedCount.toLong)

      // This result set resides in local memory.
      val rows: RDD[Row] = {
        val json = page.iterator().map(_.toJSON)
        val jsonRDD = sqlContext.sparkContext.parallelize(json.toSeq)
        sqlContext.read.schema(schema).json(jsonRDD).rdd
      }

      allRows = allRows.union(rows)

      Option(page.getLowLevelResult.getScanResult.getConsumedCapacity) match {
        case Some(consumedCapacity) =>
          // Block until rate limit is available.
          val consumedCapacityUnits = math.ceil(consumedCapacity.getCapacityUnits).toInt
          val waited = rateLimiter.acquire(consumedCapacityUnits)
          if (waited >= 1) {
            logInfo(s"Waited $waited seconds before making this request.")
          }
        case None => // consumed capacity will be null when using local dynamodb in tests
      }

      logInfo(s"Retrieved ${itemCounter.get()} of ${scannedCounter.get()} scanned items.")
    })

    allRows
  }
}
