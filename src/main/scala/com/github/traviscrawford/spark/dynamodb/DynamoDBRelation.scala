package com.github.traviscrawford.spark.dynamodb

import java.util.concurrent.atomic.AtomicLong

import com.amazonaws.auth.AWSCredentialsProviderChain
import com.amazonaws.regions.Region
import com.amazonaws.regions.Regions
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import com.amazonaws.services.dynamodbv2.document.DynamoDB
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec
import com.amazonaws.services.dynamodbv2.model.ReturnConsumedCapacity
import com.amazonaws.services.dynamodbv2.model.TableDescription
import com.amazonaws.services.dynamodbv2.xspec.Condition
import com.amazonaws.services.dynamodbv2.xspec.ExpressionSpecBuilder
import com.google.common.util.concurrent.RateLimiter
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import com.amazonaws.services.dynamodbv2.xspec.ExpressionSpecBuilder._

import scala.collection.JavaConversions.collectionAsScalaIterable
import scala.collection.JavaConversions.asScalaIterator
import scala.util.control.NonFatal

private[dynamodb] class DynamoDBRelation(
  table: String,
  pageSize: Int,
  readCapacityPct: Int,
  maybeRegion: Option[String],
  maybeEndpoint: Option[String],
  maybeSchema: Option[StructType],
  credentials: Option[AWSCredentialsProviderChain] = None)
  (@transient val sqlContext: SQLContext)
  extends BaseRelation with PrunedFilteredScan with Logging {

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

  val converter = new ItemConverter(schema)

  logInfo(s"Table ${tableDesc.getTableName} contains ${tableDesc.getItemCount} items " +
    s"using ${tableDesc.getTableSizeBytes} bytes.")

  logInfo(s"Schema for table ${tableDesc.getTableName}: $schema")

  /** Get the relation schema.
    *
    * The user-defined schema will be used, if provided. Otherwise the schema is inferred
    * from one page of items scanned from the DynamoDB table.
    */
  override def schema: StructType = TableSchema

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    // Configure the scan. This does not retrieve items from the table.

    val conditions: Seq[Condition] = filters.map {
      case EqualTo(attribute, value) =>
        DynamoDBRelation.getEqualToCondition(schema, attribute, value)

      case GreaterThan(attribute, value) =>
        DynamoDBRelation.getGreaterThanCondition(schema, attribute, value)

      case LessThan(attribute, value) =>
        DynamoDBRelation.getLessThanCondition(schema, attribute, value)

      case _ => throw new RuntimeException(s"Unsupported filters: $filters")
    }

    val expressionSpecBuilder = new ExpressionSpecBuilder().addProjections(requiredColumns: _*)

    if (conditions.nonEmpty) {
      // And conditions together per the `PrunedFilteredScan` contract.
      val condition = conditions.tail.fold(conditions.head)((a, b) => a.and(b))
      expressionSpecBuilder.withCondition(condition)
    }

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
    val failureCounter = new AtomicLong()

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
      val rows: Seq[Row] = page.iterator().flatMap(item => {
        try {
          Some(converter.toRow(item))
        } catch {
          case NonFatal(err) =>
            // Log a few items that fail conversion, but do not flood the logs.
            if (failureCounter.incrementAndGet() < 10) {
              logError(s"Failed converting DynamoDB item: ${item.toJSON}", err)
            }
            None
        }
      }).toSeq

      allRows = allRows.union(sqlContext.sparkContext.parallelize(rows))

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

private object DynamoDBRelation {
  // TODO(travis): Simplify conditions structure and implement the rest.

  private def getEqualToCondition(
      schema: StructType,
      attribute: String,
      value: Any)
    : Condition = {

    val structField = schema.fields(schema.fieldIndex(attribute))
    structField.dataType match {
      case IntegerType => N(attribute).eq(value.asInstanceOf[Int])
      case DoubleType => N(attribute).eq(value.asInstanceOf[Double])
      case LongType => N(attribute).eq(value.asInstanceOf[Long])
      case StringType => S(attribute).eq(value.asInstanceOf[String])
      case _ => throw new RuntimeException(s"Unsupported data type: $structField")
    }
  }

  private def getGreaterThanCondition(
      schema: StructType,
      attribute: String,
      value: Any)
    : Condition = {

    val structField = schema.fields(schema.fieldIndex(attribute))
    structField.dataType match {
      case IntegerType => N(attribute).gt(value.asInstanceOf[Int])
      case DoubleType => N(attribute).gt(value.asInstanceOf[Double])
      case LongType => N(attribute).gt(value.asInstanceOf[Long])
      case StringType => S(attribute).gt(value.asInstanceOf[String])
      case _ => throw new RuntimeException(s"Unsupported data type: $structField")
    }
  }

  private def getLessThanCondition(
      schema: StructType,
      attribute: String,
      value: Any)
    : Condition = {

    val structField = schema.fields(schema.fieldIndex(attribute))
    structField.dataType match {
      case IntegerType => N(attribute).lt(value.asInstanceOf[Int])
      case DoubleType => N(attribute).lt(value.asInstanceOf[Double])
      case LongType => N(attribute).lt(value.asInstanceOf[Long])
      case StringType => S(attribute).lt(value.asInstanceOf[String])
      case _ => throw new RuntimeException(s"Unsupported data type: $structField")
    }
  }
}
