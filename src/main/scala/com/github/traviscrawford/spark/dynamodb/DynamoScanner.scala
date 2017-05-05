package com.github.traviscrawford.spark.dynamodb

import com.amazonaws.services.dynamodbv2.document.ScanFilter
import com.google.common.util.concurrent.RateLimiter
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions.asScalaIterator

/** Scan a DynamoDB table in parallel and return items as stringified JSON.
  *
  * Amazon recommends table scans "Avoid Sudden Bursts of Read Activity", and documents
  * how to "Reduce Page Size" as a technique to achieve an even distribution of requests and size.
  * For details, see:
  * http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/QueryAndScanGuidelines.html
  */
object DynamoScanner extends BaseScanner {
  private val log = LoggerFactory.getLogger(this.getClass)

  def apply(
    sc: SparkContext,
    table: String,
    totalSegments: Int,
    pageSize: Int,
    maybeCredentials: Option[String] = None,
    maybeRateLimit: Option[Int] = None,
    maybeRegion: Option[String] = None,
    maybeEndpoint: Option[String] = None,
    maybeFilters : Option[List[ScanFilter]] = None)
  : RDD[String] = {

    val segments = 0 until totalSegments
    val scanConfigs = segments.map(idx => {
      ScanConfig(
        table = table,
        segment = idx,
        totalSegments = segments.length,
        pageSize = pageSize,
        maybeRateLimit = maybeRateLimit,
        maybeCredentials = maybeCredentials,
        maybeRegion = maybeRegion,
        maybeEndpoint = maybeEndpoint,
        maybeFilters = maybeFilters)
    })

    sc.parallelize(scanConfigs, scanConfigs.length).flatMap(scan)
  }

  private def scan(config: ScanConfig): Iterator[String] = {
    val maybeRateLimiter = config.maybeRateLimit.map(rateLimit => {
      log.info(s"Segment ${config.segment} using rate limit of $rateLimit")
      RateLimiter.create(rateLimit)
    })

    val table = getTable(config)
    val scanSpec = getScanSpec(config)
    val result = table.scan(scanSpec)

    // Each `pages.next` call results in a DynamoDB network call.
    result.pages().iterator().flatMap(page => {
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

      // This result set resides in local memory.
      page.iterator().map(_.toJSON)
    })
  }
}
