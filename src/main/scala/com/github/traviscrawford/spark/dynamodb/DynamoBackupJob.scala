package com.github.traviscrawford.spark.dynamodb

import java.net.URI

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

/** Backup a DynamoDB table as JSON.
  *
  * The full table is scanned and the results are stored in the given output path.
  */
object DynamoBackupJob extends Job {
  private val region = flag[String]("region", "Region of the DynamoDB table to scan.")

  private val table = flag[String]("table", "DynamoDB table to scan.")

  private val totalSegments = flag("totalSegments", 1, "Number of DynamoDB parallel scan segments.")

  private val pageSize = flag("pageSize", 1000, "Page size of each DynamoDB request.")

  private val output = flag[String]("output", "Path to write the DynamoDB table backup.")

  private val overwrite = flag("overwrite", false, "Set to true to overwrite output path.")

  private val credentials = flag[String]("credentials",
    "Optional AWS credentials provider class name.")

  private val rateLimit = flag[Int]("rateLimit",
    "Max number of read capacity units per second each scan segment will consume.")

  def run(): Unit = {
    val maybeCredentials = if (credentials.isDefined) Some(credentials()) else None
    val maybeRateLimit = if (rateLimit.isDefined) Some(rateLimit()) else None
    val maybeRegion = if (region.isDefined) Some(region()) else None
    val awsAccessKey = None
    val awsSecretKey = None
    if (overwrite()) deleteOutputPath(output())

    DynamoScanner(sc, table(), totalSegments(), pageSize(),
      maybeCredentials, awsAccessKey, awsSecretKey, maybeRateLimit, maybeRegion).saveAsTextFile(output())
  }

  private def deleteOutputPath(output: String): Unit = {
    log.info(s"Deleting existing output path $output")
    FileSystem.get(new URI(output), sc.hadoopConfiguration)
      .delete(new Path(output), true)
  }
}
