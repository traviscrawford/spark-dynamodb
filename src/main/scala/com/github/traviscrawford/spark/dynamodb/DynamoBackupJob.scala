package com.github.traviscrawford.spark.dynamodb

import java.net.URI

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

/** Backup a DynamoDB table as JSON.
  *
  * The full table is scanned and the results are stored in the given output path.
  */
object DynamoBackupJob extends Job {
  val region = flag[String]("region", "Region of the DynamoDB table to scan.")

  val table = flag[String]("table", "DynamoDB table to scan.")

  val totalSegments = flag("totalSegments", 1, "Number of DynamoDB parallel scan segments.")

  val pageSize = flag("pageSize", 1000, "Page size of each DynamoDB request.")

  val output = flag[String]("output", "Path to write the DynamoDB table backup.")

  val overwrite = flag("overwrite", false, "Set to true to overwrite output path.")

  val credentials = flag[String]("credentials", "Optional AWS credentials provider class name.")

  val rateLimit = flag[Int]("rateLimit",
    "Max number of read capacity units per second each scan segment will consume.")

  def run(): Unit = {
    val maybeCredentials = credentials.isDefined match {
      case true => Some(credentials())
      case false => None
    }

    val maybeRateLimit = rateLimit.isDefined match {
      case true => Some(rateLimit())
      case false => None
    }

    val maybeRegion = region.isDefined match {
      case true => Some(region())
      case false => None
    }

    if (overwrite()) deleteOutputPath(output())

    DynamoScanner(sc, table(), totalSegments(), pageSize(),
      maybeCredentials, maybeRateLimit, maybeRegion).saveAsTextFile(output())
  }

  private def deleteOutputPath(output: String): Unit = {
    log.info(s"Deleting existing output path $output")
    FileSystem.get(new URI(output), sc.hadoopConfiguration)
      .delete(new Path(output), true)
  }
}
