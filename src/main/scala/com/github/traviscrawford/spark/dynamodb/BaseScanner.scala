package com.github.traviscrawford.spark.dynamodb

import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.regions.Region
import com.amazonaws.regions.Regions
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import com.amazonaws.services.dynamodbv2.document.DynamoDB
import com.amazonaws.services.dynamodbv2.document.Table
import org.slf4j.LoggerFactory


private[dynamodb] trait BaseScanner {
  private val log = LoggerFactory.getLogger(this.getClass)

  def getTable(
    tableName: String,
    maybeCredentials: Option[String],
    maybeRegion: Option[String],
    maybeEndpoint: Option[String])
  : Table = {

    val amazonDynamoDBClient = maybeCredentials match {
      case Some(credentialsClassName) =>
        log.info(s"Using AWSCredentialsProvider $credentialsClassName")
        val credentials = Class.forName(credentialsClassName)
          .newInstance().asInstanceOf[AWSCredentialsProvider]
        new AmazonDynamoDBClient(credentials)
      case None => new AmazonDynamoDBClient()
    }

    maybeRegion.foreach(r => amazonDynamoDBClient.setRegion(Region.getRegion(Regions.fromName(r))))
    maybeEndpoint.foreach(amazonDynamoDBClient.setEndpoint) // for tests
    new DynamoDB(amazonDynamoDBClient).getTable(tableName)
  }
}
