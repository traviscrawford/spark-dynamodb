package com.github.traviscrawford.spark.dynamodb

import com.amazonaws.auth.AWSCredentialsProviderChain
import org.apache.spark.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.sources.RelationProvider
import org.apache.spark.sql.sources.SchemaRelationProvider
import org.apache.spark.sql.types.StructType

private[dynamodb] class DefaultSource
  extends RelationProvider with SchemaRelationProvider with Logging {

  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String])
    : BaseRelation = getDynamoDBRelation(sqlContext, parameters)

  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      schema: StructType)
    : BaseRelation = getDynamoDBRelation(sqlContext, parameters, Some(schema))

  private def getDynamoDBRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      schema: Option[StructType] = None)
    : DynamoDBRelation = {

    val tableName = parameters.getOrElse("table",
      throw new IllegalArgumentException("Required parameter 'table' was unspecified.")
    )

    // Update docs if the read_capacity_pct default is changed.
    val readCapacityPct = Integer.parseInt(parameters.getOrElse("read_capacity_pct", "20"))

    // Update docs if the page_size default is changed.
    val pageSize = Integer.parseInt(parameters.getOrElse("page_size", "1000"))

    val credentials = getCredentials(parameters.get("aws_credentials_provider_chain"))

    new DynamoDBRelation(
      tableName = tableName,
      pageSize = pageSize,
      maybeRegion = parameters.get("region"),
      maybeEndpoint = parameters.get("endpoint"),
      maybeSchema = schema,
      maybeCredentials = credentials)(sqlContext)
  }

  private def getCredentials(maybeClassName: Option[String])
    : Option[AWSCredentialsProviderChain] = {

    maybeClassName match {
      case Some(className) =>
        logInfo(s"Using AWSCredentialsProviderChain $className")

        val credentials: AWSCredentialsProviderChain =
          Class.forName(className).newInstance().asInstanceOf[AWSCredentialsProviderChain]

        Some(credentials)
      case None => None
    }
  }
}
