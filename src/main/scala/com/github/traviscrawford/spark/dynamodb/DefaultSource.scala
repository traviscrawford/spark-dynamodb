package com.github.traviscrawford.spark.dynamodb

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.sources.RelationProvider
import org.apache.spark.sql.sources.SchemaRelationProvider
import org.apache.spark.sql.types.StructType

private[dynamodb] class DefaultSource
  extends RelationProvider with SchemaRelationProvider {

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
      maybeSchema: Option[StructType] = None)
    : DynamoDBRelation = {

    val tableName = parameters.getOrElse("table",
      throw new IllegalArgumentException("Required parameter 'table' was unspecified.")
    )

    new DynamoDBRelation(
      tableName = tableName,
      maybePageSize = parameters.get("page_size"),
      maybeReadCpt = parameters.get("read_capacity_pct").map(_.toInt),
      maybeRegion = parameters.get("region"),
      maybeSegments = parameters.get("segments"),
      maybeSchema = maybeSchema,
      maybeCredentials = parameters.get("aws_credentials_provider_chain"),
      maybeEndpoint = parameters.get("endpoint"))(sqlContext)
  }
}
