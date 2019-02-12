package com.github.traviscrawford.spark.dynamodb

/** Simple parser for filter_expression options.
  *
  * This will add Dynamo indirection so that reserved keywords can
  * be handled in filter expressions.
  *
  * Regex matching inspired by:
  * https://ikaisays.com/2009/04/04/using-pattern-matching-with-regular-expressions-in-scala/
  */
private object ParsedFilterExpression {

  private val CompareLongExpr = """(\w+) (=|>|<|>=|<=|<>) (\d+)""".r
  private val CompareStringExpr = """(\w+) (=|>|<|>=|<=|<>) (\D+)""".r
  private val BeginsWithExpr = """begins_with\((\w+), (\D+)\)""".r
  private val CompareStringExprWithColonInValue = """(\w+) (=|>|<|>=|<=|<>) (.*:.*)""".r
  private val BeginsWithCompareStringExprWithColonInValue = """begins_with\((\w+), (.*:.*)\)""".r

  def apply(filterExpression: String): ParsedFilterExpression = {
    filterExpression match {
      case CompareLongExpr(fieldName, operator, fieldValue) =>
        ParsedFilterExpression(s"#$fieldName $operator :$fieldName",
          Map(s"#$fieldName" -> s"$fieldName"),
          Map(s":$fieldName" -> Long.box(fieldValue.toLong)))
      case CompareStringExpr(fieldName, operator, fieldValue) =>
        ParsedFilterExpression(s"#$fieldName $operator :$fieldName",
          Map(s"#$fieldName" -> s"$fieldName"),
          Map(s":$fieldName" -> s"$fieldValue"))
      case BeginsWithExpr(fieldName, fieldValue) =>
        ParsedFilterExpression(s"begins_with(#$fieldName, :$fieldName)",
          Map(s"#$fieldName" -> s"$fieldName"),
          Map(s":$fieldName" -> s"$fieldValue"))
      case CompareStringExprWithColonInValue(fieldName, operator, fieldValue) =>
        ParsedFilterExpression(s"#$fieldName $operator :$fieldName",
          Map(s"#$fieldName" -> s"$fieldName"),
          Map(s":$fieldName" -> s"$fieldValue"))
      case BeginsWithCompareStringExprWithColonInValue(fieldName, fieldValue) =>
        ParsedFilterExpression(s"begins_with(#$fieldName, :$fieldName)",
          Map(s"#$fieldName" -> s"$fieldName"),
          Map(s":$fieldName" -> s"$fieldValue"))
      // By default, just keep the whole filterExpression without substitutions
      case _ =>
        ParsedFilterExpression(filterExpression,
          Map.empty[String, String],
          Map.empty[String, AnyRef])
    }
  }
}

private[dynamodb] case class ParsedFilterExpression(
  expression: String,
  expressionNames: Map[String, String],
  expressionValues: Map[String, AnyRef]
)
