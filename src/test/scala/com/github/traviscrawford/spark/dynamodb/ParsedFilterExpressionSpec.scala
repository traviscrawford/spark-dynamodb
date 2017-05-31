package com.github.traviscrawford.spark.dynamodb

import org.scalatest.FlatSpec
import org.scalatest.Matchers

/** BDD tests for [[ParsedFilterExpression]]. */
class ParsedFilterExpressionSpec extends FlatSpec with Matchers {

  "ParsedFilterExpression" should "correctly parse compare string expressions" in {
    val parsedExpr = ParsedFilterExpression("name = myName")
    parsedExpr.expression should be ("#name = :name")
    parsedExpr.expressionNames should contain theSameElementsAs Map("#name" -> "name")
    parsedExpr.expressionValues should contain theSameElementsAs Map(":name" -> "myName")
  }

  it should "correctly parse compare long expressions" in {
    val parsedExpr = ParsedFilterExpression("value = 1")
    parsedExpr.expression should be ("#value = :value")
    parsedExpr.expressionNames should contain theSameElementsAs Map("#value" -> "value")
    parsedExpr.expressionValues should contain theSameElementsAs Map(":value" -> Long.box(1L))
  }

  it should "correctly parse begins_with expressions" in {
    val parsedExpr = ParsedFilterExpression("begins_with(name, myName)")
    parsedExpr.expression should be ("begins_with(#name, :name)")
    parsedExpr.expressionNames should contain theSameElementsAs Map("#name" -> "name")
    parsedExpr.expressionValues should contain theSameElementsAs Map(":name" -> "myName")
  }

  it should "correctly parse equals expressions" in {
    val parsedExpr = ParsedFilterExpression("name = myName")
    parsedExpr.expression should be ("#name = :name")
    parsedExpr.expressionNames should contain theSameElementsAs Map("#name" -> "name")
    parsedExpr.expressionValues should contain theSameElementsAs Map(":name" -> "myName")
  }

  it should "correctly parse greaterThan expressions" in {
    val parsedExpr = ParsedFilterExpression("name > myName")
    parsedExpr.expression should be ("#name > :name")
    parsedExpr.expressionNames should contain theSameElementsAs Map("#name" -> "name")
    parsedExpr.expressionValues should contain theSameElementsAs Map(":name" -> "myName")
  }

  it should "correctly parse greaterThanOrEquals expressions" in {
    val parsedExpr = ParsedFilterExpression("name >= myName")
    parsedExpr.expression should be ("#name >= :name")
    parsedExpr.expressionNames should contain theSameElementsAs Map("#name" -> "name")
    parsedExpr.expressionValues should contain theSameElementsAs Map(":name" -> "myName")
  }

  it should "correctly parse lessThan expressions" in {
    val parsedExpr = ParsedFilterExpression("name < myName")
    parsedExpr.expression should be ("#name < :name")
    parsedExpr.expressionNames should contain theSameElementsAs Map("#name" -> "name")
    parsedExpr.expressionValues should contain theSameElementsAs Map(":name" -> "myName")
  }

  it should "correctly parse lessThanOrEqual expressions" in {
    val parsedExpr = ParsedFilterExpression("name <= myName")
    parsedExpr.expression should be ("#name <= :name")
    parsedExpr.expressionNames should contain theSameElementsAs Map("#name" -> "name")
    parsedExpr.expressionValues should contain theSameElementsAs Map(":name" -> "myName")
  }

  it should "correctly parse notEquals expressions" in {
    val parsedExpr = ParsedFilterExpression("name <> myName")
    parsedExpr.expression should be ("#name <> :name")
    parsedExpr.expressionNames should contain theSameElementsAs Map("#name" -> "name")
    parsedExpr.expressionValues should contain theSameElementsAs Map(":name" -> "myName")
  }

  it should "handle expressions that it cannot parse" in {
    val parsedExpr = ParsedFilterExpression("name = myName AND value = 1")
    parsedExpr.expression should be ("name = myName AND value = 1")
    parsedExpr.expressionNames shouldBe empty
    parsedExpr.expressionValues shouldBe empty
  }
}
