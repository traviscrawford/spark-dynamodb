package com.github.traviscrawford.spark.dynamodb

class DynamoScannerIntegrationSpec extends BaseIntegrationSpec {
  "DynamoBackupJob" should "scan a table" in {
    val items = DynamoScanner(spark.sparkContext,
      table = TestUsersTableName,
      totalSegments = 1,
      pageSize = 1000,
      maybeRateLimit = None,
      maybeEndpoint = Some(LocalDynamoDBEndpoint))

    val expected = Array(
      "{\"__createdAt\":11,\"user_id\":1,\"username\":\"a\"}",
      "{\"__createdAt\":22,\"user_id\":2,\"username\":\"b\"}",
      "{\"__createdAt\":33,\"user_id\":3,\"username\":\"c\"}",
      "{\"__createdAt\":44,\"user_id\":4,\"username\":\"modifier:f058b7e1-689f-42af-a9a9-94c5cecc035e\"}")

    items.collect() should contain theSameElementsAs expected
  }
}
