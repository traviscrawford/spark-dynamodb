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
      "{\"is_admin\":true,\"__createdAt\":11,\"user_id\":1,\"username\":\"a\"}",
      "{\"is_admin\":false,\"__createdAt\":22,\"user_id\":2,\"username\":\"b\"}",
      "{\"is_admin\":false,\"__createdAt\":33,\"user_id\":3,\"username\":\"c\"}")

    items.collect() should contain theSameElementsAs expected
  }
}
