package com.github.traviscrawford.spark.dynamodb

class DynamoScannerSpec extends BaseSpec {
  "DynamoBackupJob" should "scan a table" in {
    val items = DynamoScanner(sc,
      table = TestUsersTableName,
      totalSegments = 1,
      pageSize = 1000,
      maybeRateLimit = None,
      maybeEndpoint = Some(LocalDynamoDBEndpoint))

    val expected = Array(
      "{\"user_id\":1,\"username\":\"a\"}",
      "{\"user_id\":2,\"username\":\"b\"}",
      "{\"user_id\":3,\"username\":\"c\"}")

    items.collect() should contain theSameElementsAs expected
  }
}
