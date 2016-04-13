# DynamoDB Data Source for Apache Spark

This library provides support for reading an [Amazon DynamoDB](https://aws.amazon.com/dynamodb/)
table as an [Apache Spark](https://spark.apache.org/) DataFrame. Users can run ad-hoc SQL
queries directly against DynamoDB tables, and easily build ETL pipelines that load
DynamoDB tables into another system. This library was created by the Product Science team
at [Medium](https://medium.com/).

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**

- [Quick Start](#quick-start)
- [Schemas](#schemas)
- [Configuration](#configuration)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Quick Start

Install and start a spark shell:

```
$ mvn install
$ spark-shell --packages com.github.traviscrawford:spark-dynamodb:0.0.1-SNAPSHOT
```

You can register a DynamoDB table and run SQL queries against it, or query with the Spark SQL DSL.
The schema will be inferred by sampling items in the table, or you can provide your own schema.

```
// Read a table in the default region.
val users = sqlContext.read.dynamodb("users")

// Or read a table from another region.
val users2 = sqlContext.read.dynamodb("us-west-2", "users")

// Query with SQL.
users.registerTempTable("users")
val data = sqlContext.sql("select username from users where username = 'tc'")

// Or query with the DSL.
val data2 = users.select("username").filter($"username" = "tc")
```

## Schemas

DynamoDB tables do not have a schema beyond the primary key(s). If no schema is provided,
the schema will be inferred from a sample of items in the table. If items with multiple
schemas are stored in the table you may choose to provide the schema.

At a high-level, Spark SQL schemas are a `StructType` that contains a sequence of typed
`StructField`s. At Medium we generate `StructType`s from protobuf schemas that define the data
structure stored in a particular DynamoDB table.

```
// Example schema
val schema = StructType(Seq(
  StructField("userId", LongType),
  StructField("username", StringType)))
```

For details about Spark SQL schemas, see
[[StructType]](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.types.StructType).

## Configuration

| Option | Description |
| --- | --- |
| `read_capacity_pct` | Percent of provisioned read capacity to use. Default: `20` |
| `page_size` | Scan page size. Default: `1000` |
| `aws_credentials_provider_chain` | Class name of the AWS provider chain to use when connecting to DynamoDB. |
| `endpoint` | DynamoDB client endpoint in `http://localhost:8000` format. This is generally not needed and intended for unit tests. |
