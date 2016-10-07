# DynamoDB Data Source for Apache Spark

This library provides support for reading an [Amazon DynamoDB](https://aws.amazon.com/dynamodb/)
table with [Apache Spark](https://spark.apache.org/).

Tables can be read directly as a DataFrame, or as an RDD of stringified JSON. Users can run ad-hoc
SQL queries directly against DynamoDB tables, and easily build ETL pipelines that load DynamoDB
tables into another system. This library was created by the Data Platform team at
[Medium](https://medium.com/).

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**

- [Installation](#installation)
- [DataFrame Usage](#dataframe-usage)
  - [Schemas](#schemas)
  - [Configuration](#configuration)
- [RDD Usage](#rdd-usage)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Installation

Depend on this library in your application with the following Maven coordinates:

```
<dependency>
  <groupId>com.github.traviscrawford</groupId>
  <artifactId>spark-dynamodb</artifactId>
  <version>0.0.5</version>
</dependency>
```

Start a spark shell with this library as a dependency:

```
$ spark-shell --packages com.github.traviscrawford:spark-dynamodb:0.0.5
```

## DataFrame Usage

You can register a DynamoDB table and run SQL queries against it, or query with the Spark SQL DSL.
The schema will be inferred by sampling items in the table, or you can provide your own schema.

```
import com.github.traviscrawford.spark.dynamodb._

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

### Schemas

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
[StructType](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.types.StructType).

### Configuration

| Option | Description |
| --- | --- |
| `rate_limit_per_segment` | Max number of read capacity units per second each scan segment will consume from the DynamoDB table. Default: no rate limit |
| `page_size` | Scan page size. Default: `1000` |
| `segments` | Number of segments to scan the DynamoDB table with. |
| `aws_credentials_provider` | Class name of the AWS credentials provider to use when connecting to DynamoDB. |
| `endpoint` | DynamoDB client endpoint in `http://localhost:8000` format. This is generally not needed and intended for unit tests. |

## RDD Usage

Like most NoSQL databases, DynamoDB does not strictly enforce schemas. For this reason it may not be appropriate to
load your DynamoDB tables as Spark DataFrames. This library provides support for loading DynamoDB tables as
`RDD[String]` containing stringified JSON.

Scan a table from the command-line with the following command. Spark requires the name of your job jar,
though you may not have one when scanning a table - simply put a placeholder name to satisfy the launcher script.

```bash
spark-submit \
  --class com.github.traviscrawford.spark.dynamodb.DynamoBackupJob \
  --packages com.github.traviscrawford:spark-dynamodb:0.0.5 \
  fakeJar
```

The following flags are supported:

```
usage: com.github.traviscrawford.spark.dynamodb.DynamoBackupJob$ [<flag>...]
flags:
  -credentials='java.lang.String': Optional AWS credentials provider class name.
  -help='false': Show this help
  -output='java.lang.String': Path to write the DynamoDB table backup.
  -pageSize='1000': Page size of each DynamoDB request.
  -rateLimit='Int': Max number of read capacity units per second each scan segment will consume.
  -region='java.lang.String': Region of the DynamoDB table to scan.
  -table='java.lang.String': DynamoDB table to scan.
  -totalSegments='1': Number of DynamoDB parallel scan segments.
```

To integrate with your own code, see `DynamoScanner`.
