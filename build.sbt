organization := "com.github.traviscrawford"

name := "spark-dynamodb"

version := "0.0.2-SNAPSHOT"

s3sse := true

crossScalaVersions := Seq("2.10.6", "2.11.8")

libraryDependencies ++= Seq(
  "com.amazonaws" % "aws-java-sdk-dynamodb" % "1.11.31",
  "org.apache.spark" %% "spark-sql" % "2.0.0" % "provided",
  "org.scalatest" %% "scalatest" % "2.2.6" % "test"
)

publishTo := Some(s3resolver.value("LevelMoney S3 Maven Repository", s3("level-maven-repo/releases/")))
