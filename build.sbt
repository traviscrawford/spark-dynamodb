organization := "com.github.traviscrawford"

name := "spark-dynamodb"

version := "0.0.1-SNAPSHOT"

crossScalaVersions := Seq("2.10.6", "2.11.8")

libraryDependencies ++= Seq(
  "com.amazonaws" % "aws-java-sdk-dynamodb" % "1.10.68",
  "com.google.guava" % "guava" % "18.0",
  "org.apache.spark" %% "spark-sql" % "2.0.0" % "provided",
  "org.scalatest" %% "scalatest" % "2.2.6" % "test"
)

publishTo := Some(s3resolver.value("LevelMoney S3 Maven Repository", s3("level-maven-repo-test/releases/")))