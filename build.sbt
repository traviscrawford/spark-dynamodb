organization := "com.github.traviscrawford"

name := "spark-dynamodb"

version := "0.0.1-SNAPSHOT"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "com.amazonaws" % "aws-java-sdk-dynamodb" % "1.10.68",
  "com.google.guava" % "guava" % "18.0",
  "org.apache.spark" %% "spark-sql" % "1.6.1" % "provided",
  "org.scalatest" %% "scalatest" % "2.2.6" % "test"
)