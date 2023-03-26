name := "SparkAssessment"

version := "0.1"

scalaVersion := "2.13.10"



libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.3.2",
  "org.apache.spark" %% "spark-sql" % "3.3.2" % "compile",
  "org.scalatest" %% "scalatest" % "3.3.0-SNAP3" % Test,
  "org.scalatestplus" %% "scalatestplus-mockito" % "1.0.0-M2" % Test,
  "io.github.cdimascio" % "java-dotenv" % "5.2.2",
  "com.amazonaws" % "aws-java-sdk-bundle" % "1.12.215",
  "org.apache.hadoop" % "hadoop-aws" % "3.3.4",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.4"
)