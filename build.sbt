name := "Projects"

version := "0.1"

scalaVersion := "2.13.10"



libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.3.2",
  "org.apache.spark" %% "spark-sql" % "3.3.2" % "provided",
  "org.scalatest" %% "scalatest" % "3.3.0-SNAP3" % Test,
  "org.scalatestplus" %% "scalatestplus-mockito" % "1.0.0-M2" % Test,
  "com.github.codingwell" %% "scala-dotenv" % "0.9.0"
)