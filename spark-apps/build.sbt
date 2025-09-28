// Project metadata
name := "ecommerce-pipeline-spark-jobs"
version := "1.0"

// Define the Scala version.
scalaVersion := "2.12.15"

// Define library dependencies.
libraryDependencies ++= Seq(
  // Spark Core, SQL, and Hive libraries.
  "org.apache.spark" %% "spark-core" % "3.5.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "3.5.0" % "provided",
  "org.apache.spark" %% "spark-hive" % "3.5.0" % "provided",
  
  // This is the Spark-Kafka connector library.
  // It is needed for the StreamProcessor job to read data from Kafka topics.
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.0" % "provided"
)