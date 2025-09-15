// Project metadata
name := "ecommerce-pipeline-spark-jobs"
version := "1.0"

// Define the Scala version. This must match the version Spark was compiled with.
scalaVersion := "2.12.15"

// Define library dependencies needed for the project to compile.
libraryDependencies ++= Seq(
  // Spark Core, SQL, and Hive libraries.
  // The "% 'provided'" part is crucial: it tells sbt NOT to include these JARs
  // in our final package, because they already exist on the Spark cluster.
  // This keeps our final JAR file small and clean.
  "org.apache.spark" %% "spark-core" % "3.5.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "3.5.0" % "provided",
  "org.apache.spark" %% "spark-hive" % "3.5.0" % "provided"
)