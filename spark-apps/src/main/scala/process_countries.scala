// spark-apps/src/main/scala/process_countries.scala

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, substring, lit, when}
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}

object ProcessCountries {
  def main(args: Array[String]): Unit = {
    
    // 1. Create a SparkSession with specific configurations for Hive integration.
    val spark = SparkSession.builder
      // Set a name for the application, which will appear in the Spark UI.
      .appName("Process Country Dimension")
      // Configure the default location for Hive tables on HDFS.
      // Both Spark and Hive properties are set for robustness.
      .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse")
      .config("hive.metastore.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse")
      // Provide the URI for the external Hive Metastore service.
      .config("hive.metastore.uris", "thrift://hive-metastore:9083")
      // Enable integration with the Hive Metastore.
      .enableHiveSupport()
      // Build the SparkSession, or get it if it already exists.
      .getOrCreate()
      
    println("SparkSession created for country processing.")

    // 2. Read the raw country data from HDFS
    val rawDataPath = "hdfs://namenode:9000/user/spark/raw_dimensions/countries.csv"
    
    // Read the CSV file,  auto-detect schema, require header
    val df_raw_countries = spark.read
      .option("header", "true") // The first line of the CSV is a header
      .csv(rawDataPath)
      
    println(s"Successfully read ${df_raw_countries.count()} records from HDFS.")
    df_raw_countries.show()

    // 3. Apply transformations to enrich the data based on the project specification
    val df_transformed_countries = df_raw_countries
      // Extract the first two digits from CountryID to get the ContinentID.
      .withColumn("ContinentID", substring(col("CountryID"), 1, 2).cast(IntegerType))
      // Use a when/otherwise clause to map the ContinentID to a ContinentName.
      .withColumn("ContinentName",
        when(col("ContinentID") === 11, "Europe")
        .when(col("ContinentID") === 22, "Africa")
        .when(col("ContinentID") === 33, "Asia")
        .when(col("ContinentID") === 44, "Australia and Oceania")
        .when(col("ContinentID") === 55, "North America")
        .when(col("ContinentID") === 66, "South America")
        .when(col("ContinentID") === 13, "Europe/Asia") // Handle transcontinental cases
        .when(col("ContinentID") === 10, "Europe/Worldwide") // Handle worldwide territories cases
        .when(col("ContinentID") === 50, "North America/Worldwide")
        .otherwise("Unknown")
      )
      
    println("Transformed country data. New schema:")
    df_transformed_countries.printSchema()
    df_transformed_countries.show()

    // 4. Write the cleaned and enriched data to a new Hive table
    val hiveTableName = "countries_bronze"
    
    df_transformed_countries
      .write
      .mode("overwrite") // Overwrite the table on each run to ensure idempotency
      .saveAsTable(hiveTableName) // Saves the DataFrame as a managed Hive table
      
    println(s"Successfully saved data to Hive table: ${hiveTableName}")

    // Stop the SparkSession to release cluster resources
    spark.stop()
  }
}