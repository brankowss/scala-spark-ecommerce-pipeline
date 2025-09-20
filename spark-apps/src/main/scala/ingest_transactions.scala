// spark-apps/scala/ingest_transactions.scala

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, TimestampType}
import org.apache.spark.sql.functions.{col, split, to_timestamp}

object IngestTransactions {
  def main(args: Array[String]): Unit = {
    
    // 1. Create SparkSession with Hive support
    val spark = SparkSession.builder
      // Set a name for the application, which will appear in the Spark UI.
      .appName("HDFS to Hive Ingestion")
      // Configure the default location for Hive tables on HDFS.
      // Both Spark and Hive properties are set for robustness.
      .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse")
      .config("hive.metastore.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse")
      // Provide the URI for the external Hive Metastore service.
      .config("hive.metastore.uris", "thrift://hive-metastore:9083")
      // Enable integration with the Hive Metastore, allowing Spark to work with Hive tables.
      .enableHiveSupport()
      // Build the SparkSession, or get it if it already exists.
      .getOrCreate()
        
    println("SparkSession created with Hive support.")

    // 2. Define the schema for the raw text data
    // This is necessary because we are reading raw text files.
    val rawSchema = new StructType()
      .add("InvoiceNo", StringType, true)
      .add("StockCode", StringType, true)
      .add("Quantity", IntegerType, true)
      .add("InvoiceDate", StringType, true) // Read as String first, then parse
      .add("CustomerID", StringType, true)
      .add("Country", StringType, true)
      
    // 3. Read data from HDFS
    // The path points to the directory where our data generator places the files.
    val rawDataPath = "hdfs://namenode:9000/user/spark/raw_transactions"
    
    val df_raw = spark.read
      .option("header", "false") // Our files do not have a header row
      .schema(rawSchema)
      .csv(rawDataPath)
      
    println(s"Successfully read ${df_raw.count()} records from HDFS.")
    df_raw.show(5, truncate = false)

    // 4. Transform the Data
    val df_transformed = df_raw
      // Parse the InvoiceDate string into a proper Timestamp type
      .withColumn("InvoiceTimestamp", to_timestamp(col("InvoiceDate"), "dd/MM/yyyy HH:mm"))
      // Parse the 'Country' column to extract CountryID and RegionID
      .withColumn("CountryID", split(col("Country"), "-").getItem(0))
      .withColumn("RegionID", split(col("Country"), "-").getItem(1))
      
    println("Data transformed. New schema:")
    df_transformed.printSchema()

    // 5. Write the transformed data to a Hive table
    // This will be our "Bronze" layer table.
    val hiveTableName = "transactions_bronze"
    
    df_transformed
      .write
      .mode("overwrite") // Using overwrite makes the job idempotent and repeatable
      .saveAsTable(hiveTableName) // The command that saves the DataFrame as a Hive table
      
    println(s"Successfully saved data to Hive table: ${hiveTableName}")
    
    // Verify that the data exists in Hive by running a simple query
    spark.sql(s"SELECT * FROM ${hiveTableName} LIMIT 5").show()

    // Stop the SparkSession
    spark.stop()
  }
}