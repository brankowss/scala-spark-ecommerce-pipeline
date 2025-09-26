// spark-apps/src/main/scala/ingest_transactions.scala

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, TimestampType}

object IngestTransactions {
  def main(args: Array[String]): Unit = {
    
    val spark = SparkSession.builder
      .appName("HDFS to Hive Ingestion with Data Quality")
      .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse")
      .config("hive.metastore.uris", "thrift://hive-metastore:9083")
      .enableHiveSupport()
      .getOrCreate()
      
    import spark.implicits._

    // Define the schema for the raw text data.
    val rawSchema = new StructType()
      .add("InvoiceNo", StringType, true)
      .add("StockCode", StringType, true)
      .add("Quantity", IntegerType, true)
      .add("InvoiceDate", StringType, true)
      .add("CustomerID", StringType, true)
      .add("Country", StringType, true)

    // Read the raw transaction data from HDFS.
    val rawDataPath = "hdfs://namenode:9000/user/spark/raw_transactions"
    val df_raw = spark.read
      .option("header", "false")
      .schema(rawSchema)
      .csv(rawDataPath)

    // Perform initial transformations (timestamps, splitting columns).
    val df_transformed = df_raw
      .withColumn("InvoiceTimestamp", to_timestamp(col("InvoiceDate"), "dd/MM/yyyy HH:mm"))
      .withColumn("CountryID", split(col("Country"), "-").getItem(0))
      .withColumn("RegionID", split(col("Country"), "-").getItem(1))

    // DATA QUALITY CHECK (EP-10) ---
    println("Starting data quality check for missing countries...")

    // 1. Load the valid countries from the bronze layer to check against.
    val df_valid_countries = spark.table("countries_bronze").select("CountryID")

    // 2. Separate valid transactions from invalid ones using a left join.
    val df_joined = df_transformed.join(df_valid_countries, Seq("CountryID"), "left_outer")

    val df_valid_transactions = df_joined.filter(df_valid_countries("CountryID").isNotNull)
    val df_quarantined_transactions = df_joined.filter(df_valid_countries("CountryID").isNull)

    // 3. Write valid transactions to the Hive bronze table.
    // Use 'append' mode to add new data without deleting the old.
    if (df_valid_transactions.count() > 0) {
      println(s"Writing ${df_valid_transactions.count()} valid transactions to transactions_bronze table...")
      df_valid_transactions
        .select(df_transformed.columns.map(col): _*) // Select original columns to avoid join artifacts
        .write
        .mode("append") // Append new data to the existing table
        .saveAsTable("transactions_bronze")
    } else {
      println("No new valid transactions to write.")
    }

    // 4. Write invalid transactions to a quarantine location on HDFS.
    if (df_quarantined_transactions.count() > 0) {
      val quarantinePath = "hdfs://namenode:9000/user/spark/quarantine/missing_countries"
      println(s"Found ${df_quarantined_transactions.count()} transactions with missing countries. Writing to ${quarantinePath}")
      
      df_quarantined_transactions
        .select(df_transformed.columns.map(col): _*)
        .write
        .mode("append")
        .parquet(quarantinePath) // Save as Parquet for efficiency
    } else {
      println("No transactions with missing countries found.")
    }

    spark.stop()
  }
}