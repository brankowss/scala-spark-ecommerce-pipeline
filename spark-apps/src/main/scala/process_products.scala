// spark-apps/src/main/scala/process_products.scala

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, to_date}
import org.apache.spark.sql.types.{DecimalType, DateType}

object ProcessProducts {
  def main(args: Array[String]): Unit = {
    
    // 1. Create a SparkSession with Hive integration
    val spark = SparkSession.builder
      .appName("Process Product Dimension")
      .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse")
      .config("hive.metastore.uris", "thrift://hive-metastore:9083")
      .enableHiveSupport()
      .getOrCreate()
      
    println("SparkSession created for product processing.")

    // 2. Read the raw product data from HDFS
    val rawDataPath = "hdfs://namenode:9000/user/spark/raw_dimensions/product_info.csv"
    
    val df_raw_products = spark.read
      .option("header", "true")
      .csv(rawDataPath)
      
    println(s"Successfully read ${df_raw_products.count()} records from HDFS.")
    df_raw_products.show(5)

    // 3. Transform data and cast to correct types
    val df_transformed_products = df_raw_products
      .withColumn("UnitPrice", col("UnitPrice").cast(DecimalType(10, 2)))
      .withColumn("Date", to_date(col("Date"), "yyyy-MM-dd"))
      
    println("Transformed product data. New schema:")
    df_transformed_products.printSchema()
    df_transformed_products.show(5)

    // 4. Write the cleaned data to a new Hive table
    val hiveTableName = "products_bronze"
    
    df_transformed_products
      .write
      .mode("overwrite")
      .saveAsTable(hiveTableName)
      
    println(s"Successfully saved data to Hive table: ${hiveTableName}")

    spark.stop()
  }
}