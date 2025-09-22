// spark-apps/src/main/scala/load_dwh.scala

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import java.util.Properties
import java.io.PrintWriter

object LoadDWH {

  /**
   * Writes a DataFrame to a specified PostgreSQL table using JDBC.
   * @param df The DataFrame to write.
   * @param tableName The name of the target table in PostgreSQL.
   */
  def writeToPostgres(df: DataFrame, tableName: String): Unit = {
    val pgUser = sys.env.getOrElse("POSTGRES_USER", "default_user")
    val pgPassword = sys.env.getOrElse("POSTGRES_PASSWORD", "default_password")
    val pgDb = sys.env.getOrElse("POSTGRES_DB", "default_db")
    val pgHost = "postgres-warehouse"
    val pgPort = 5432
    val pgJdbcUrl = s"jdbc:postgresql://${pgHost}:${pgPort}/${pgDb}"

    val connectionProperties = new Properties()
    connectionProperties.setProperty("user", pgUser)
    connectionProperties.setProperty("password", pgPassword)
    connectionProperties.setProperty("driver", "org.postgresql.Driver")

    println(s"Writing data to PostgreSQL table: ${tableName}")
    df.write.mode("overwrite").jdbc(pgJdbcUrl, tableName, connectionProperties)
    println(s"Successfully wrote ${df.count()} records to ${tableName}")
  }

  def main(args: Array[String]): Unit = {
    
    val spark = SparkSession.builder
      .appName("Load Data Warehouse")
      .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse")
      .config("hive.metastore.uris", "thrift://hive-metastore:9083")
      .enableHiveSupport()
      .getOrCreate()
      
    println("SparkSession created for DWH loading.")

    // --- 1. Read from Bronze Layer and Build Star Schema ---
    val df_transactions = spark.table("transactions_bronze")
    val df_countries = spark.table("countries_bronze")
    val df_products = spark.table("products_bronze")

    // Create Dimension tables in memory
    println("Creating dim_date...")
    val dim_date = df_transactions.select(to_date(col("InvoiceTimestamp")).alias("full_date")).distinct()
      .withColumn("date_id", date_format(col("full_date"), "yyyyMMdd").cast("int"))
      .withColumn("year", year(col("full_date")))
      .withColumn("month", month(col("full_date")))
      .withColumn("day", dayofmonth(col("full_date")))
    
    println("Creating dim_customers...")
    val dim_customers = df_transactions.select(col("CustomerID")).distinct()
      .withColumn("customer_id", col("CustomerID").cast("int"))

    println("Creating dim_products...")
    val dim_products = df_products.select(col("StockCode").alias("product_id"), col("ProductDescription").alias("description")).distinct()

    println("Creating dim_geography...")
    val dim_geography = df_countries.select(col("CountryID").alias("geo_id"), col("CountryName").alias("country"), col("ContinentName").alias("continent")).distinct()

    // Create Fact table
    println("Creating fct_sales...")
    val df_with_prices = df_transactions.join(
      df_products,
      df_transactions.col("StockCode") === df_products.col("StockCode") && to_date(df_transactions.col("InvoiceTimestamp")) === df_products.col("Date"),
      "inner"
    )

    val fct_sales = df_with_prices
      .join(dim_date, to_date(df_with_prices.col("InvoiceTimestamp")) === dim_date.col("full_date"), "left")
      .join(dim_geography, df_with_prices.col("CountryID") === dim_geography.col("geo_id"), "left")
      .withColumn("total_price", col("Quantity") * col("UnitPrice"))
      .select(
        concat(col("InvoiceNo"), lit("-"), df_transactions.col("StockCode")).alias("sale_id"),
        col("InvoiceNo").alias("invoice_no"),
        col("date_id"),
        col("CustomerID").alias("customer_id"),
        df_transactions.col("StockCode").alias("product_id"), 
        col("geo_id"),
        col("Quantity").alias("quantity"),
        col("UnitPrice").alias("unit_price"),
        col("total_price")
      )

    // --- 2. Write to PostgreSQL Data Warehouse ---
    writeToPostgres(dim_date, "dim_date")
    writeToPostgres(dim_customers, "dim_customers")
    writeToPostgres(dim_products, "dim_products")
    writeToPostgres(dim_geography, "dim_geography")
    writeToPostgres(fct_sales, "fct_sales")

    println("Data Warehouse loading complete.")

    // --- 3. Business Logic Alerting (EP-7) - Corrected Logic per Project Spec ---
    println("Starting anomaly detection for high-quantity orders...")
    val outlierFilePath = "/opt/bitnami/spark/reports/outliers.txt"

    try {
      if (df_transactions.count() > 0) {
        
        // Step 1: Calculate the median and standard deviation of quantity FOR EACH product.
        val product_stats = df_transactions
          .groupBy("StockCode")
          .agg(
            expr("percentile_approx(Quantity, 0.5)").alias("median_quantity"),
            stddev("Quantity").alias("stddev_quantity")
          )

        // Step 2: Join these stats back to the original transactions.
        val transactions_with_stats = df_transactions.join(product_stats, "StockCode")

        // Step 3: Define the threshold and find the outliers.
        // The rule is: Quantity > median + (2 * stddev)
        val outliers = transactions_with_stats.filter(
          col("Quantity") > col("median_quantity") + (lit(2) * col("stddev_quantity"))
        ).select("CustomerID", "InvoiceNo", "StockCode", "Quantity", "median_quantity")
         .distinct()

        // Step 4: Write the results to the outliers.txt file.
        if (outliers.count() > 0) {
          println(s"Found ${outliers.count()} outlier transactions. Writing to ${outlierFilePath}")
          outliers.show()
          
          val outlier_data = outliers.collect().map(row => 
              s"User: ${row.getAs[String]("CustomerID")}, Invoice: ${row.getAs[String]("InvoiceNo")}, Product: ${row.getAs[String]("StockCode")}, Quantity: ${row.getAs[Long]("Quantity")}, Median for Product: ${row.getAs[Double]("median_quantity")}"
          ).mkString("\n")

          new PrintWriter(outlierFilePath) { write(outlier_data); close() }
        } else {
          println("No outlier transactions found. Creating an empty outliers.txt file.")
          new PrintWriter(outlierFilePath) { write(""); close() }
        }
      } else {
        println("No transactions found. Creating an empty outliers.txt file.")
        new PrintWriter(outlierFilePath) { write(""); close() }
      }
    } catch {
      case e: Exception =>
        println(s"An error occurred during anomaly detection: ${e.getMessage}")
        println("Creating an empty outliers.txt file to allow pipeline to continue.")
        new PrintWriter(outlierFilePath) { write(""); close() }
    }

    spark.stop()
  }
}
