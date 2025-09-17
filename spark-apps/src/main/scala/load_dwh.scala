// spark-apps/src/main/scala/load_dwh.scala

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, md5, concat, lit, to_date, year, month, dayofmonth, date_format}
import org.apache.spark.sql.DataFrame
import java.util.Properties

object LoadDWH {

  def writeToPostgres(df: DataFrame, tableName: String): Unit = {
    // Helper function to write a DataFrame to a PostgreSQL table.
    // It reads the database connection details from environment variables.
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
    df.write
      .mode("overwrite")
      .jdbc(pgJdbcUrl, tableName, connectionProperties)
    
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

    // 1. Read all Bronze layer tables from Hive
    val df_transactions = spark.table("transactions_bronze")
    val df_countries = spark.table("countries_bronze")
    val df_products = spark.table("products_bronze")

    println("Successfully read all bronze tables from Hive.")

    // --- 2. Create Dimension Tables ---

    // Create dim_date from the transaction data
    println("Creating dim_date...")
    val dim_date = df_transactions.select(
        to_date(col("InvoiceTimestamp")).alias("full_date")
    ).distinct()
     .withColumn("date_id", date_format(col("full_date"), "yyyyMMdd").cast("int"))
     .withColumn("year", year(col("full_date")))
     .withColumn("month", month(col("full_date")))
     .withColumn("day", dayofmonth(col("full_date")))
    
    // Create dim_customers from transaction data
    println("Creating dim_customers...")
    val dim_customers = df_transactions.select(
        col("CustomerID")
    ).distinct()
     .withColumn("customer_id", col("CustomerID").cast("int"))

    // Create dim_products from product data
    println("Creating dim_products...")
    val dim_products = df_products.select(
        col("StockCode").alias("product_id"),
        col("ProductDescription").alias("description")
    ).distinct()

    // Create dim_geography from country data
    println("Creating dim_geography...")
    val dim_geography = df_countries.select(
        col("CountryID").alias("geo_id"),
        col("CountryName").alias("country"),
        col("ContinentName").alias("continent")
    ).distinct()


    // --- 3. Create the Fact Table ---
    // This requires joining the transactions with all the dimensions to get the foreign keys.
    println("Creating fct_sales...")

    // Join transactions with products based on StockCode AND the date to get the correct price
    val df_with_prices = df_transactions.join(
      df_products,
      df_transactions.col("StockCode") === df_products.col("StockCode") &&
      to_date(df_transactions.col("InvoiceTimestamp")) === df_products.col("Date"),
      "inner"
    )

    // Join the result with all other dimensions to build the final fact table
    val fct_sales = df_with_prices
      .join(dim_date, to_date(df_with_prices.col("InvoiceTimestamp")) === dim_date.col("full_date"), "left")
      .join(dim_geography, df_with_prices.col("CountryID") === dim_geography.col("geo_id"), "left")
      .withColumn("total_price", col("Quantity") * col("UnitPrice"))
      .select(
        concat(col("InvoiceNo"), lit("-"), df_transactions.col("StockCode")).alias("sale_id"), // Use specific df_transactions
        col("InvoiceNo").alias("invoice_no"),
        col("date_id"),
        col("CustomerID").alias("customer_id"),
        df_transactions.col("StockCode").alias("product_id"), 
        col("geo_id"),
        col("Quantity").alias("quantity"),
        col("UnitPrice").alias("unit_price"),
        col("total_price")
      )

    // --- 4. Write all final tables to PostgreSQL Data Warehouse ---
    println("Writing final tables to PostgreSQL...")
    writeToPostgres(dim_date, "dim_date")
    writeToPostgres(dim_customers, "dim_customers")
    writeToPostgres(dim_products, "dim_products")
    writeToPostgres(dim_geography, "dim_geography")
    writeToPostgres(fct_sales, "fct_sales")

    println("Data Warehouse loading complete.")
    spark.stop()
  }
}