// spark-apps/src/main/scala/KafkaBatchProcessor.scala

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.util.Properties

object KafkaBatchProcessor {
  def main(args: Array[String]): Unit = {
    
    // 1. Create a SparkSession.
    // This is the entry point for any Spark functionality.
    val spark = SparkSession.builder
      .appName("Kafka Batch Processor")
      .getOrCreate()
      
    import spark.implicits._

    // 2. Read all available data from the Kafka topic as a single batch.
    println("Reading all messages from Kafka topic 'twitter_trends'...")
    val kafkaBatchDF = spark.read
      .format("kafka") // Specify the data source format.
      .option("kafka.bootstrap.servers", "kafka:29092") // Address of the Kafka broker.
      .option("subscribe", "twitter_trends") // The topic to read from.
      // This option is crucial for batch processing: it tells Spark to read all data
      // currently in the topic up to the latest message, and then stop.
      .option("endingOffsets", "latest") 
      .load()

    // 3. Transform the raw Kafka data.
    // The actual tweet is in the 'value' column as binary data; we cast it to a readable STRING.
    val tweetDF = kafkaBatchDF.selectExpr("CAST(value AS STRING) as tweet")
    
    // 4. Define the product keywords and their full descriptions.
    // This list is used to match parts of a tweet to a specific product.
    val productKeywords = List(
      ("cake", "SET OF 3 CAKE TINS PANTRY DESIGN"), 
      ("glass star", "GLASS STAR FROSTED T-LIGHT HOLDER"),
      ("union jack", "HAND WARMER UNION JACK"),
      ("bird ornament", "ASSORTED COLOUR BIRD ORNAMENT"),
      ("playhouse", "POPPY'S PLAYHOUSE BEDROOM"),
      ("mug cosy", "IVORY KNITTED MUG COSY")
    )

    // 5. Build a dynamic SQL CASE expression to match keywords.
    // This creates a nested `CASE WHEN tweet CONTAINS keyword THEN description ... END` statement.
    val keywordMatchingExpr = productKeywords
      .map { case (keyword, desc) => when(lower(col("tweet")).contains(keyword), lit(desc)) }
      .foldLeft(lit(null)) { (acc, nextWhen) => nextWhen.otherwise(acc) }

    // 6. Apply the matching expression and filter out non-matching tweets.
    val matchedDF = tweetDF
      .withColumn("product_mention", keywordMatchingExpr)
      .filter($"product_mention".isNotNull)
    
    // 7. Aggregate the data to count product mentions.
    println("Aggregating trends...")
    val trendingProductsDF = matchedDF
      .groupBy($"product_mention") // Group by the full product description.
      .count() // Count the occurrences of each product.
      .orderBy(desc("count")) // Sort to find the most popular.
      .limit(10) // Take only the top 10.

    // 8. Display the final results in the console.
    println("Top 10 trending products:")
    trendingProductsDF.show(false)

    // 9. Configure the PostgreSQL connection properties.
    // It reads credentials from environment variables.
    println("Writing results to PostgreSQL...")
    val pgUser = sys.env.getOrElse("POSTGRES_USER", "postgres")
    val pgPassword = sys.env.getOrElse("POSTGRES_PASSWORD", "your_secure_password")
    val pgDb = sys.env.getOrElse("POSTGRES_DB", "ecommerce_dwh")
    val pgJdbcUrl = "jdbc:postgresql://postgres-warehouse:5432/" + pgDb

    val connectionProperties = new Properties()
    connectionProperties.setProperty("user", pgUser)
    connectionProperties.setProperty("password", pgPassword)
    connectionProperties.setProperty("driver", "org.postgresql.Driver")

    // 10. Write the final Top list to the PostgreSQL database.
    // The 'overwrite' mode ensures the table always contains the latest trend analysis.
    trendingProductsDF.write
      .mode("overwrite")
      .jdbc(pgJdbcUrl, "trending_products", connectionProperties)
    
    println("Successfully wrote trending products to PostgreSQL.")

    // 11. Stop the SparkSession to release resources.
    spark.stop()
  }
}