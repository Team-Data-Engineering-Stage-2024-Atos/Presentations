package com.datafusion

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SparkSession, SaveMode}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.RegexTokenizer
import com.datafusion.pipelines._
import org.apache.log4j.Logger

object Main {
  def main(args: Array[String]): Unit = {
    val logger = Logger.getLogger(getClass.getName)

    val config = ConfigFactory.load()

    val appName = config.getString("spark.appName")
    val master = config.getString("spark.master")
    val jdbcUrl = config.getString("jdbc.url")
    val jdbcUser = config.getString("jdbc.user")
    val jdbcPassword = config.getString("jdbc.password")
    
    val spark = SparkSession.builder()
      .appName(appName)
      .master(master)
      .getOrCreate()

    val jdbcProperties = new java.util.Properties()
    jdbcProperties.setProperty("user", jdbcUser)
    jdbcProperties.setProperty("password", jdbcPassword)

    val salesSchema = StructType(Array(
      StructField("Invoice", StringType, nullable = true),
      StructField("StockCode", StringType, nullable = true),
      StructField("Description", StringType, nullable = true),
      StructField("Quantity", IntegerType, nullable = true),
      StructField("InvoiceDate", StringType, nullable = true),
      StructField("Price", DoubleType, nullable = true),
      StructField("Customer ID", StringType, nullable = true),
      StructField("Country", StringType, nullable = true)
    ))

    val customerSchema = StructType(Array(
      StructField("CUST_ID", StringType, nullable = true),
      StructField("BALANCE", DoubleType, nullable = true),
      StructField("BALANCE_FREQUENCY", DoubleType, nullable = true),
      StructField("PURCHASES", DoubleType, nullable = true),
      StructField("ONEOFF_PURCHASES", DoubleType, nullable = true),
      StructField("INSTALLMENTS_PURCHASES", DoubleType, nullable = true),
      StructField("CASH_ADVANCE", DoubleType, nullable = true),
      StructField("PURCHASES_FREQUENCY", DoubleType, nullable = true),
      StructField("ONEOFF_PURCHASES_FREQUENCY", DoubleType, nullable = true),
      StructField("PURCHASES_INSTALLMENTS_FREQUENCY", DoubleType, nullable = true),
      StructField("CASH_ADVANCE_FREQUENCY", DoubleType, nullable = true),
      StructField("CASH_ADVANCE_TRX", IntegerType, nullable = true),
      StructField("PURCHASES_TRX", IntegerType, nullable = true),
      StructField("CREDIT_LIMIT", DoubleType, nullable = true),
      StructField("PAYMENTS", DoubleType, nullable = true),
      StructField("MINIMUM_PAYMENTS", DoubleType, nullable = true),
      StructField("PRC_FULL_PAYMENT", DoubleType, nullable = true),
      StructField("TENURE", IntegerType, nullable = true)
    ))

    val productSchema = StructType(Array(
      StructField("Uniq Id", StringType, nullable = true),
      StructField("Crawl Timestamp", StringType, nullable = true),
      StructField("Pageurl", StringType, nullable = true),
      StructField("Website", StringType, nullable = true),
      StructField("Title", StringType, nullable = true),
      StructField("Num Of Reviews", IntegerType, nullable = true),
      StructField("Average Rating", DoubleType, nullable = true),
      StructField("Number Of Ratings", IntegerType, nullable = true),
      StructField("Model Num", StringType, nullable = true),
      StructField("Sku", StringType, nullable = true),
      StructField("Upc", StringType, nullable = true),
      StructField("Manufacturer", StringType, nullable = true),
      StructField("Model Name", StringType, nullable = true),
      StructField("Price", DoubleType, nullable = true),
      StructField("Monthly Price", StringType, nullable = true),
      StructField("Stock", BooleanType, nullable = true),
      StructField("Carrier", StringType, nullable = true),
      StructField("Color Category", StringType, nullable = true),
      StructField("Internal Memory", StringType, nullable = true),
      StructField("Screen Size", StringType, nullable = true),
      StructField("Specifications", StringType, nullable = true),
      StructField("Five Star", IntegerType, nullable = true),
      StructField("Four Star", IntegerType, nullable = true),
      StructField("Three Star", IntegerType, nullable = true),
      StructField("Two Star", IntegerType, nullable = true),
      StructField("One Star", IntegerType, nullable = true),
      StructField("Broken Link", BooleanType, nullable = true),
      StructField("Discontinued", BooleanType, nullable = true)
    ))

    logger.info("Loading datasets...")
    val salesData = loadDataset(spark, "hdfs://master-namenode:9000/user/hadoopuser/datasets/online_retail_II.csv", salesSchema, logger)
    val customerData = loadDataset(spark, "hdfs://master-namenode:9000/user/hadoopuser/datasets/ccdata.csv", customerSchema, logger)
    val productData = loadDataset(spark, "hdfs://master-namenode:9000/user/hadoopuser/datasets/amazon-product-listing-data.csv", productSchema, logger)

    // New Data Normalization and Tokenization
    logger.info("Normalizing and Tokenizing datasets...")
    val salesDataNormalized = normalizeText(salesData, "Description")
    val productDataNormalized = normalizeText(productData, "Title")

    val salesDataTokenized = tokenizeText(spark, salesDataNormalized, "Description")
    val productDataTokenized = tokenizeText(spark, productDataNormalized, "Title")

    // Run pipelines and save results
    logger.info("Running pipelines and saving results to PostgreSQL...")
    val productPerformanceResult = ProductPerformancePipeline.run(spark, salesDataTokenized, productDataTokenized)
    saveToPostgres(productPerformanceResult, jdbcUrl, jdbcProperties, "product_performance_analysis", logger)

    val salesAnalysisResult = SalesAnalysisPipeline.run(spark, salesDataTokenized)
    saveToPostgres(salesAnalysisResult, jdbcUrl, jdbcProperties, "sales_analysis", logger)

    val cltvResult = CLTVPipeline.run(spark, salesDataTokenized, customerData)
    saveToPostgres(cltvResult, jdbcUrl, jdbcProperties, "cltv_analysis", logger)

    val repeatPurchaseResult = RepeatPurchasePipeline.run(spark, salesDataTokenized)
    saveToPostgres(repeatPurchaseResult, jdbcUrl, jdbcProperties, "repeat_purchase_analysis", logger)

    val anomalyDetectionResult = AnomalyDetectionPipeline.run(spark, customerData)
    saveToPostgres(anomalyDetectionResult, jdbcUrl, jdbcProperties, "transaction_anomalies", logger)

    spark.stop()
  }

  def loadDataset(spark: SparkSession, path: String, schema: StructType, logger: Logger): DataFrame = {
    try {
      val df = spark.read
        .option("header", "true")
        .schema(schema)
        .csv(path)
      logger.info(s"Loaded dataset from $path with ${df.count()} rows.")
      df
    } catch {
      case e: Exception =>
        logger.error(s"Error loading dataset from $path: ${e.getMessage}")
        spark.emptyDataFrame
    }
  }

  def normalizeText(df: DataFrame, columnName: String): DataFrame = {
    df.withColumn(columnName, lower(regexp_replace(col(columnName), "[^a-zA-Z0-9\\s]", "")))
  }

  def tokenizeText(spark: SparkSession, df: DataFrame, inputCol: String): DataFrame = {
    val tokenizer = new RegexTokenizer().setInputCol(inputCol).setOutputCol(s"${inputCol}Tokens").setPattern("\\W+")
    tokenizer.transform(df)
  }

  def saveToPostgres(dataFrame: DataFrame, jdbcUrl: String, jdbcProperties: java.util.Properties, tableName: String, logger: Logger): Unit = {
    if (dataFrame.isEmpty) {
      logger.warn(s"DataFrame for $tableName is empty. Skipping write to PostgreSQL.")
    } else {
      try {
        dataFrame.write.mode(SaveMode.Overwrite).jdbc(jdbcUrl, tableName, jdbcProperties)
        logger.info(s"Successfully wrote DataFrame to $tableName in PostgreSQL.")
      } catch {
        case e: Exception =>
          logger.error(s"Error saving DataFrame to $tableName: ${e.getMessage}")
      }
    }
  }
}
