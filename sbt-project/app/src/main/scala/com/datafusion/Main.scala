package com.datafusion

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import com.datafusion.pipelines._
import com.typesafe.config.ConfigFactory

object Main {
  def main(args: Array[String]): Unit = {
    val config = ConfigFactory.load()
    val appName = config.getString("spark.appName")
    val master = config.getString("spark.master")

    val spark = SparkSession.builder()
      .appName(appName)
      .master(master)
      .getOrCreate()

    val jdbcUrl = "jdbc:postgresql://postgres:5432/sparkdb"
    val jdbcProperties = new java.util.Properties()
    jdbcProperties.setProperty("user", "sparkuser")
    jdbcProperties.setProperty("password", "passer")


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



    // Load datasets from HDFS
    val salesData = loadDataset(spark, "hdfs://master-namenode:9000/user/hadoopuser/datasets/online_retail_II.csv", salesSchema)
    val customerData = loadDataset(spark, "hdfs://master-namenode:9000/user/hadoopuser/datasets/ccdata.csv", customerSchema)
    val productData = loadDataset(spark, "hdfs://master-namenode:9000/user/hadoopuser/datasets/amazon-product-listing-data.csv", productSchema)


    // val salesData = loadCsvFromLocal(spark, "/datasets/online_retail_II.csv")
    // val customerData = loadCsvFromLocal(spark, "/datasets/ccdata.csv")
    // val productData = loadCsvFromLocal(spark, "/datasets/amazon-product-listing-data.csv")


    val productPerformanceResult = ProductPerformancePipeline.run(spark, salesData, productData)
    saveToPostgres(productPerformanceResult, jdbcUrl, jdbcProperties, "product_performance_analysis")

    val salesAnalysisResult = SalesAnalysisPipeline.run(spark, salesData)
    saveToPostgres(salesAnalysisResult, jdbcUrl, jdbcProperties, "sales_analysis")

    val cltvResult = CLTVPipeline.run(spark, salesData, customerData)
    saveToPostgres(cltvResult, jdbcUrl, jdbcProperties, "cltv_analysis")

    val repeatPurchaseResult = RepeatPurchasePipeline.run(spark, salesData)
    saveToPostgres(repeatPurchaseResult, jdbcUrl, jdbcProperties, "repeat_purchase_analysis")

    val anomalyDetectionResult = AnomalyDetectionPipeline.run(spark, customerData)
    saveToPostgres(anomalyDetectionResult, jdbcUrl, jdbcProperties, "transaction_anomalies")

    spark.stop()
  }

  def loadCsvFromLocal(spark: SparkSession, path: String): DataFrame = {
    spark.read.option("header", "true").option("inferSchema", "true").csv(path)
  }

  def loadDataset(spark: SparkSession, path: String, schema: StructType): DataFrame = {
    spark.read
      .option("header", "true")
      .schema(schema)
      .csv(path)
  }


  def saveToPostgres(dataFrame: DataFrame, jdbcUrl: String, jdbcProperties: java.util.Properties, tableName: String): Unit = {
    dataFrame.write.mode("overwrite").jdbc(jdbcUrl, tableName, jdbcProperties)
  }
}