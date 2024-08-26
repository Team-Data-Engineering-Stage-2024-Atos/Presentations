package com.datafusion.pipelines

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.log4j.Logger

object ProductPerformancePipeline {
  def run(spark: SparkSession, salesData: DataFrame, productData: DataFrame): DataFrame = {
    val logger = Logger.getLogger(getClass.getName)
    logger.info("Running Product Performance Pipeline...")

    val salesDataRenamed = salesData.withColumnRenamed("Price", "SalesPrice")
    val productDataRenamed = productData.withColumnRenamed("Price", "ProductPrice")

    val joinedData = salesDataRenamed.join(productDataRenamed, salesDataRenamed("StockCode") === productDataRenamed("Sku"))

    val result = joinedData.groupBy("StockCode")
      .agg(
        avg("SalesPrice").alias("AverageSalesPrice"),
        sum("Quantity").alias("TotalSales"),
        avg("Average Rating").alias("AmazonRating")
      )
      .orderBy(desc("TotalSales"))

    logger.info(s"Product Performance Pipeline completed. Resulting DataFrame has ${result.count()} rows.")
    
    result
  }
}
