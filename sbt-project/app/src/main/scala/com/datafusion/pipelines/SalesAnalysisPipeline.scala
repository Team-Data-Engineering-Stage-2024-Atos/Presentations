package com.datafusion.pipelines

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.log4j.Logger

object SalesAnalysisPipeline {
  def run(spark: SparkSession, salesData: DataFrame): DataFrame = {
    val logger = Logger.getLogger(getClass.getName)
    logger.info("Running Sales Analysis Pipeline...")

    val result = salesData.groupBy("StockCode")
      .agg(
        sum("Quantity").alias("TotalQuantity"),
        sum(col("Quantity") * col("Price")).alias("TotalRevenue")
      )
      .orderBy(desc("TotalRevenue"))

    logger.info(s"Sales Analysis Pipeline completed. Resulting DataFrame has ${result.count()} rows.")
    
    result
  }
}
